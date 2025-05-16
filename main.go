package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc/peer"
)

var (
	maxConnsPerHost     = 0
	maxIdleConnsPerHost = 100

	// MiB means 1024 KiB.
	MiB = int64(1024 * 1024)

	numOfWorkers = flag.Int("worker", 128, "Number of concurrent worker to read")

	runTime = flag.Duration("run-time", 3*time.Minute, "Actual workload runtime")

	warmUpTime = flag.Duration("warm-up-time", 2*time.Second, "Ramp up time")

	grpcConnPoolSize = flag.Int("grpc-conn-pool-size", 1, "grpc connection pool size")

	maxRetryDuration = 30 * time.Second

	retryMultiplier = 2.0

	bucketName = flag.String("bucket", "kislayk_europe_west4", "GCS bucket name.")

	clientProtocol = flag.String("client-protocol", "http", "Network protocol.")

	totalDownload = flag.Int("total-mbytes", 0, "Stop after this amount of MiB downloaded.")

	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	// Object name = objectNamePrefix + {thread_id} + objectNameSuffix
	objectNamePrefix = flag.String("obj-prefix", "1GB/experiment.", "Object prefix")
	objectNameSuffix = flag.String("obj-suffix", ".0", "Object suffix")
)

// CreateHTTPClient create http storage client.
func CreateHTTPClient(ctx context.Context) (client *storage.Client, err error) {
	var transport *http.Transport
	// Using http1 makes the client more performant.
	transport = &http.Transport{
		MaxConnsPerHost:     maxConnsPerHost,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		// This disables HTTP/2 in transport.
		TLSNextProto: make(
			map[string]func(string, *tls.Conn) http.RoundTripper,
		),
	}

	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("while generating tokenSource, %v", err)
	}

	// Custom http client for Go Client.
	httpClient := &http.Client{
		Transport: &userAgentRoundTripper{
			wrapped: &oauth2.Transport{
				Base:   transport,
				Source: tokenSource,
			},
			UserAgent: "prince",
		},
		Timeout: 0,
	}
	return storage.NewClient(ctx, option.WithHTTPClient(httpClient))
}

func rampUp(warmupCtx context.Context, cancelFn context.CancelFunc, bucketHandle *storage.BucketHandle) {
	idx := 0
	var eG errgroup.Group
	for {
		select {
		case <-warmupCtx.Done():
			cancelFn()
			return
		default:
			if idx == *numOfWorkers {
				eG.Wait()
				return
			}
			time.Sleep(1 * time.Second)
			eG.Go(func() error {
				_, err := ReadObject(warmupCtx, idx, bucketHandle)
				if err != nil {
					err = fmt.Errorf("while reading object %v: %w", *objectNamePrefix+strconv.Itoa(idx)+*objectNameSuffix, err)
					return err
				}
				return err
			})
			idx++
		}
	}
}

// CreateGrpcClient creates grpc client.
func CreateGrpcClient(ctx context.Context) (client *storage.Client, err error) {
	tokenSource, err := GetTokenSource(ctx, "")
	if err != nil {
		return nil, err
	}
	return storage.NewGRPCClient(ctx, option.WithGRPCConnectionPool(*grpcConnPoolSize), option.WithTokenSource(tokenSource), storage.WithDisabledClientMetrics())
}

// ReadObject creates reader object corresponding to workerID with the help of bucketHandle.
func ReadObject(ctx context.Context, workerID int, bucketHandle *storage.BucketHandle) (bytesRead int64, err error) {
	objectName := *objectNamePrefix + strconv.Itoa(workerID) + *objectNameSuffix

	select {
	case <-ctx.Done():
		return 0, nil
	default:
		object := bucketHandle.Object(objectName)
		rc, err := object.NewReader(ctx)
		if err != nil {
			return 0, fmt.Errorf("while creating reader object: %v", err)
		}
		defer rc.Close()

		// Calls Reader.WriteTo implicitly.
		count, err := io.Copy(io.Discard, rc)
		bytesRead += count
		if err != nil {
			return bytesRead, fmt.Errorf("while reading and discarding content: %v", err)
		}
		return bytesRead, nil
	}
}

type peerEvent struct {
	time  time.Time
	dur   time.Duration
	event string
	peer  *peer.Peer
}

func main() {
	flag.Parse()
	fmt.Printf("Starting benchmark with params:\n")
	fmt.Printf("workers: %d, grpcConnPoolSize: %d\n", *numOfWorkers, *grpcConnPoolSize)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		printConns(ctx)
	}()

	fmt.Printf("Workload start time: %s\n", time.Now().String())

	var client *storage.Client
	var err error
	protocol := ""
	if *clientProtocol == "http" {
		protocol = "http"
		client, err = CreateHTTPClient(ctx)
	} else {
		protocol = "grpc"
		client, err = CreateGrpcClient(ctx)
	}

	if err != nil {
		fmt.Printf("while creating the client: %v", err)
		os.Exit(1)
	}

	client.SetRetry(
		storage.WithBackoff(gax.Backoff{
			Max:        maxRetryDuration,
			Multiplier: retryMultiplier,
		}),
		storage.WithPolicy(storage.RetryAlways))

	// assumes bucket already exist
	bucketHandle := client.Bucket(*bucketName)

	var totalBytesRead atomic.Int64

	warmupCtx, cancelFn := context.WithDeadline(ctx, time.Now().Add(*warmUpTime))
	//fmt.Println("Ramp-up starts")

	rampUp(warmupCtx, cancelFn, bucketHandle)

	// runtime.SetMutexProfileFraction(1)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	//fmt.Println("Ramp-up complete. Starting run on actual traffic.")
	startTime := time.Now()
	var eG errgroup.Group

	actualRunCtx, cancelFn := context.WithDeadline(ctx, startTime.Add(*runTime))
	defer cancelFn()

	var errCount atomic.Int64
	events := []peerEvent{}
	mu := sync.Mutex{}

	// Run the actual workload
	for i := 0; i < *numOfWorkers; i++ {
		idx := i
		eG.Go(func() error {
			//fmt.Printf("Worker %d started\n", idx)
			for {
				select {
				case <-actualRunCtx.Done():
					return nil
				default:
					peerStrt := time.Now()
					p := &peer.Peer{}
					bytesRead, err := ReadObject(peer.NewContext(actualRunCtx, p), idx, bucketHandle)
					if err != nil {
						errCount.Add(1)
					}
					if *clientProtocol == "grpc" && bytesRead > 0 && p.Addr != nil && p.LocalAddr != nil {
						mu.Lock()
						end := time.Now()
						events = append(events, peerEvent{
							time:  peerStrt,
							event: "start",
							peer:  p,
						}, peerEvent{
							time:  end,
							dur:   end.Sub(peerStrt),
							event: "end",
							peer:  p,
						})
						mu.Unlock()
					}

					totalBytesRead.Add(bytesRead)
					if *totalDownload > 0 && totalBytesRead.Load() > int64(*totalDownload)*MiB {
						return nil
					}
				}
			}
		})
	}

	err = eG.Wait()
	totalDuration := time.Since(startTime)
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}

	// Sort events by time.
	sort.Slice(events, func(i, j int) bool {
		return events[i].time.Before(events[j].time)
	})

	uniq_backends := make(map[string]struct{})
	backend_load := make(map[string]int)
	conn_load := make(map[string]int)

	// Analyze events
	accRPB := float64(0)
	accNIB := float64(0)
	accNIC := float64(0)

	favgRPB := float64(0)
	favgNIB := float64(0)
	favgNIC := float64(0)

	peerDuration := make(map[string][]time.Duration)

	if *clientProtocol == "grpc" {
		prevMicro := microOffset(events[0].time)
		for _, event := range events {
			conn_key := event.peer.LocalAddr.String() + "-" + event.peer.Addr.String()
			if event.event == "start" {
				uniq_backends[event.peer.Addr.String()] = struct{}{}
				if _, ok := backend_load[event.peer.Addr.String()]; !ok {
					backend_load[event.peer.Addr.String()] = 1
				} else {
					backend_load[event.peer.Addr.String()]++
				}
				if _, ok := conn_load[conn_key]; !ok {
					conn_load[conn_key] = 1
				} else {
					conn_load[conn_key]++
				}
			}
			if event.event == "end" {
				peerDuration[event.peer.Addr.String()] = append(peerDuration[event.peer.Addr.String()], event.dur)
				backend_load[event.peer.Addr.String()]--
				conn_load[conn_key]--
			}
			micro := microOffset(event.time)
			bas := make([]string, 0, len(backend_load))
			for ba := range backend_load {
				bas = append(bas, ba)
			}
			sort.Strings(bas)
			non_idle_bes := 0
			non_idle_conns := 0
			max_rpb := 0

			for _, ba := range bas {
				if backend_load[ba] > 0 {
					non_idle_bes++
				}
				if max_rpb < backend_load[ba] {
					max_rpb = backend_load[ba]
				}
			}

			for _, v := range conn_load {
				if v > 0 {
					non_idle_conns++
				}
			}

			dur := micro - prevMicro
			accRPB += float64(dur) * float64(max_rpb)
			accNIB += float64(dur) * float64(non_idle_bes)
			accNIC += float64(dur) * float64(non_idle_conns)
			prevMicro = micro
		}

		favgRPB = float64(accRPB) / float64(prevMicro-microOffset(events[0].time))
		favgNIB = float64(accNIB) / float64(prevMicro-microOffset(events[0].time))
		favgNIC = float64(accNIC) / float64(prevMicro-microOffset(events[0].time))
	}

	cancel()

	// fmt.Println("MUTEX INFO START")
	// pprof.Lookup("mutex").WriteTo(os.Stdout, 0)
	// fmt.Println("MUTEX INFO END")

	if err == nil && err != context.DeadlineExceeded {
		bndwth := float64(1_000_000) / float64(MiB) * float64(totalBytesRead.Load()) / float64(totalDuration.Microseconds())

		if *clientProtocol == "grpc" {
			fmt.Printf("Unique backends: %d\n", len(uniq_backends))
			fmt.Printf("Average maxRPB/s: %.3f\n", favgRPB)
			fmt.Printf("Average NIB/s: %.3f (%.2f MiB/s per backend)\n", favgNIB, bndwth/favgNIB)
			fmt.Printf("Average NIC/s: %.3f (%.2f MiB/s per connection)\n", favgNIC, bndwth/favgNIC)
		}

		for p, durs := range peerDuration {
			slices.Sort(durs)
			tot := len(durs)
			p50 := durs[tot*50/100].Milliseconds()
			p75 := durs[tot*75/100].Milliseconds()
			p90 := durs[tot*90/100].Milliseconds()
			p95 := durs[tot*95/100].Milliseconds()
			p99 := durs[tot*99/100].Milliseconds()
			pmax := durs[tot-1].Milliseconds()
			fmt.Printf("Peer %s durations: %d, 50%%: %d, 75%%: %d, 90%%: %d, 95%%: %d, 99%%: %d, max: %d\n", p, tot, p50, p75, p90, p95, p99, pmax)
			// for _, d := range durs {
			// 	fmt.Printf("%d ", d.Milliseconds())
			// }
			// fmt.Println("")
		}

		fmt.Printf("Protocol: %s, Bandwidth: %.0f MiB/s, errors: %d\n", protocol, bndwth, errCount.Load())
		fmt.Printf("Workload end time: %s\n\n", time.Now().String())
		if *cpuprofile != "" {
			fmt.Println("Waiting to exit...")
			time.Sleep(5 * time.Minute)
		}
		os.Exit(0)
	} else {
		fmt.Fprintf(os.Stderr, "Error while running benchmark: %v", err)
		fmt.Printf("Workload end time: %s\n\n", time.Now().String())
		os.Exit(1)
	}
}

func printConns(ctx context.Context) {
	cmd := exec.Command("netstat", "-pn")

	output, err := cmd.Output()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	dp_conns := 0
	https_conns := 0

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, "ESTABLISHED") {
			if strings.Contains(line, "34.126.") {
				dp_conns++
			} else if strings.Contains(line, ":443 ") {
				https_conns++
			}
		}
	}

	fmt.Printf("Directpath connections: %d\n", dp_conns)
	fmt.Printf("HTTPS connections: %d\n", https_conns)
	time.Sleep(time.Second * 10)
	if ctx.Err() == nil {
		printConns(ctx)
	}
}

func microOffset(t time.Time) int64 {
	if _, strtime, ok := strings.Cut(t.String(), " m=+"); ok {
		seconds, err := strconv.ParseFloat(strtime, 64)
		if err != nil {
			return int64(0)
		}
		return int64(math.Round(seconds * 1e6))
	}
	return int64(0)
}
