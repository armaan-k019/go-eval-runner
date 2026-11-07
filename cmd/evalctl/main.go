// Command evalctl submits eval runs and reports on them.
//
//	evalctl gen-mock -n 1000 -unique 700 > cases.jsonl
//	evalctl submit -suite smoke -type mock -config '{"latency_median_ms":100}' -cases cases.jsonl -wait
//	evalctl status <run-id>
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: evalctl <command> [flags]

commands:
  submit    submit a run from a JSONL file of {"case_id", "payload"} lines
  status    print a run's status and stats as JSON
  gen-mock  write synthetic mock cases as JSONL to stdout`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "submit":
		submit(os.Args[2:])
	case "status":
		statusCmd(os.Args[2:])
	case "gen-mock":
		genMock(os.Args[2:])
	default:
		usage()
	}
}

func dial(addr string) pb.CoordinatorClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256<<20)))
	if err != nil {
		fail(err)
	}
	return pb.NewCoordinatorClient(conn)
}

func coordFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("EVALRUNNER_COORDINATOR")
	if def == "" {
		def = "localhost:7070"
	}
	return fs.String("coordinator", def, "coordinator gRPC address")
}

// ReadCases parses JSONL lines of {"case_id": "...", "payload": {...}}.
func ReadCases(r io.Reader) ([]*pb.Case, error) {
	var out []*pb.Case
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var c struct {
			CaseID  string          `json:"case_id"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if c.CaseID == "" {
			c.CaseID = fmt.Sprintf("case-%d", line)
		}
		if len(c.Payload) == 0 {
			c.Payload = json.RawMessage("{}")
		}
		out = append(out, &pb.Case{CaseId: c.CaseID, PayloadJson: c.Payload})
	}
	return out, sc.Err()
}

func submit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	addr := coordFlag(fs)
	suite := fs.String("suite", "default", "suite name")
	jobType := fs.String("type", "mock", "job type (executor): mock | exec")
	config := fs.String("config", "{}", "executor config JSON, or @file.json")
	casesPath := fs.String("cases", "-", "JSONL cases file (- for stdin)")
	tries := fs.Int("max-attempts", 3, "attempts per job, including the first")
	timeout := fs.Duration("timeout", 5*time.Minute, "per-attempt timeout")
	noCache := fs.Bool("no-cache", false, "do not answer jobs from the result cache")
	wait := fs.Bool("wait", false, "wait for the run to finish, then print stats")
	fs.Parse(args)

	cfg := []byte(*config)
	if len(cfg) > 0 && cfg[0] == '@' {
		var err error
		if cfg, err = os.ReadFile(string(cfg[1:])); err != nil {
			fail(err)
		}
	}
	var in io.Reader = os.Stdin
	if *casesPath != "-" {
		f, err := os.Open(*casesPath)
		if err != nil {
			fail(err)
		}
		defer f.Close()
		in = f
	}
	cases, err := ReadCases(in)
	if err != nil {
		fail(err)
	}
	c := dial(*addr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := c.SubmitRun(ctx, &pb.SubmitRunRequest{
		Suite: *suite, JobType: *jobType, ConfigJson: cfg, Cases: cases,
		MaxAttempts: int32(*tries), TimeoutMs: int32(timeout.Milliseconds()), NoCache: *noCache,
	})
	if err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "run %s: %d jobs, %d cache hits, %d deduped\n", resp.RunId, resp.Jobs, resp.CacheHits, resp.Deduped)
	if !*wait {
		fmt.Println(resp.RunId)
		return
	}
	st := WaitRun(c, resp.RunId, func(s *pb.RunStatus) {
		fmt.Fprintf(os.Stderr, "  %d/%d done (queued %d, leased %d, waiting %d, failed %d)\n",
			s.Succeeded+s.Cached+s.Failed, s.Total, s.Queued, s.Leased, s.Waiting, s.Failed)
	})
	printJSON(st)
}

// WaitRun polls until the run is done.
func WaitRun(c pb.CoordinatorClient, runID string, progress func(*pb.RunStatus)) *pb.RunStatus {
	last := time.Time{}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		st, err := c.GetRun(ctx, &pb.GetRunRequest{RunId: runID})
		cancel()
		if err != nil {
			fail(err)
		}
		if st.Done {
			return st
		}
		if progress != nil && time.Since(last) > 2*time.Second {
			progress(st)
			last = time.Now()
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func statusCmd(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addr := coordFlag(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		fail(fmt.Errorf("usage: evalctl status <run-id>"))
	}
	st, err := dial(*addr).GetRun(context.Background(), &pb.GetRunRequest{RunId: fs.Arg(0)})
	if err != nil {
		fail(err)
	}
	printJSON(st)
}

func genMock(args []string) {
	fs := flag.NewFlagSet("gen-mock", flag.ExitOnError)
	n := fs.Int("n", 1000, "number of cases")
	unique := fs.Int("unique", 0, "distinct payloads (0 = all distinct); n-unique cases repeat earlier payloads")
	salt := fs.String("salt", "", "string mixed into every payload, to defeat the cache between runs")
	fs.Parse(args)
	u := *unique
	if u <= 0 || u > *n {
		u = *n
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	enc := json.NewEncoder(w)
	for i := 0; i < *n; i++ {
		p := map[string]any{"task": i % u}
		if *salt != "" {
			p["salt"] = *salt
		}
		enc.Encode(map[string]any{"case_id": fmt.Sprintf("mock-%06d", i), "payload": p})
	}
}

func printJSON(st *pb.RunStatus) {
	b, _ := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: true, UseProtoNames: true}.Marshal(st)
	fmt.Println(string(b))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "evalctl:", err)
	os.Exit(1)
}
