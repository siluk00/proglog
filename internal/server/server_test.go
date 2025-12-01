package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/siluk00/proglog/api/v1"
	"github.com/siluk00/proglog/internal/auth"
	"github.com/siluk00/proglog/internal/config"
	"github.com/siluk00/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume streaam succeeds":                   testProduceConsumeStream,
		"consume paast log boundary fails":                   testConsumePastBoundary,
		"unauthorized fails":                                 testUnathorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

// sets up a complete test environment with:
// 1. A grpc server running on localhost with TLS
// 2. Two clients  with different certificates
// 3. A clean up function to stop everything after the test
func setupTest(t *testing.T, fn func(*Config)) (
	rootClient, nobodyClient api.LogClient, // Two client instances with different privileges
	cfg *Config, //Server Configuration
	teardown func(), // Cleanup function
) {
	t.Helper()

	// Create a TCP listener on localhost with random port (the 0 means to let the OS choose the port)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Helper function that allows to create a grpc client with specific TLS credentials
	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		// Client certificate, client private key, CAFile to verify server
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		// Wraps the tls.config to grpc-ready credentials
		tlsCreds := credentials.NewTLS(tlsConfig)
		// Configure dial options: using tls for secure connection
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		// asks for connection to the server listener with opts above l.Addr().String() returns the actual address
		conn, err := grpc.NewClient(l.Addr().String(), opts...)
		require.NoError(t, err)

		// creates the log service client
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	// create root client that has admin privileges
	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)
	//create nobody client with no privileges
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// configure TLS for the server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	// wraps configuration into tls credentials
	serverCreds := credentials.NewTLS(serverTLSConfig)

	// temp dir for test isolation
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	// Initialize log
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Initialize the authorizer
	// Use casbin model and policy files
	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)

	// Sets up telemetry exporter for debugging
	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err := exporter.NewLogExporter(
			exporter.Options{
				MetricsLogFile:    metricsLogFile.Name(),
				TracesLogFile:     tracesLogFile.Name(),
				ReportingInterval: time.Second,
			},
		)
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	// Build server configuration
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	// aply any custom configurations
	if fn != nil {
		fn(cfg)
	}
	// creates a grpc server with tls credentials
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// start server in goroutine for non-blocking
	go func() {
		server.Serve(l)
	}()

	// returns clients, configuration and cleanup function
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}

}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	// creates context for cancellation
	ctx := context.Background()
	// Creates a test record to write
	want := &api.Record{
		Value: []byte("hello world"),
	}

	// Write record to log
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	// read recording back using offset returned
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	// What we read matches what we wrote?
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got er: %v, wan: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testUnathorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
