package main

import (
	"strings"
	"testing"
	"time"
)

func testConfig() *TestConfig {
	return &TestConfig{
		ProtocolVersion: clusterProtocolVersion,
		URL:             "http://127.0.0.1:8080/",
		Method:          "GET",
		Headers:         []string{"X: y"},
		Concurrency:     4,
		Rate:            100,
		Requests:        1000,
		DurationNS:      (5 * time.Second).Nanoseconds(),
		RampUp:          4,
	}
}

func TestConfigHashStableAndTamperDetected(t *testing.T) {
	c := testConfig()
	h1 := c.ConfigHash()
	h2 := c.ConfigHash()
	if h1 != h2 || len(h1) != 64 {
		t.Fatalf("hash unstable or wrong length: %s %s", h1, h2)
	}
	tampered := *c
	tampered.Rate = 101
	if tampered.ConfigHash() == h1 {
		t.Fatal("hash must change when config changes")
	}
	// Field order of headers is part of the canonical form.
	reordered := *c
	reordered.Headers = []string{"X:y"} // different value (space) -> different hash
	if reordered.ConfigHash() == h1 {
		t.Fatal("header change must alter hash")
	}
}

func TestVerifyConfigAcceptsValid(t *testing.T) {
	c := testConfig()
	cmd := &Command{
		Type:    cmdPrepare,
		Version: clusterResultVersion,
		Config:  c,
		Hash:    c.ConfigHash(),
	}
	got, err := cmd.VerifyConfig()
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.URL != c.URL {
		t.Fatal("config mismatch")
	}
}

func TestVerifyConfigRejects(t *testing.T) {
	c := testConfig()
	cases := []struct {
		name    string
		mutate  func(*Command)
		wantErr string
	}{
		{"wrong result version", func(cmd *Command) { cmd.Version = 999 }, errCodeResultMismatch},
		{"tampered hash", func(cmd *Command) { cmd.Hash = "deadbeef" }, errCodeConfigMismatch},
		{"not prepare", func(cmd *Command) { cmd.Type = cmdSetRate }, errCodeBadRequest},
		{"bad protocol in config", func(cmd *Command) { cmd.Config.ProtocolVersion = 42 }, errCodeVersionMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &Command{Type: cmdPrepare, Version: clusterResultVersion, Config: c, Hash: c.ConfigHash()}
			tc.mutate(cmd)
			_, err := cmd.VerifyConfig()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want contains %q", err, tc.wantErr)
			}
		})
	}
}

func TestVersionsCompatible(t *testing.T) {
	if err := versionsCompatible(clusterProtocolVersion, clusterResultVersion, version); err != nil {
		t.Fatalf("same version must be compatible: %v", err)
	}
	if err := versionsCompatible(clusterProtocolVersion, clusterResultVersion, "dev"); err != nil {
		t.Fatalf("dev build must bypass plow version check: %v", err)
	}
	if err := versionsCompatible(999, clusterResultVersion, "1.0"); err == nil {
		t.Fatal("protocol mismatch must be rejected")
	}
	// Exact plow version enforcement for release builds.
	v := version
	version = "1.2.3"
	defer func() { version = v }()
	if err := versionsCompatible(clusterProtocolVersion, clusterResultVersion, "1.2.4"); err == nil {
		t.Fatal("different release plow versions must be rejected")
	}
}
