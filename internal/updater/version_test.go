package updater

import (
	"runtime"
	"testing"
)

func TestVersionParseAndCompare(t *testing.T) {
	t.Parallel()

	v010, err := ParseVersion("0.1.0")
	if err != nil {
		t.Fatalf("parse 0.1.0: %v", err)
	}
	v020, err := ParseVersion("0.2.0")
	if err != nil {
		t.Fatalf("parse 0.2.0: %v", err)
	}
	v021, err := ParseVersion("0.2.1")
	if err != nil {
		t.Fatalf("parse 0.2.1: %v", err)
	}
	v100, err := ParseVersion("1.0.0")
	if err != nil {
		t.Fatalf("parse 1.0.0: %v", err)
	}
	v099, err := ParseVersion("0.9.9")
	if err != nil {
		t.Fatalf("parse 0.9.9: %v", err)
	}

	if !v010.LessThan(v020) {
		t.Fatalf("expected 0.1.0 < 0.2.0")
	}
	if !v020.LessThan(v021) {
		t.Fatalf("expected 0.2.0 < 0.2.1")
	}
	if v100.LessThan(v099) {
		t.Fatalf("expected 1.0.0 > 0.9.9")
	}
}

func TestParseVersionErrors(t *testing.T) {
	t.Parallel()

	invalid := []string{"", "1", "1.2", "1.2.3.4", "a.b.c", "1.-1.0"}
	for _, tc := range invalid {
		tc := tc
		t.Run(tc, func(t *testing.T) {
			_, err := ParseVersion(tc)
			if err == nil {
				t.Fatalf("expected parse error for %q", tc)
			}
		})
	}
}

func TestCurrentPlatformKey(t *testing.T) {
	t.Parallel()

	want := runtime.GOOS + "-" + runtime.GOARCH
	if got := CurrentPlatformKey(); got != want {
		t.Fatalf("unexpected platform key: got %q want %q", got, want)
	}
}
