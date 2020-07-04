package que

import (
	"testing"
	"time"
)

func TestArgs(t *testing.T) {
	now := time.Now()
	i := 1
	data := Args(now, i)

	var pnow time.Time
	var pi int
	count, err := ParseArgs(data, &pnow, &pi)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("want count is 2 but get %d", count)
	}
	if !now.Equal(pnow) {
		t.Fatalf("want %s but get %s", now, pnow)
	}
	if i != pi {
		t.Fatalf("want %d but get %d", i, pi)
	}

	count, err = ParseArgs(data, &pnow)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("want count is 1 but get %d", count)
	}
	if !now.Equal(pnow) {
		t.Fatalf("want %s but get %s", now, pnow)
	}
}
