package scheduler

import (
	"testing"
)

func TestValidateSchedule(t *testing.T) {
	if err := ValidateSchedule(wantSchedule); err != nil {
		t.Fatal(err)
	}
}
