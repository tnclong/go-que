package que

import (
	"strings"
	"testing"
)

func TestStack(t *testing.T) {
	stack := Stack(3)
	t.Log(stack)
	prefix := "github.com/tnclong/go-que.TestStack"
	if !strings.HasPrefix(stack, prefix) {
		t.Fatalf("want stack has prefix: %s", prefix)
	}
	stackCaller := "github.com/tnclong/go-que/stack_test.go:9"
	if !strings.Contains(stack, stackCaller) {
		t.Fatalf("want stack has caller: %s", stackCaller)
	}
}
