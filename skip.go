package que

import (
	"context"
)

// ID value indicating a record was skipped due to a unique constraint conflict
const SkippedConflictID int64 = -1

// ctxKeySkipConflict is the key used in context to identify if unique constraint conflicts should be skipped
type ctxKeySkipConflict struct{}

// WithSkipConflict returns a new context with a flag to skip unique constraint conflicts.
// When using this context with the Enqueue method, records that violate unique constraints
// will be skipped instead of returning an error.
// The returned ID array corresponds one-to-one with the input plans array,
// with conflicted records' IDs set to SkippedConflictID.
//
// Example:
//
//	ctx := que.WithSkipConflict(context.Background())
//	ids, err := queue.Enqueue(ctx, tx, plans...)
//	if err != nil {
//	    // Handle other errors
//	}
//	// ids corresponds one-to-one with plans, with conflicted records having ID=SkippedConflictID
//	for i, id := range ids {
//	    if id == que.SkippedConflictID {
//	        fmt.Printf("Plan %d was skipped due to unique constraint conflict\n", i)
//	    }
//	}
func WithSkipConflict(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeySkipConflict{}, true)
}

// ShouldSkipConflict checks if the context has the skip unique constraint conflict flag
func ShouldSkipConflict(ctx context.Context) bool {
	v, ok := ctx.Value(ctxKeySkipConflict{}).(bool)
	return ok && v
}
