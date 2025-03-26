package que_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/tnclong/go-que"
)

// TestSkipConflict tests handling of unique constraint conflicts in a batch.
// When using a context created with WithSkipConflict, conflicting records should
// be skipped instead of returning an error.
func TestSkipConflict(t *testing.T) {
	q := newQueue()
	qs := randQueue()
	uniqueIDStr := "test-unique-id"
	uniqueID := &uniqueIDStr

	// Create a plan with a unique ID for testing
	plan1 := que.Plan{
		Queue:           qs,
		Args:            que.Args("initial job"),
		RunAt:           time.Now(),
		UniqueID:        uniqueID,
		UniqueLifecycle: que.Always,
	}

	// Each test part uses independent transactions to avoid transaction interruption issues

	// Part 1: Initial job insertion
	dbTx(t, false, func(tx *sql.Tx) {
		ids, err := q.Enqueue(context.Background(), tx, plan1)
		if err != nil {
			t.Fatalf("Failed to enqueue initial job: %v", err)
		}
		if len(ids) != 1 {
			t.Fatalf("Expected 1 ID, got %d", len(ids))
		}
		t.Logf("Initial job enqueued with ID: %d", ids[0])
	})

	// Part 2: Testing conflict without WithSkipConflict
	plan2 := que.Plan{
		Queue:           qs,
		Args:            que.Args("conflicting job"),
		RunAt:           time.Now(),
		UniqueID:        uniqueID, // Same unique ID, will cause conflict
		UniqueLifecycle: que.Always,
	}

	dbTx(t, true, func(tx *sql.Tx) {
		// Test 1: Without WithSkipConflict, should return an error
		_, err := q.Enqueue(context.Background(), tx, plan2)
		if err == nil {
			t.Fatalf("Expected error due to unique constraint violation, but got nil")
		}

		if !errors.Is(err, que.ErrViolateUniqueConstraint) {
			t.Fatalf("Expected que.ErrViolateUniqueConstraint, got: %v", err)
		}
		t.Logf("Test 1 passed: Got expected unique constraint error")
	})

	// Part 3: Testing conflict with WithSkipConflict
	dbTx(t, true, func(tx *sql.Tx) {
		anotherIDStr := "another-id"
		anotherID := &anotherIDStr
		plan3 := que.Plan{
			Queue:           qs,
			Args:            que.Args("non-conflicting job"),
			RunAt:           time.Now(),
			UniqueID:        anotherID,
			UniqueLifecycle: que.Always,
		}

		// Test 2: With WithSkipConflict, should not return error and conflicting records should be marked
		ctx := que.WithSkipConflict(context.Background())
		ids, err := q.Enqueue(ctx, tx, plan2, plan3)
		if err != nil {
			t.Fatalf("Failed with SkipConflict enabled: %v", err)
		}

		if len(ids) != 2 {
			t.Fatalf("Expected 2 IDs, got %d", len(ids))
		}

		// First ID should be SkippedID, second should be valid
		if ids[0] != que.SkippedID {
			t.Errorf("Expected first ID to be SkippedID (%d), got %d", que.SkippedID, ids[0])
		}

		if ids[1] == que.SkippedID {
			t.Errorf("Expected second ID to be valid, got SkippedID (%d)", que.SkippedID)
		}

		t.Logf("Test 2 passed: SkipConflict correctly handled conflicting records")
	})

	// Part 4: Testing multiple case scenarios - Note each test runs in an independent transaction
	dbTx(t, true, func(tx *sql.Tx) {
		thirdIDStr := "third-id"
		thirdID := &thirdIDStr
		anotherIDStr := "another-id"
		anotherID := &anotherIDStr

		plan4 := que.Plan{
			Queue:           qs,
			Args:            que.Args("yet another job"),
			RunAt:           time.Now(),
			UniqueID:        thirdID, // New unique ID, should succeed
			UniqueLifecycle: que.Always,
		}

		plan5 := que.Plan{
			Queue:           qs,
			Args:            que.Args("another conflicting job"),
			RunAt:           time.Now(),
			UniqueID:        uniqueID, // Same unique ID, will cause conflict with plan1
			UniqueLifecycle: que.Always,
		}

		plan6 := que.Plan{
			Queue:           qs,
			Args:            que.Args("one more job"),
			RunAt:           time.Now(),
			UniqueID:        anotherID, // Should not conflict since we're in a new transaction
			UniqueLifecycle: que.Always,
		}

		// Multiple plans with various IDs
		ctx := que.WithSkipConflict(context.Background())
		ids, err := q.Enqueue(ctx, tx, plan4, plan5, plan6)
		if err != nil {
			t.Fatalf("Failed with multiple plans: %v", err)
		}

		if len(ids) != 3 {
			t.Fatalf("Expected 3 IDs, got %d", len(ids))
		}

		if ids[0] == que.SkippedID {
			t.Errorf("Expected position 1 to be a valid ID, got SkippedID")
		}

		if ids[1] != que.SkippedID {
			t.Errorf("Expected position 0 to be SkippedID (%d), got %d", que.SkippedID, ids[0])
		}

		if ids[2] == que.SkippedID {
			t.Errorf("Expected position 2 to be a valid ID, got SkippedID")
		}

		t.Logf("Test 3 passed: Multiple test cases correctly handled")
	})

	// Part 5: Testing duplicate unique IDs within the same Enqueue call
	dbTx(t, true, func(tx *sql.Tx) {
		duplicateIDStr := "duplicate-id"
		duplicateID := &duplicateIDStr

		// Create two plans with the same unique ID
		planDup1 := que.Plan{
			Queue:           qs,
			Args:            que.Args("first duplicate job"),
			RunAt:           time.Now(),
			UniqueID:        duplicateID,
			UniqueLifecycle: que.Always,
		}

		planDup2 := que.Plan{
			Queue:           qs,
			Args:            que.Args("second duplicate job"),
			RunAt:           time.Now(),
			UniqueID:        duplicateID, // Same unique ID as planDup1
			UniqueLifecycle: que.Always,
		}

		randomIDStr := "random-id"
		randomID := &randomIDStr
		planRandom := que.Plan{
			Queue:           qs,
			Args:            que.Args("non-duplicate job"),
			RunAt:           time.Now(),
			UniqueID:        randomID,
			UniqueLifecycle: que.Always,
		}

		// Test with a single call containing duplicate UniqueIDs
		// With our new implementation, this should work and mark duplicates as skipped
		ctx := que.WithSkipConflict(context.Background())
		ids, err := q.Enqueue(ctx, tx, planDup1, planDup2, planRandom)
		// We expect no error with the new implementation
		if err != nil {
			t.Fatalf("Failed to enqueue plans with duplicate UniqueIDs: %v", err)
		}

		// Should return 3 IDs
		if len(ids) != 3 {
			t.Fatalf("Expected 3 IDs, got %d", len(ids))
		}

		// First ID should be valid (not skipped)
		if ids[0] == que.SkippedID {
			t.Errorf("Expected first ID to be valid, got SkippedID")
		}

		// Second ID (duplicate) should be marked as skipped
		if ids[1] != que.SkippedID {
			t.Errorf("Expected second ID (duplicate) to be SkippedID, got %d", ids[1])
		}

		// Third ID (random) should be valid
		if ids[2] == que.SkippedID {
			t.Errorf("Expected third ID to be valid, got SkippedID")
		}

		t.Logf("Test 4 passed: Successfully handled duplicate UniqueIDs within the same Enqueue call")
	})

	// Part 6: Testing multiple duplicate IDs to ensure they're all handled correctly
	dbTx(t, true, func(tx *sql.Tx) {
		// Create plans with various patterns of duplicate IDs
		id1Str := "multi-test-id-1"
		id1 := &id1Str
		id2Str := "multi-test-id-2"
		id2 := &id2Str

		// We'll create 7 plans with various patterns:
		// - Plans 0, 2, 5 have the same ID (id1)
		// - Plans 1, 3 have the same ID (id2)
		// - Plans 4, 6 have nil ID (should always be processed)
		plans := []que.Plan{
			{Queue: qs, Args: que.Args("first id1"), RunAt: time.Now(), UniqueID: id1, UniqueLifecycle: que.Always},
			{Queue: qs, Args: que.Args("first id2"), RunAt: time.Now(), UniqueID: id2, UniqueLifecycle: que.Always},
			{Queue: qs, Args: que.Args("second id1"), RunAt: time.Now(), UniqueID: id1, UniqueLifecycle: que.Lockable},
			{Queue: qs, Args: que.Args("second id2"), RunAt: time.Now(), UniqueID: id2, UniqueLifecycle: que.Always},
			{Queue: qs, Args: que.Args("first nil id"), RunAt: time.Now(), UniqueID: nil, UniqueLifecycle: que.Ignore},
			{Queue: qs, Args: que.Args("third id1"), RunAt: time.Now(), UniqueID: id1, UniqueLifecycle: que.Done},
			{Queue: qs, Args: que.Args("second nil id"), RunAt: time.Now(), UniqueID: nil, UniqueLifecycle: que.Ignore},
		}

		// With our new implementation, this should work with duplicates marked as skipped
		ctx := que.WithSkipConflict(context.Background())
		ids, err := q.Enqueue(ctx, tx, plans...)
		if err != nil {
			t.Fatalf("Failed to enqueue plans with multiple duplicate IDs: %v", err)
		}

		// Should have 7 IDs, matching our 7 plans
		if len(ids) != 7 {
			t.Fatalf("Expected 7 IDs, got %d", len(ids))
		}

		// Expected results:
		// - Position 0: Valid ID (first occurrence of id1)
		// - Position 1: Valid ID (first occurrence of id2)
		// - Position 2: SkippedID (duplicate of id1)
		// - Position 3: SkippedID (duplicate of id2)
		// - Position 4: Valid ID (first nil ID - always processed)
		// - Position 5: SkippedID (duplicate of id1)
		// - Position 6: Valid ID (second nil ID - always processed)
		expectedResults := []bool{
			false, // Not skipped (first of id1)
			false, // Not skipped (first of id2)
			true,  // Skipped (duplicate of id1)
			true,  // Skipped (duplicate of id2)
			false, // Not skipped (first nil ID)
			true,  // Skipped (duplicate of id1)
			false, // Not skipped (second nil ID)
		}

		for i, isSkipped := range expectedResults {
			if isSkipped {
				if ids[i] != que.SkippedID {
					t.Errorf("Expected position %d to be SkippedID, got %d", i, ids[i])
				}
			} else {
				if ids[i] == que.SkippedID {
					t.Errorf("Expected position %d to be a valid ID, got SkippedID", i)
				}
			}
		}

		t.Logf("Test 5 passed: Successfully handled multiple duplicate IDs in complex pattern")
	})

	// Part 7: Testing same uniqueID across multiple queues
	dbTx(t, true, func(tx *sql.Tx) {
		// Create a new queue name
		qs2 := randQueue()

		// We'll use the same uniqueID for both queues
		sharedUniqueIDStr := "shared-unique-id-across-queues"
		sharedUniqueID := &sharedUniqueIDStr

		// First plan for queue 1
		planQ1 := que.Plan{
			Queue:           qs,
			Args:            que.Args("job in queue 1"),
			RunAt:           time.Now(),
			UniqueID:        sharedUniqueID,
			UniqueLifecycle: que.Always,
		}

		// Second plan for queue 2 with same uniqueID
		planQ2 := que.Plan{
			Queue:           qs2,
			Args:            que.Args("job in queue 2"),
			RunAt:           time.Now(),
			UniqueID:        sharedUniqueID, // Same uniqueID but different queue
			UniqueLifecycle: que.Always,
		}

		// Enqueue in queue 1
		ids1, err := q.Enqueue(context.Background(), tx, planQ1)
		if err != nil {
			t.Fatalf("Failed to enqueue job in first queue: %v", err)
		}
		if len(ids1) != 1 {
			t.Fatalf("Expected 1 ID for first queue, got %d", len(ids1))
		}
		if ids1[0] == que.SkippedID {
			t.Errorf("Expected valid ID for queue 1, got SkippedID")
		}
		t.Logf("Successfully enqueued job in queue 1 with ID: %d", ids1[0])

		// Enqueue in queue 2 with same uniqueID
		ids2, err := q.Enqueue(context.Background(), tx, planQ2)
		if err != nil {
			t.Fatalf("Failed to enqueue job in second queue: %v", err)
		}
		if len(ids2) != 1 {
			t.Fatalf("Expected 1 ID for second queue, got %d", len(ids2))
		}
		if ids2[0] == que.SkippedID {
			t.Errorf("Expected valid ID for queue 2, got SkippedID")
		}
		t.Logf("Successfully enqueued job in queue 2 with ID: %d", ids2[0])

		// Now try to enqueue again in both queues with same uniqueID using SkipConflict
		ctx := que.WithSkipConflict(context.Background())

		// Create plans identical to the previous ones
		planQ1Dup := que.Plan{
			Queue:           qs,
			Args:            que.Args("duplicate job in queue 1"),
			RunAt:           time.Now(),
			UniqueID:        sharedUniqueID,
			UniqueLifecycle: que.Always,
		}

		planQ2Dup := que.Plan{
			Queue:           qs2,
			Args:            que.Args("duplicate job in queue 2"),
			RunAt:           time.Now(),
			UniqueID:        sharedUniqueID,
			UniqueLifecycle: que.Always,
		}

		// Create a different uniqueID for additional plans
		anotherSharedIDStr := "another-shared-unique-id"
		anotherSharedID := &anotherSharedIDStr

		// Additional plans for both queues with new uniqueID
		planQ1New := que.Plan{
			Queue:           qs,
			Args:            que.Args("new job in queue 1"),
			RunAt:           time.Now(),
			UniqueID:        anotherSharedID,
			UniqueLifecycle: que.Always,
		}

		planQ2New := que.Plan{
			Queue:           qs2,
			Args:            que.Args("new job in queue 2"),
			RunAt:           time.Now(),
			UniqueID:        anotherSharedID,
			UniqueLifecycle: que.Always,
		}

		// Another plan with same queue and uniqueID as planQ1New (should be skipped)
		planQ1NewDup := que.Plan{
			Queue:           qs, // Same queue as planQ1New
			Args:            que.Args("duplicate of new job in queue 1"),
			RunAt:           time.Now(),
			UniqueID:        anotherSharedID, // Same uniqueID as planQ1New
			UniqueLifecycle: que.Always,
		}

		// Test all five plans in a single call with SkipConflict
		// - Plans 0-1: should be skipped (conflict with existing database records)
		// - Plan 2: should be valid (new uniqueID in queue 1)
		// - Plan 3: should be valid (new uniqueID in queue 2)
		// - Plan 4: should be skipped (duplicate of Plan 2 in the same call)
		ids, err := q.Enqueue(ctx, tx, planQ1Dup, planQ2Dup, planQ1New, planQ2New, planQ1NewDup)
		if err != nil {
			t.Fatalf("Failed to enqueue jobs with SkipConflict: %v", err)
		}

		if len(ids) != 5 {
			t.Fatalf("Expected 5 IDs, got %d", len(ids))
		}

		// First two should be skipped since they conflict in their respective queues
		if ids[0] != que.SkippedID {
			t.Errorf("Expected SkippedID for duplicate in queue 1, got %d", ids[0])
		}

		if ids[1] != que.SkippedID {
			t.Errorf("Expected SkippedID for duplicate in queue 2, got %d", ids[1])
		}

		// Plan 2 should be valid since it's the first occurrence of anotherSharedID in queue 1
		if ids[2] == que.SkippedID {
			t.Errorf("Expected valid ID for new job in queue 1, got SkippedID")
		}

		// Plan 3 should be valid since it's the first occurrence of anotherSharedID in queue 2
		if ids[3] == que.SkippedID {
			t.Errorf("Expected valid ID for new job in queue 2, got SkippedID")
		}

		// Plan 4 should be skipped as it's a duplicate of Plan 2 within the same call
		if ids[4] != que.SkippedID {
			t.Errorf("Expected SkippedID for duplicate of new job in queue 1, got %d", ids[4])
		}

		t.Logf("Test 6 passed: Successfully verified cross-queue uniqueID behavior")
	})
}
