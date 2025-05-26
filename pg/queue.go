package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tnclong/go-que"
)

func New(db *sql.DB) (que.Queue, error) {
	return NewWithOptions(Options{DB: db, DBMigrate: true})
}

func validateOptions(opts Options) error {
	if opts.DB == nil {
		return errors.New("Options.DB must not be nil")
	}
	return nil
}

func NewWithOptions(opts Options, optFns ...func(*Options)) (que.Queue, error) {
	opts = opts.Copy()
	for _, optFn := range optFns {
		optFn(&opts)
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}

	if opts.DBMigrate {
		if err := Migrate(opts.DB); err != nil {
			return nil, err
		}
	}

	return &queue{db: opts.DB}, nil
}

type Options struct {
	DB *sql.DB

	// Run database migrations
	DBMigrate bool
}

func (o Options) Copy() Options {
	o2 := o
	return o2
}

func Migrate(db *sql.DB) error {
	_, err := db.Exec(migrateSchemaSQL)
	return err
}

type queue struct {
	db *sql.DB
}

var emptyArgs = []byte{'[', ']'}

func (q *queue) Enqueue(ctx context.Context, tx *sql.Tx, plans ...que.Plan) (ids []int64, err error) {
	if len(plans) == 0 {
		return nil, nil
	}

	// Normalize all plans first
	for i := range plans {
		if err := normalize(&plans[i]); err != nil {
			return nil, err
		}
	}

	skipConflict := que.ShouldSkipConflict(ctx)

	// Handle duplicate UniqueIDs in same request to avoid PostgreSQL error
	// "ON CONFLICT DO UPDATE command cannot affect row a second time"
	if skipConflict {
		type compositeKey struct {
			queue    string
			uniqueID string
		}

		uniquePlans := make([]que.Plan, 0, len(plans))
		seenKeys := make(map[compositeKey]bool)
		skippedIndexes := make(map[int]bool)

		for i, plan := range plans {
			// Plans with Ignore lifecycle or nil UniqueID always get processed
			if plan.UniqueLifecycle == que.Ignore || plan.UniqueID == nil {
				uniquePlans = append(uniquePlans, plan)
				continue
			}

			key := compositeKey{
				queue:    plan.Queue,
				uniqueID: *plan.UniqueID,
			}

			if seenKeys[key] {
				skippedIndexes[i] = true
				continue
			}

			seenKeys[key] = true
			uniquePlans = append(uniquePlans, plan)
		}

		// If we found duplicates, prepare the result
		if len(skippedIndexes) > 0 {
			processedIDs, err := q.doEnqueue(ctx, tx, skipConflict, uniquePlans...)
			if err != nil {
				return nil, err
			}

			finalIDs := make([]int64, len(plans))
			processedIdx := 0
			for i := range plans {
				if skippedIndexes[i] {
					finalIDs[i] = que.SkippedConflictID
				} else {
					finalIDs[i] = processedIDs[processedIdx]
					processedIdx++
				}
			}

			return finalIDs, nil
		}
	}

	return q.doEnqueue(ctx, tx, skipConflict, plans...)
}

var (
	sqlStandardReturning = " RETURNING id"

	sqlSkipConflictReturning = " ON CONFLICT (queue, unique_id) DO UPDATE SET id = goque_jobs.id" +
		fmt.Sprintf(" RETURNING CASE WHEN xmax = 0 THEN id ELSE %d END AS id", que.SkippedConflictID)

	growDelta = len(sqlSkipConflictReturning) - len(sqlStandardReturning)
)

func (q *queue) doEnqueue(ctx context.Context, tx *sql.Tx, skipConflict bool, plans ...que.Plan) (ids []int64, err error) {
	const values = "($%d::text, $%d::timestamptz, $%d::jsonb, $%d::jsonb, $%d, $%d::smallint)"
	args := make([]interface{}, 0, 6*len(plans))
	var b strings.Builder
	b.WriteString("INSERT INTO goque_jobs(queue, run_at, args, retry_policy, unique_id, unique_lifecycle) VALUES ")
	n := (len(values)+2)*len(plans) + 13
	if skipConflict {
		n += growDelta
	}
	b.Grow(n)
	fmt.Fprintf(&b, values, 1, 2, 3, 4, 5, 6)
	plan := plans[0]
	args = append(args, plan.Queue, plan.RunAt, plan.Args, jsonRetryPolicy(plan.RetryPolicy), plan.UniqueID, plan.UniqueLifecycle)
	i := 7
	for _, plan = range plans[1:] {
		b.WriteString(", ")
		fmt.Fprintf(&b, values, i, i+1, i+2, i+3, i+4, i+5)
		i += 6
		args = append(args, plan.Queue, plan.RunAt, plan.Args, jsonRetryPolicy(plan.RetryPolicy), plan.UniqueID, plan.UniqueLifecycle)
	}

	if skipConflict {
		b.WriteString(sqlSkipConflictReturning)
	} else {
		b.WriteString(sqlStandardReturning)
	}

	rows, err := q.query(tx)(ctx, b.String(), args...)
	if err != nil {
		if strings.HasSuffix(err.Error(), `unique constraint "goque_jobs_unique_uidx"`) {
			return nil, &que.ErrQueue{Err: que.ErrViolateUniqueConstraint}
		}
		return nil, err
	}
	defer rows.Close()
	ids = make([]int64, 0, len(plans))
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, err
}

func normalize(plan *que.Plan) error {
	if len(plan.Args) == 0 {
		plan.Args = emptyArgs
	}
	if plan.UniqueLifecycle == que.Ignore {
		plan.UniqueID = nil
	} else {
		if plan.UniqueID == nil {
			return errors.New("unique id is required when unique lifecycle is always or done or lockable")
		}
	}
	return nil
}

func (q *queue) Mutex() que.Mutex {
	return &mutex{db: q.db, ids: make(map[int64]bool)}
}
