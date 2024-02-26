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

	const values = "($%d::text, $%d::timestamptz, $%d::jsonb, $%d::jsonb, $%d, $%d::smallint)"
	args := make([]interface{}, 0, 6*len(plans))
	var b strings.Builder
	b.WriteString("INSERT INTO goque_jobs(queue, run_at, args, retry_policy, unique_id, unique_lifecycle) VALUES ")
	n := (len(values)+2)*len(plans) + 13
	b.Grow(n)
	fmt.Fprintf(&b, values, 1, 2, 3, 4, 5, 6)
	plan := plans[0]
	if err := normalize(&plan); err != nil {
		return nil, err
	}
	args = append(args, plan.Queue, plan.RunAt, plan.Args, jsonRetryPolicy(plan.RetryPolicy), plan.UniqueID, plan.UniqueLifecycle)
	i := 7
	for _, plan = range plans[1:] {
		b.WriteString(", ")
		fmt.Fprintf(&b, values, i, i+1, i+2, i+3, i+4, i+5)
		i += 6
		if err := normalize(&plan); err != nil {
			return nil, err
		}
		args = append(args, plan.Queue, plan.RunAt, plan.Args, jsonRetryPolicy(plan.RetryPolicy), plan.UniqueID, plan.UniqueLifecycle)
	}
	b.WriteString(" RETURNING id")
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
