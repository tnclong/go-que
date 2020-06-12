package pg

var migrateSchemaSQL = `
create table if not exists goque_jobs
(
    id                   bigserial                                    not null
        constraint goque_jobs_pkey
            primary key,
    queue                text                                         not null
        constraint queue_length
            check (char_length(queue) > 0 AND char_length(queue) <= 100),
    args                 jsonb                    default '[]'::jsonb not null
        constraint valid_args
            check (jsonb_typeof(args) = 'array'::text),

    run_at               timestamp with time zone default now()       not null,
    retry_policy         jsonb                                        not null,
    done_at              timestamp with time zone,
    expired_at           timestamp with time zone,

    retry_count          integer                  default 0           not null,
    last_err_msg   text,
    last_err_stack text,

    unique_id            varchar(255),
    unique_lifecycle     smallint 
        constraint valid_unique_lifecycle 
            check(unique_lifecycle>=0 AND unique_lifecycle<=3),

    constraint err_length
        check ((char_length(last_err_msg) <= 512) AND (char_length(last_err_stack) <= 8192))
);

create index if not exists goque_jobs_lock_idx
    on goque_jobs (queue, run_at, id)
    where (done_at IS NULL AND expired_at IS NULL);

create unique index if not exists goque_jobs_unique_uidx on goque_jobs (queue, unique_id);
`

var setupConnSQL = `
CREATE TYPE pg_temp.goque_remaining_result AS
(
    locked    boolean,
    remaining integer
);

CREATE OR REPLACE FUNCTION pg_temp.goque_lock_and_decrease_remaining(remaining integer, job goque_jobs)
    RETURNS pg_temp.goque_remaining_result
AS
$$
WITH lock_taken AS (
    SELECT pg_try_advisory_lock(job.id) AS taken
)

SELECT (SELECT taken FROM lock_taken),
       CASE (SELECT taken FROM lock_taken)
           WHEN FALSE THEN
               remaining
           WHEN TRUE THEN
               remaining - 1
           END
$$
    STABLE
    LANGUAGE SQL;
`

var cleanupConnSQL = `
drop function pg_temp.goque_lock_and_decrease_remaining(integer, goque_jobs);
drop type pg_temp.goque_remaining_result;
`
