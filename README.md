Que

## Introduce

## Install

## Doc

## Quickstart
    docker-compose up
    
    PostgreSQL:
        docker exec -i go-que_postgres_1 psql -U myuser -d mydb < pg/migrate1.sql
        source pg_env

    ord/api.go
    ord/worker.go

## Benchmark

    1. Guide performace improvement.
    2. Obtain a real WorkerOptions on production with accepted database resource wastage.
