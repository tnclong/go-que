# Hello World example

To start the postgres database with `docker-compose`

```
cd ../.. && docker-compose up
```

To start worker

```
$ go run worker.go
```

You can also start multiple worker in separate terminal,
Workers and triggers are connected through database

Then run the trigger to register jobs to let the worker run

```
$ go run trigger.go
```

You will see in the multiple worker terminal, that the jobs are done.

