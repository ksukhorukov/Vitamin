# Vitamin

Blazingly fast key-value storage

## Features

- Simple interface
- Fault tolerance
- Master-master replication

## Dependencies

- Golang 1.10.4
- RabbitMQ 3.6.10

## Build & Run

```bash
go get github.com/streadway/amqp
go build ./vitamin.go
./vitamin
```
## Use

Store "test_key" for 60 seconds

```bash
nc localhost 8080
set("test_key","test value",60) 
test value
```
Get "test_key"

```bash
get("test_key")
test value
```
## Help

```bash
./vitamin --help
```

## [EOF]
