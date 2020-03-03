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

## Master-Master replication and fault-tollerant quorum

It is easy to distribute work between several instances of Vitamin located at different servers with help of out of the box Master-Master replication.

Let's suppose we have four severs: vitamin_first_node (192.168.10.10), vitamin_second_node (192.168.10.11), server with RabbitMQ (192.168.10.12) and load balancer server (192.168.10.13).

For the sace of simplicity we will use HAProxy for load balancing.  Example configuration can be found [here](https://github.com/ksukhorukov/Vitamin/blob/master/haproxy/haproxy.cfg)

Run RabbitMQ(192.168.10.12):

```bash
/etc/init.d/rabbitmq-server start
```

Run first server (192.168.10.10):

```bash
./vitamin --rabbitmq_host=192.168.0.12 --rabbitmq_port=5672 --rabbitmq_user=guest --rabbitmq_password=guest --rabbitmq_exchange=vitamin
```

Run second server (192.168.10.11):

```bash
./vitamin --rabbitmq_host=192.168.0.12 --rabbitmq_port=5672 --rabbitmq_user=guest --rabbitmq_password=guest --rabbitmq_exchange=vitamin
```

Run Nth server (192.168.10.N):

```bash
./vitamin --rabbitmq_host=192.168.0.12 --rabbitmq_port=5672 --rabbitmq_user=guest --rabbitmq_password=guest --rabbitmq_exchange=vitamin
```

Run load-balancer (192.168.10.13):

```bash
/etc/init.d/haproxy start
```

For now all request to load-balancer (192.168.10.13:8080) will be evenly distributed across cache servers.

## Help

```bash
./vitamin --help
```

## [EOF]
