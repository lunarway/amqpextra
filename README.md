# Extra features for streadway/amqp package. 

## Auto reconnecting.

The package provides an auto reconnect feature for [streadway/amqp](https://github.com/streadway/amqp). The approach idea is to add as little abstraction as possible. In your code instead of using `*amqp.Connection` you should use `<-chan *amqp.Connection`. The channel returns a healthy connection. You should subscribe to `chan *amqp.Error` to get notified when a connection is not helthy any more and you should request a new one via  `<-chan *amqp.Connection`. The channel `<-chan *amqp.Connection` is closed when you explicitly closed it by calling `connextra.Close()` method, otherwise, it tries to reconnect in background.

See an [example](examples/conn_example.go). 

