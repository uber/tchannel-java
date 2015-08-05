# TChannel for the JVM

A Java implementation of the [TChannel](https://github.com/uber/tchannel) protocol.

#### Stability: *Experimental*

## Completed
- Frame codecs
- Message codecs
- Checksumming
- Message Multiplexing a.k.a. fragment aggregation

## In Progress
- Tracing

## TODO
- Final API design and implementation
- Message Canceling
- Message Claiming
- Handling Errors properly
- Performance

## *Proposed* Example Usage

```java
TChannel server = new TChannel('server');
server.register('noop', new RawRequestHandler() {
    @Override
    public void onRequest(Stream s, Request req) {
        System.out.println(req);
    }
});
ChannelFuture f = server.listen('127.0.0.1:8888');
f.addListener(ChannelFutureListener.CLOSE_ON_FAILURE)

TChannel client = new TChannel('client');
ChannelFuture response = client.request('127.0.0.1:8888', 'noop', new RawRequest('func1', 'arg1', 'arg2'));
response.addListener(new ChannelFutureListener {
    @Override
    public void operationComplete(ChannelFuture f) {
        System.out.println(f.isSuccess());
    }
});
```

## Run The Examples
#### PingServer
```bash
mvn package
java -cp tchannel-example/target/tchannel-example-1.0-SNAPSHOT.jar com.uber.tchannel.ping.PingServer
# Starting server on port: 8888
# Jul 29, 2015 10:28:07 AM io.netty.handler.logging.LoggingHandler channelRegistered
# INFO: [id: 0x8b9e9085] REGISTERED
# Jul 29, 2015 10:28:07 AM io.netty.handler.logging.LoggingHandler bind
# INFO: [id: 0x8b9e9085] BIND: 0.0.0.0/0.0.0.0:8888
# Jul 29, 2015 10:28:07 AM io.netty.handler.logging.LoggingHandler channelActive
# INFO: [id: 0x8b9e9085, /0:0:0:0:0:0:0:0:8888] ACTIVE
```

#### PingClient
```bash
mvn package
java -cp tchannel-example/target/tchannel-example-1.0-SNAPSHOT.jar com.uber.tchannel.ping.PingClient
#Connecting from client to server on port: 8888
#<com.uber.tchannel.messages.InitResponse id=42 version=2 hostPort=0.0.0.0:0 processName=test-process>
#com.uber.tchannel.messages.PingResponse@18ec223
#Stopping Client...
```

## Contributing

Contributions are welcome and encouraged! Please push contributions to branches namespaced by your username and then create a pull request. Pull requests *must* have thorough testing and be reviewed by at least one other party. 

## MIT Licenced
