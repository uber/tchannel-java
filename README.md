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

## Example Usage

```java
// Create a TChannel, and register a RequestHandler
TChannel tchannel = new TChannel.Builder("ping-server")
	.register("ping-handler", new PingRequestHandler())
	.setServerPort(this.port)
	.build();

// Listen for incoming connections
tchannel.listen();

// Create another TChannel to act as a client.
TChannel tchannelClient = new TChannel.Builder("ping-client").build();
Request<Ping> request = new Request.Builder<>(new Ping("ping?"))
	.setEndpoint("ping-handler")
	.build();

// Make an asynchronous request
Promise<Response<Pong>> responseFuture = tchannel.callJSON(
	tchannel.getHost(),
	tchannel.getServerPort(),
	"service",
	request,
	Pong.class
);

// Block and wait for the response
Response<Pong> response = responseFuture.get(100, TimeUnit.MILLISECONDS);
System.out.println(response);
```

## Run The Examples
#### PingServer
```bash
mvn package
java -cp tchannel-example/target/tchannel-example-1.0-SNAPSHOT.jar com.uber.tchannel.ping.PingServer -p 8888
```

#### PingClient
```bash
mvn package
java -cp tchannel-example/target/tchannel-example-1.0-SNAPSHOT.jar com.uber.tchannel.ping.PingClient -h localhost -p 8888 -n 1000
```

## Contributing

Contributions are welcome and encouraged! Please push contributions to branches namespaced by your username and then
create a pull request. Pull requests *must* have thorough testing and be reviewed by at least one other party. 

## MIT Licenced
