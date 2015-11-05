# TChannel for the JVM [![Build Status](https://travis-ci.org/uber/tchannel-java.svg?branch=master)] (https://travis-ci.org/uber/tchannel-java)

A Java implementation of the [TChannel](https://github.com/uber/tchannel) protocol.

#### Stability: *Experimental*

## Completed
- Frame codecs
- Message codecs
- Checksumming
- Message Multiplexing a.k.a. fragment aggregation

## In Progress
- Tracing
- Error Handling
- Performance
- Final API design and implementation

## TODO
- Message Canceling
- Message Claiming
- Handling Errors properly

## Building
```bash
mvn clean package
```

## Running Tests
```bash
mvn clean test
```

## Example Usage

```java
// Create a TChannel, and register a RequestHandler
TChannel tchannel = new TChannel.Builder("ping-server")
	.setServerPort(this.port)
	.build();
tchannel.makeSubChannel("json-server")
	.register("ping-handler", new PingRequestHandler());

// Listen for incoming connections
tchannel.listen();

// Create another TChannel to act as a client.
TChannel tchannelClient = new TChannel.Builder("ping-client").build();
JsonRequest<Ping> request = new JsonRequest.Builder<Ping>(new Ping("ping?"))
	.setEndpoint("ping-handler")
	.build();

// Make an asynchronous request
ListenableFuture<JsonResponse<Pong>> responseFuture = tchannel
	.makeSubChannel("json-service").send(
		request,
		tchannel.getHost(),
		tchannel.getListeningPort()
	);

// Block and wait for the response
JsonResponse<Pong> response = responseFuture.get();
System.out.println(response);
response.release();
```

## Run The Examples
#### PingServer
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingServer -p 8888
```

#### PingClient
```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingClient -h localhost -p 8888 -n 1000
```

## Contributing

Contributions are welcome and encouraged! Please push contributions to branches namespaced by your username and then
create a pull request. Pull requests *must* have thorough testing and be reviewed by at least one other party. 

## MIT Licenced
