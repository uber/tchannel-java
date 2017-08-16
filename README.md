# TChannel for JVM
[![Build Status](https://travis-ci.org/uber/tchannel-java.svg)](https://travis-ci.org/uber/tchannel-java)
[![codecov.io](https://codecov.io/github/uber/tchannel-java/coverage.svg)](https://codecov.io/github/uber/tchannel-java)

The Java implementation of the [TChannel](https://github.com/uber/tchannel) protocol.

#### Stability: *Stable*
[![stable](http://badges.github.io/stability-badges/dist/stable.svg)](http://github.com/badges/stability-badges)

## Example

```java
// create TChannel for server, and register a RequestHandler
TChannel server = new TChannel.Builder("ping-server").build();
server.makeSubChannel("ping-server")
	.register("ping-handler", new RawRequestHandler() {
        @Override
        public RawResponse handleImpl(RawRequest request) {
            return new RawResponse.Builder(request)
                .setTransportHeaders(request.getTransportHeaders())
                .setHeader("Polo")
                .setBody("Pong!")
                .build();
        }
	});

// listen for incoming connections
server.listen();

// create another TChannel for client.
TChannel client = new TChannel.Builder("ping-client").build();
RawRequest request = new RawRequest.Builder("ping-server", "ping-handler")
    .setHeader("Marco")
    .setBody("Ping!")
	.build();

// make an asynchronous request
TFuture<RawResponse> responseFuture = client
	.makeSubChannel("ping-server").send(
		request,
		server.getHost(),
		server.getListeningPort()
	);

// block and wait for the response
try (RawResponse response = responseFuture.get()) {
    System.out.println(response);
}

// shutdown the channel after done
server.shutdown();
client.shutdown();
```

## Overview

TChannel is a network protocol with the following goals:

 * request / response model
 * multiple requests multiplexed across the same TCP socket
 * out of order responses
 * streaming request and responses
 * all frames checksummed
 * transport arbitrary payloads
 * easy to implement in multiple languages
 * near-redis performance

This protocol is intended to run on datacenter networks for inter-process communication.

## Protocol

TChannel frames have a fixed length header and 3 variable length fields. The underlying protocol
does not assign meaning to these fields, but the included client/server implementation uses
the first field to represent a unique endpoint or function name in an RPC model.
The next two fields can be used for arbitrary data. Some suggested way to use the 3 fields are:

* URI path, HTTP method and headers as JSON, body
* function name, headers, thrift / protobuf

Note however that the only encoding supported by TChannel is UTF-8.  If you want JSON, you'll need
to stringify and parse outside of TChannel.

This design supports efficient routing and forwarding of data where the routing information needs
to parse only the first or second field, but the 3rd field is forwarded without parsing.

 - See [protocol.md](https://github.com/uber/tchannel/blob/master/docs/protocol.md) for more details

## Build

```bash
mvn clean package
```

## Run Tests
```bash
mvn clean test
```


## More Examples

See the [examples](./tchannel-example/).

Run Ping server/client example:
```bash
mvn package
# ping server
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingServer -p 8888

# ping client
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingClient -h localhost -p 8888 -n 1000
```

## Contributing

Pull requests *must* have thorough testing and be reviewed by at least one other party.
You *must* run [benchmarks](./tchannel-benchmark/src/main/java/com/uber/tchannel/benchmarks/)
to ensure there is no performance degradation.

## MIT Licenced
