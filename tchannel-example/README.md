TChannel Examples
=================

Running the Examples
--------------------

```bash
mvn package
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingServer --port 8888
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
java -cp tchannel-example/target/tchannel-example.jar com.uber.tchannel.ping.PingClient --port 8888
#Connecting from client to server on port: 8888
#<RawResponse id=42 transportHeaders={as=json} arg1=ping arg2={} arg3={"response":"pong!"}>
#Stopping Client...
```

## MIT Licenced
