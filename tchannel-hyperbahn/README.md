TChannel Hyperbahn
=================

Example use case
--------------------

``` java
# Create a TChannel first
TChannel tchannel = new TChannel.Builder("ping-server")
                    .register("ping", new PingRequestHandler())
                    .setServerPort(this.port)
                    .build();

# Create a hyperbahn client and add the created tchannel to it
HyperbahnClient client = new HyperbahnClient.Builder()
                         .addChannel(tchannel)
                         .build();

# Advertise the tchannel
client.advertise();

```

## MIT Licenced
