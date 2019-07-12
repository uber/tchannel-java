
package com.uber.tchannel.crossdock;

import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import com.uber.tchannel.crossdock.behavior.trace.TraceBehavior;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import com.uber.tchannel.tracing.TracingContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.InMemoryReporter;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.jaegertracing.spi.Sampler;
import io.netty.channel.ChannelFuture;
import io.opentracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static final String SERVER_NAME = "java";

    protected final TChannel tchannel;
    protected final Tracer tracer;
    protected final TracingContext tracingContext;

    protected TraceBehavior traceBehavior;

    public Server(String address, Tracer tracer) throws UnknownHostException {
        this.tracer = tracer;
        this.tracingContext = new TracingContext.ThreadLocal();

        tchannel = new TChannel.Builder(SERVER_NAME)
                .setServerHost(InetAddress.getByName(address))
                .setServerPort(8081)
                .setTracer(tracer)
                .setTracingContext(tracingContext)
                .build();
        tchannel.makeSubChannel(SERVER_NAME).register("echo", new JSONEchoHandler());
        logger.info("TChannel created at {}:{}", tchannel.getHost(), tchannel.getPort());
    }

    private static class JSONEchoHandler extends JSONRequestHandler<String, String> {
        @Override
        public JsonResponse<String> handleImpl(JsonRequest<String> request) {
            return new JsonResponse.Builder<String>(request)
                    .setTransportHeaders(request.getTransportHeaders())
                    .setBody(request.getBody(String.class))
                    .build();
        }
    }

    /**
     * @return future that completes once the channel is shut down
     */
    public ChannelFuture start() throws InterruptedException {
        traceBehavior = new TraceBehavior(tchannel);
        return tchannel.listen().channel().closeFuture();
    }

    public void shutdown() {
        tchannel.shutdown(false);
    }

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        InMemoryReporter reporter = new InMemoryReporter();
        Sampler sampler = new ConstSampler(true);
        Tracer tracer = new JaegerTracer.Builder(SERVER_NAME).withReporter(reporter).withSampler(sampler).build();

        HTTPServer httpServer = new HTTPServer();
        Thread httpThread = new Thread(httpServer);
        httpThread.setDaemon(true);
        httpThread.start();

        Server server = new Server("0.0.0.0", tracer);
        server.start().sync();
        server.shutdown();
    }

}
