package com.uber.tchannel.tracing;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.channels.Connection;
import io.opentracing.Tracer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * TChannel test contraption.
 *
 * @author yegor 2017-11-10.
 */

public class TestChannel implements AutoCloseable {

    private static final @NotNull InetAddress localhost = loopback();

    private final @NotNull String name;

    private final @NotNull ExecutorService executor;

    private final @NotNull TChannel tchannel;

    private final @NotNull SubChannel subChannel;

    private final @NotNull List<InetSocketAddress> addresses;

    public TestChannel(
        @NotNull String name,
        @Nullable Tracer tracer,
        @Nullable TracingContext tracingContext
    ) throws InterruptedException {
        this.name = name;
        executor = Executors.newSingleThreadExecutor();

        TChannel.Builder tcBuilder = new TChannel.Builder(name)
            .setServerHost(localhost)
            .setServerPort(0)
            .setExecutorService(executor);
        if (tracer != null) {
            tcBuilder.setTracer(tracer);
        }
        if (tracingContext != null) {
            tcBuilder.setTracingContext(tracingContext);
        }
        tchannel = tcBuilder.build();

        subChannel = tchannel.makeSubChannel(name, Connection.Direction.IN);
        tchannel.listen().sync();
        addresses = Collections.singletonList(new InetSocketAddress(localhost, tchannel.getListeningPort()));
    }

    private static @NotNull InetAddress loopback() {
        try {
            return InetAddress.getByAddress(new byte[]{ 127, 0, 0, 1 });
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    public @NotNull String name() {
        return name;
    }

    public @NotNull ExecutorService executor() {
        return executor;
    }

    public @NotNull List<InetSocketAddress> addresses() {
        return addresses;
    }

    public void register(@NotNull String endpoint, RequestHandler handler) {
        subChannel.register(endpoint, handler);
    }

    public @NotNull SubChannel clientChannel(
        @NotNull String peerName,
        @NotNull List<InetSocketAddress> peerAddresses
    ) {
        return tchannel
            .makeSubChannel(peerName, Connection.Direction.OUT)
            .setPeers(peerAddresses);
    }

    @Override
    public void close() {
        tchannel.shutdown();
        executor.shutdown();
    }

}
