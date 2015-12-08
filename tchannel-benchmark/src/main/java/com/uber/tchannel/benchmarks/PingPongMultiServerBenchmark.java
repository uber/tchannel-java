/*
 * Copyright (c) 2015 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.uber.tchannel.benchmarks;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Thread)
public class PingPongMultiServerBenchmark {

    private List<TChannel> servers = new ArrayList<>();
    private TChannel client;
    private SubChannel subClient;

    private int connections = 3;

    private NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private NioEventLoopGroup childGroup = new NioEventLoopGroup();

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(".*" + PingPongMultiServerBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(30)
            .measurementIterations(50)
            .forks(1)
            .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        BasicConfigurator.configure();
        LogManager.getRootLogger().setLevel(org.apache.log4j.Level.INFO);

        createServers();
        this.client = new TChannel.Builder("ping-client")
            .setClientMaxPendingRequests(150000)
            .setBossGroup(bossGroup)
            .setChildGroup(childGroup)
            .build();
        this.subClient = this.client.makeSubChannel("ping-server");
        List<InetSocketAddress> peers = new ArrayList<>();
        List<Connection> conns = new ArrayList<>();
        for (int i = 0; i < connections; i++) {
            TChannel server = servers.get(i);
            InetSocketAddress address = new InetSocketAddress(server.getHost(), server.getListeningPort());
            peers.add(address);
            conns.add(subClient.getPeerManager().connectTo(address));
        }

        this.subClient.setPeers(peers);
        for (Connection conn : conns) {
            conn.waitForIdentified(120000);
        }
    }

    protected void createServers() throws Exception {
        for (int i = 0; i < connections; i++) {
            TChannel server = new TChannel.Builder("ping-server")
                .setServerHost(InetAddress.getByName("127.0.0.1"))
                .setBossGroup(bossGroup)
                .setChildGroup(childGroup)
                .build();
            server.makeSubChannel("ping-server").register("ping", new PingDefaultRequestHandler());
            server.listen();
            servers.add(server);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark(final AdditionalCounters counters) throws Exception {
        JsonRequest<Ping> request = new JsonRequest.Builder<Ping>("ping-server", "ping")
            .setBody(new Ping("ping?"))
            .setTimeout(20000)
            .setRetryLimit(0)
            .build();

        TFuture<JsonResponse<Pong>> future = this.subClient.send(request);
        future.addCallback(new TFutureCallback<JsonResponse<Pong>>() {
            @Override
            public void onResponse(JsonResponse<Pong> pongResponse) {
                if (!pongResponse.isError()) {
                    counters.actualQPS.incrementAndGet();
                    pongResponse.release();
                } else {
                    counters.errorQPS.incrementAndGet();
                }
            }
        });
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        this.client.shutdown(false);

        for (TChannel server : servers) {
            server.shutdown(false);
        }
    }

    public class Ping {
        private final String request;

        public Ping(String request) {
            this.request = request;
        }
    }

    public class Pong {
        private final String response;

        public Pong(String response) {
            this.response = response;
        }
    }

    public class PingDefaultRequestHandler extends JSONRequestHandler<Ping, Pong> {

        public JsonResponse<Pong> handleImpl(JsonRequest<Ping> request) {
            return new JsonResponse.Builder<Pong>(request)
                .setBody(new Pong("pong!"))
                .build();
        }
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class AdditionalCounters {
        public AtomicInteger actualQPS = new AtomicInteger(0);
        public AtomicInteger errorQPS = new AtomicInteger(0);

        @Setup(Level.Iteration)
        public void clean() {
            errorQPS.set(0);
            actualQPS.set(0);
        }

        public int actualQPS() {
            return actualQPS.get();
        }

        public int errorQPS() {
            return errorQPS.get();
        }
    }
}
