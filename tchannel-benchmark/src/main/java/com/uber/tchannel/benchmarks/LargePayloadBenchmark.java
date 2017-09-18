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

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Thread)
public class LargePayloadBenchmark {

    private TChannel channel;
    private TChannel client;
    private SubChannel subClient;
    private int port;
    private InetAddress host;
    private ByteBuf payload;

    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup childGroup = new NioEventLoopGroup();

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
            .include(".*" + LargePayloadBenchmark.class.getSimpleName() + ".*")
            .warmupIterations(30)
            .measurementIterations(50)
            .forks(1)
            .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        this.host = InetAddress.getByName(null);
        this.channel = new TChannel.Builder("ping-server")
            .setServerHost(host)
            .setBossGroup(bossGroup)
            .setChildGroup(childGroup)
            .build();
        channel.makeSubChannel("ping-server").register("ping", new PingDefaultRequestHandler());
        channel.listen();
        this.port = this.channel.getListeningPort();

        this.client = new TChannel.Builder("ping-client")
            .setClientMaxPendingRequests(200000)
            .setBossGroup(bossGroup)
            .setChildGroup(childGroup)
            .build();
        this.subClient = this.client.makeSubChannel("ping-server");
        this.client.listen();

        byte[] buf = new byte[60 * 1024];
        new Random().nextBytes(buf);
        payload = Unpooled.wrappedBuffer(buf);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark(final AdditionalCounters counters) throws Exception {
        RawRequest request = new RawRequest.Builder("ping-server", "ping")
            .setArg3(payload.retain())
            .setTimeout(20000)
            .build();

        TFuture<RawResponse> future = this.subClient
            .send(
                request,
                this.host,
                this.port
            );
        future.addCallback(new TFutureCallback<RawResponse>() {
            @Override
            public void onResponse(RawResponse pongResponse) {
                if (!pongResponse.isError()) {
                    counters.actualQPS.incrementAndGet();
                    pongResponse.release();
                } else {
                    switch (pongResponse.getError().getErrorType()) {
                        case Busy:
                            counters.busyQPS.incrementAndGet();
                            break;
                        case Timeout:
                            counters.timeoutQPS.incrementAndGet();
                            break;
                        case NetworkError:
                            counters.networkQPS.incrementAndGet();
                            break;
                        default:
                            counters.errorQPS.incrementAndGet();
                    }
                }
            }
        });
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        this.client.shutdown(false);
        this.channel.shutdown(false);
    }

    public static class PingDefaultRequestHandler implements RequestHandler {
        @Override
        public Response handle(Request request) {
            return new RawResponse.Builder(request)
                .build();
        }
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class AdditionalCounters {
        private final AtomicInteger actualQPS = new AtomicInteger(0);
        private final AtomicInteger errorQPS = new AtomicInteger(0);
        private final AtomicInteger timeoutQPS = new AtomicInteger(0);
        private final AtomicInteger busyQPS = new AtomicInteger(0);
        private final AtomicInteger networkQPS = new AtomicInteger(0);

        @Setup(Level.Iteration)
        public void clean() {
            errorQPS.set(0);
            actualQPS.set(0);
            timeoutQPS.set(0);
            busyQPS.set(0);
            networkQPS.set(0);
        }

        public int actualQPS() {
            return actualQPS.get();
        }

        public int errorQPS() {
            return errorQPS.get();
        }
        public int timeoutQPS() {
            return timeoutQPS.get();
        }
        public int busyQPS() {
            return busyQPS.get();
        }
        public int networkQPS() {
            return networkQPS.get();
        }
    }
}
