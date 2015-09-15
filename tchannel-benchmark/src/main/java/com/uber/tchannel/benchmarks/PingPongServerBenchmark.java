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

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.DefaultRequestHandler;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.TChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
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
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

@State(Scope.Thread)
public class PingPongServerBenchmark {

    TChannel channel;
    TChannel client;
    int port;

    @Param({ "0", "1", "10" })
    private int sleepTime;

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .include(".*" + PingPongServerBenchmark.class.getSimpleName() + ".*")
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {

        this.channel = new TChannel.Builder("ping-server")
                .register("ping", new PingDefaultRequestHandler())
                .build();
        this.client = new TChannel.Builder("ping-client").build();
        channel.listen();
        this.port = this.channel.getListeningPort();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void benchmark(final AdditionalCounters counters) throws Exception {

        Request<Ping> request = new Request.Builder<>(new Ping("ping?"), "some-service", "ping").build();

        Promise<Response<Pong>> f = this.client.callJSON(
                InetAddress.getLocalHost(),
                this.port,
                request,
                Pong.class
        );
        f.addListener(new GenericFutureListener<Future<Response<Pong>>>() {
            @Override
            public void operationComplete(Future<Response<Pong>> future) throws Exception {
                Response<Pong> pongResponse = future.get();
                counters.actualQPS.incrementAndGet();
            }
        });
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        this.client.shutdown();
        this.channel.shutdown();
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

    public class PingDefaultRequestHandler extends DefaultRequestHandler<Ping, Pong> {

        @Override
        public Response<Pong> handle(Request<Ping> request) {
            try {
                sleep(sleepTime);
            } catch (InterruptedException ex) {

            }
            return new Response.Builder<>(new Pong("pong!"), request.getEndpoint(), ResponseCode.OK).build();

        }

    }

    @AuxCounters
    @State(Scope.Thread)
    public static class AdditionalCounters {
        public AtomicInteger actualQPS = new AtomicInteger(0);

        @Setup(Level.Iteration)
        public void clean() {
            actualQPS.set(0);
        }

        public int actualQPS() {
            return actualQPS.get();
        }
    }
}
