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

package com.uber.tchannel.channels;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionTest {

    @Test
    public void testConnectionClientReset() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
                .setServerHost(host)
                .build();
        final SubChannel subClient = client.makeSubChannel("server");
        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        try (Response res = future.get()) {
            assertEquals("title", res.getArg2().toString(CharsetUtil.UTF_8));
            assertEquals("hello", res.getArg3().toString(CharsetUtil.UTF_8));
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        server.shutdown();
    }

    @Test
    public void testConnectionServerReset() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        try (Response res = future.get()) {
            assertEquals("title", res.getArg2().toString(CharsetUtil.UTF_8));
            assertEquals("hello", res.getArg3().toString(CharsetUtil.UTF_8));
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        server.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
    }

    @Test
    public void testConnectionFailure() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req,
            host,
            8888
        );

        try (Response res = future.get()) {
            assertTrue(res.isError());
            assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
            assertEquals("Failed to connect to the host", res.getError().getMessage());
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testPeerConnectionFailure() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        subClient.setPeers(new ArrayList<InetSocketAddress>(){
            {
                add(new InetSocketAddress(InetAddress.getLoopbackAddress(), 8888));
            }
        });

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2500)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req
        );

        try (Response res = future.get()) {
            assertTrue(res.isError());
            assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
            assertEquals("Failed to connect to the host", res.getError().getMessage());
        }
        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testCleanupDuringTimeout() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        client.shutdown();
        server.shutdown();

        try (RawResponse res = future.get()) {
            assertTrue(res.isError());
            assertEquals("Connection was reset due to network error", res.getError().getMessage());
            assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
        }
    }

    @Test
    public void testResetOnTimeout() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        EchoHandler echoHandler = new EchoHandler(true);
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", echoHandler);
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .setResetOnTimeoutLimit(10)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        client.listen();


        final AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            RawRequest req = new RawRequest.Builder("server", "echo")
                .setHeader("title")
                .setBody("hello")
                .setTimeout(5)
                .build();

            TFuture<RawResponse> future = subClient.send(
                req,
                host,
                port
            );
            future.addCallback(new TFutureCallback<RawResponse>() {
                @Override
                public void onResponse(RawResponse response) {
                    if (counter.incrementAndGet() < 10) {
                        assertEquals(ErrorType.Timeout, response.getError().getErrorType());
                    } else {
                        assertEquals(ErrorType.NetworkError, response.getError().getErrorType());
                        assertEquals("Connection was reset due to network error", response.getError().getMessage());
                    }
                }
            });
        }

        sleep(500);

        // Connection should still work after reset
        echoHandler.setDelayed(false);
        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(20000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        try (RawResponse res = future.get()) {
            assertFalse(res.isError());
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        client.shutdown();
        server.shutdown();
    }

    protected static class EchoHandler implements RequestHandler {
        public boolean accessed = false;
        private boolean delayed = false;

        public EchoHandler() {}

        public EchoHandler(boolean delayed) {
            this.delayed = delayed;
        }

        public void setDelayed(boolean delayed) {
            this.delayed = delayed;
        }

        @Override
        public RawResponse handle(Request request) {

            if (delayed) {
                try {
                    sleep(300);
                } catch (Exception ignored) {
                }
            }

            RawResponse response = new RawResponse.Builder(request)
                .setArg2(request.getArg2().retain())
                .setArg3(request.getArg3().retain())
                .build();

            accessed = true;
            return response;
        }
    }
}
