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

package com.uber.tchannel.api;

import com.uber.tchannel.BaseTest;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetryTest extends BaseTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void retryUnsupportedFlag() throws Exception {
        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .build();
        req.release();
        exception.expect(UnsupportedOperationException.class);
        req.setRetryFlags("tcn");
    }

    @Test
    public void retryOnConnectionFailure() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        ArrayList<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", 8000));
        peers.add(new InetSocketAddress("127.0.0.1", 8001));
        peers.add(new InetSocketAddress("127.0.0.1", 8002));
        peers.add(new InetSocketAddress("127.0.0.1", 8003));
        peers.add(new InetSocketAddress("127.0.0.1", port));
        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(req);
        try (RawResponse res = future.get()) {
            assertFalse(res.isError());
            assertEquals("hello", res.getBody());
        }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void retryOnConnectionFailureWithFlag() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", new EchoHandler());
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", 8000));
        peers.add(new InetSocketAddress("127.0.0.1", 8001));
        peers.add(new InetSocketAddress("127.0.0.1", 8002));
        peers.add(new InetSocketAddress("127.0.0.1", 8003));
        peers.add(new InetSocketAddress("127.0.0.1", port));

        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();
        req.setRetryFlags("ct");

        TFuture<RawResponse> future = subClient.send(req);
        try (RawResponse res = future.get()) {
            assertFalse(res.isError());
            assertEquals("hello", res.getBody());
        }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void retryOnTimeout() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        EchoHandler handler = new EchoHandler(4);
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", handler);
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", port));
        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(100)
            .build();
        req.setRetryFlags("t");

        TFuture<RawResponse> future = subClient.send(req);
        try (RawResponse res = future.get()) {
            assertFalse(res.isError());
            assertEquals("hello", res.getBody());
            assertEquals(-1, handler.getDelayedCount());
        }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void retryOnTimeoutWithFlag() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        EchoHandler handler = new EchoHandler(4);
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", handler);
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", port));
        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(100)
            .build();
        req.setRetryFlags("tc");

        TFuture<RawResponse> future = subClient.send(req);

        try (RawResponse res = future.get()) {
            assertFalse(res.isError());
            assertEquals("hello", res.getBody());
            assertEquals(-1, handler.getDelayedCount());
        }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void retryOnTimeoutFails() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        EchoHandler handler = new EchoHandler(5);
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", handler);
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", port));
        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setRetryLimit(3)
            .setTimeout(100)
            .build();
        req.setRetryFlags("t");

        TFuture<RawResponse> future = subClient.send(req);

        try (RawResponse res = future.get()) {
            assertTrue(res.isError());
            assertEquals(ErrorType.Timeout, res.getError().getErrorType());
            assertEquals(1, handler.getDelayedCount());
        }

        client.shutdown();
        server.shutdown();
    }

    @Test
    public void retryOnConnectionFailureRespectFlag() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", 8000));
        peers.add(new InetSocketAddress("127.0.0.1", 8001));
        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();
        req.setRetryFlags("n");

        TFuture<RawResponse> future = subClient.send(req);

        try (RawResponse res = future.get()) {
            assertTrue(res.isError());
            assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
        }

        client.shutdown();
    }

    @Test
    public void retryOnTimeoutRespectFlag() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        EchoHandler handler = new EchoHandler(5);
        final SubChannel subServer = server.makeSubChannel("server")
            .register("echo", handler);
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        List<InetSocketAddress> peers = new ArrayList<>();
        peers.add(new InetSocketAddress("127.0.0.1", port));
        subClient.setPeers(peers);

        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(100)
            .build();

        TFuture<RawResponse> future = subClient.send(req);

        try (RawResponse res = future.get()) {
            assertTrue(res.isError());
            assertEquals(ErrorType.Timeout, res.getError().getErrorType());
            assertEquals(4, handler.getDelayedCount());
        }

        client.shutdown();
        server.shutdown();
    }

    protected static class EchoHandler implements RequestHandler {
        public boolean accessed = false;
        private int delayedCount = 0;

        public EchoHandler() {}

        public EchoHandler(int delayedCount) {
            this.delayedCount = delayedCount;
        }

        public int getDelayedCount() {
            return delayedCount;
        }

        @Override
        public RawResponse handle(Request request) {

            if (--delayedCount >= 0) {
                try {
                    sleep(150);
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
