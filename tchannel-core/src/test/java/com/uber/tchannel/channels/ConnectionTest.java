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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.BaseTest;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.util.CharsetUtil;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionTest extends BaseTest {

    @BeforeClass
    public static void setup() {
        TChannelUtilities.setLogLevel(Level.FATAL);
    }

    @Test
    public void testConnectionClientReset() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

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

        ListenableFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        Response res = (Response)future.get();
        assertEquals(res.getArg2().toString(CharsetUtil.UTF_8), "title");
        assertEquals(res.getArg3().toString(CharsetUtil.UTF_8), "hello");
        res.release();

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 1);

        stats = server.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 1);
        assertEquals((int)stats.get("connections.out"), 0);

        client.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        stats = server.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        server.shutdown();
    }

    @Test
    public void testConnectionServerReset() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

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

        ListenableFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        Response res = (Response)future.get();
        assertEquals(res.getArg2().toString(CharsetUtil.UTF_8), "title");
        assertEquals(res.getArg3().toString(CharsetUtil.UTF_8), "hello");
        res.release();

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 1);

        stats = server.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 1);
        assertEquals((int)stats.get("connections.out"), 0);

        server.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        stats = server.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        client.shutdown();
    }

    @Test
    public void testConnectionFailure() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

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

        ListenableFuture<RawResponse> future = subClient.send(
            req,
            host,
            8888
        );

        Response res = (Response)future.get();
        assertTrue(res.isError());
        assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
        assertEquals("Failed to connect to the host", res.getError().getMessage());

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        res.release();
        client.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);
    }

    @Test
    public void testPeerConnectionFailure() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        subClient.setPeers(new ArrayList<InetSocketAddress>(){
            {
                add(new InetSocketAddress("127.0.0.1", 8888));
            }
        });

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        ListenableFuture<RawResponse> future = subClient.send(
            req
        );

        Response res = (Response)future.get();
        assertTrue(res.isError());
        assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
        assertEquals("Failed to connect to the host", res.getError().getMessage());

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        res.release();
        client.shutdown();
        sleep(100);

        stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);
    }

    @Test
    public void testCleanupDuringTimeout() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

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

        ListenableFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        client.shutdown();
        server.shutdown();

        RawResponse res = future.get();
        assertEquals(true, res.isError());
        assertEquals("Connection was reset due to network error", res.getError().getMessage());
        assertEquals(ErrorType.NetworkError, res.getError().getErrorType());
    }

    @Test
    public void testResetOnTimeout() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

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

            ListenableFuture<RawResponse> future = subClient.send(
                req,
                host,
                port
            );

            Futures.addCallback(future, new FutureCallback<RawResponse>() {
                @Override
                public void onSuccess(RawResponse response) {
                    if (counter.incrementAndGet() < 10) {
                        assertEquals(ErrorType.Timeout, response.getError().getErrorType());
                    } else {
                        assertEquals(ErrorType.NetworkError, response.getError().getErrorType());
                        assertEquals("Connection was reset due to network error", response.getError().getMessage());
                    }

                    response.release();
                }

                @Override
                public void onFailure(Throwable throwable) {
                    // we should never reach here
                    assertTrue(false);
                    System.out.println(throwable.getMessage());
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

        ListenableFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        RawResponse res = future.get();
        assertFalse(res.isError());
        assertEquals("title", res.getHeader());
        assertEquals("hello", res.getBody());

        res.release();

        client.shutdown();
        server.shutdown();
    }

    protected  class EchoHandler implements RequestHandler {
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
                } catch (Exception ex) {
                }
            }

            request.getArg2().retain();
            request.getArg3().retain();
            RawResponse response = new RawResponse.Builder(request)
                .setArg2(request.getArg2())
                .setArg3(request.getArg3())
                .build();

            accessed = true;
            return response;
        }
    }
}
