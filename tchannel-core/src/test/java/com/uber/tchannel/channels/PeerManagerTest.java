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

import com.uber.tchannel.BaseTest;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.net.InetAddress;

import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.messages.RawRequest;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PeerManagerTest extends BaseTest {

    @Test
    public void testPeerAndConnections() throws Exception {

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

        TFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        server.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testConnectionPooling() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        server.makeSubChannel("server").register("echo", new EchoHandler());
        server.makeSubChannel("server2").register("echo", new EchoHandler());
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server");
        final SubChannel subClient2 = client.makeSubChannel("server2");
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

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        req = new RawRequest.Builder("server2", "echo")
            .setHeader("title")
            .setBody("hello")
            .setTimeout(2000)
            .build();

        future = subClient.send(
            req,
            host,
            port
        );

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        server.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testWithPeerSelection() throws Exception {

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
        final SubChannel subClient = client.makeSubChannel("server")
            .setPeers(new ArrayList<InetSocketAddress>() {
                {
                    add(new InetSocketAddress("127.0.0.1", port));
                }
            });
        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setId(1000)
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req
        );

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        server.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testPreferIncoming() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server", Connection.Direction.IN)
            .register("echo", new EchoHandler());
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server", Connection.Direction.IN)
            .setPeers(new ArrayList<InetSocketAddress>() {
                {
                    add(new InetSocketAddress("127.0.0.1", port));
                }
            });
        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setId(1000)
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req
        );

        try (RawResponse res = future.get()) {
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
        server.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testPreferOutgoing() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final SubChannel subServer = server.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", new EchoHandler());
        server.listen();

        final int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server", Connection.Direction.OUT)
            .setPeers(new ArrayList<InetSocketAddress>() {
                {
                    add(new InetSocketAddress("127.0.0.1", port));
                }
            });
        client.listen();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setId(1000)
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req
        );

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        client.shutdown();
        server.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testChooseOutgoing() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server1 = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final EchoHandler echo1 = new EchoHandler();
        final SubChannel subServer1 = server1.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo1);
        server1.listen();
        final InetSocketAddress serverAddress1 = new InetSocketAddress("127.0.0.1", server1.getListeningPort());

        final TChannel server2 = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final EchoHandler echo2 = new EchoHandler();
        final SubChannel subServer2 = server2.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo2);
        server2.listen();
        final InetSocketAddress serverAddress2 = new InetSocketAddress("127.0.0.1", server2.getListeningPort());

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server", Connection.Direction.OUT)
            .setPeers(new ArrayList<InetSocketAddress>() {
                {
                    add(serverAddress1);
                    add(serverAddress2);
                }
            });
        client.listen();
        final InetSocketAddress clientAddress = new InetSocketAddress("127.0.0.1", client.getListeningPort());

        Connection conn1 = subClient.getPeerManager().connectTo(serverAddress1);
        Connection conn2 = server2.getPeerManager().connectTo(clientAddress);
        conn1.waitForIdentified(6000);
        conn2.waitForIdentified(6000);

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals("client connections.in", 1, (int)stats.get("connections.in"));
        assertEquals("client connections.out", 1, (int)stats.get("connections.out"));

        stats = server1.getPeerManager().getStats();
        assertEquals("server1 connections.in", 1, (int)stats.get("connections.in"));
        assertEquals("server1 connections.out", 0, (int)stats.get("connections.out"));

        stats = server2.getPeerManager().getStats();
        assertEquals("server2 connections.in", 0, (int)stats.get("connections.in"));
        assertEquals("server2 connections.out", 1, (int)stats.get("connections.out"));

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setId(1000)
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req
        );

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getArg2().toString(CharsetUtil.UTF_8));
            assertEquals("hello", res.getArg3().toString(CharsetUtil.UTF_8));
        }

        assertTrue(echo1.accessed);
        assertFalse(echo2.accessed);

        client.shutdown();
        server1.shutdown();
        server2.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server1.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server2.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    @Test
    public void testChooseIncoming() throws Exception {

        InetAddress host = InetAddress.getByName("127.0.0.1");

        // create server
        final TChannel server1 = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final EchoHandler echo1 = new EchoHandler();
        final SubChannel subServer1 = server1.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo1);
        server1.listen();
        final InetSocketAddress serverAddress1 = new InetSocketAddress("127.0.0.1", server1.getListeningPort());

        final TChannel server2 = new TChannel.Builder("server")
            .setServerHost(host)
            .build();
        final EchoHandler echo2 = new EchoHandler();
        final SubChannel subServer2 = server2.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo2);
        server2.listen();
        final InetSocketAddress serverAddress2 = new InetSocketAddress("127.0.0.1", server2.getListeningPort());

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .build();
        final SubChannel subClient = client.makeSubChannel("server", Connection.Direction.IN)
            .setPeers(new ArrayList<InetSocketAddress>() {
                {
                    add(serverAddress1);
                    add(serverAddress2);
                }
            });
        client.listen();
        final InetSocketAddress clientAddress = new InetSocketAddress("127.0.0.1", client.getListeningPort());

        Connection conn1 = subClient.getPeerManager().connectTo(serverAddress1);
        Connection conn2 = server2.getPeerManager().connectTo(clientAddress);
        conn1.waitForIdentified(2000);
        conn2.waitForIdentified(2000);

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        stats = server1.getPeerManager().getStats();
        assertEquals(1, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server2.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(1, (int)stats.get("connections.out"));

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setId(1000)
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(
            req
        );

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getArg2().toString(CharsetUtil.UTF_8));
            assertEquals("hello", res.getArg3().toString(CharsetUtil.UTF_8));
        }

        assertFalse(echo1.accessed);
        assertTrue(echo2.accessed);

        client.shutdown();
        server1.shutdown();
        server2.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server1.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));

        stats = server2.getPeerManager().getStats();
        assertEquals(0, (int)stats.get("connections.in"));
        assertEquals(0, (int)stats.get("connections.out"));
    }

    protected static class EchoHandler implements RequestHandler {
        public boolean accessed = false;

        @Override
        public Response handle(Request request) {
            accessed = true;
            return new RawResponse.Builder(request)
                .setArg2(request.getArg2().retain())
                .setArg3(request.getArg3().retain())
                .build();
        }
    }
}


