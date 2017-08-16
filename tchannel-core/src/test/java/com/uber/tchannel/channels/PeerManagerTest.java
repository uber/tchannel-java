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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.tchannel.BaseTest;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.RequestHandler;
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

import static org.junit.Assert.*;

public class PeerManagerTest extends BaseTest {

    @Test
    public void testPeerAndConnections() throws Exception {

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

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getHeader());
            assertEquals("hello", res.getBody());
        }

        // checking the connections
        assertStats("client", client, 0, 1);
        assertStats("server", server, 1, 0);

        client.shutdown();
        server.shutdown();

        assertStats("client", client, 0, 0);
        assertStats("server", server, 0, 0);

    }

    @Test
    public void testConnectionPooling() throws Exception {

        InetAddress host = InetAddress.getByName(null);

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
        assertStats("client", client, 0, 1);
        assertStats("server", server, 1, 0);

        client.shutdown();
        server.shutdown();

        assertStats("client", client, 0, 0);
        assertStats("server", server, 0, 0);

    }

    @Test
    public void testWithPeerSelection() throws Exception {

        InetAddress host = InetAddress.getByName(null);

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
            .setPeers(ImmutableList.of(new InetSocketAddress(host, port)));
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
        assertStats("client", client, 0, 1);
        assertStats("server", server, 1, 0);

        client.shutdown();
        server.shutdown();

        assertStats("client", client, 0, 0);
        assertStats("server", server, 0, 0);

    }

    @Test
    public void testPreferIncoming() throws Exception {

        InetAddress host = InetAddress.getByName(null);

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
            .setPeers(ImmutableList.of(new InetSocketAddress(host, port)));
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
        assertStats("client", client, 0, 1);
        assertStats("server", server, 1, 0);

        client.shutdown();
        server.shutdown();

        assertStats("client", client, 0, 0);
        assertStats("server", server, 0, 0);

    }

    @Test
    public void testPreferOutgoing() throws Exception {

        InetAddress host = InetAddress.getByName(null);

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
            .setPeers(ImmutableList.of(new InetSocketAddress(host, port)));
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
        assertStats("client", client, 0, 1);
        assertStats("server", server, 1, 0);

        client.shutdown();
        server.shutdown();

        assertStats("client", client, 0, 0);
        assertStats("server", server, 0, 0);

    }

    @Test
    public void testChooseOutgoing() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server1 = new TChannel.Builder("server")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final EchoHandler echo1 = new EchoHandler();
        final SubChannel subServer1 = server1.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo1);
        server1.listen();
        final InetSocketAddress serverAddress1 = new InetSocketAddress(host, server1.getListeningPort());

        final TChannel server2 = new TChannel.Builder("server")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final EchoHandler echo2 = new EchoHandler();
        final SubChannel subServer2 = server2.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo2);
        server2.listen();
        final InetSocketAddress serverAddress2 = new InetSocketAddress(host, server2.getListeningPort());

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final SubChannel subClient = client.makeSubChannel("server", Connection.Direction.OUT)
            .setPeers(ImmutableList.of(serverAddress1, serverAddress2));
        client.listen();
        final InetSocketAddress clientAddress = new InetSocketAddress(host, client.getListeningPort());

        Connection conn1 = subClient.getPeerManager().connectTo(serverAddress1);
        Connection conn2 = server2.getPeerManager().connectTo(clientAddress);
        conn1.waitForIdentified(2000);
        conn2.waitForIdentified(2000);

        // checking the connections
        assertStats("client", client, 1, 1);
        assertStats("server1", server1, 1, 0);
        assertStats("server2", server2, 0, 1);

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader("title")
            .setBody("hello")
            .setId(1000)
            .setTimeout(2000)
            .build();

        TFuture<RawResponse> future = subClient.send(req);

        try (RawResponse res = future.get()) {
            assertEquals("title", res.getArg2().toString(CharsetUtil.UTF_8));
            assertEquals("hello", res.getArg3().toString(CharsetUtil.UTF_8));
        }

        assertTrue(echo1.accessed);
        assertFalse(echo2.accessed);

        client.shutdown();
        server1.shutdown();
        server2.shutdown();

        assertStats("client", client, 0, 0);
        assertStats("server1", server1, 0, 0);
        assertStats("server2", server2, 0, 0);

    }

    @Test
    public void testChooseIncoming() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server1 = new TChannel.Builder("server")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final EchoHandler echo1 = new EchoHandler();
        final SubChannel subServer1 = server1.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo1);
        server1.listen();
        final InetSocketAddress serverAddress1 = new InetSocketAddress(host, server1.getListeningPort());

        final TChannel server2 = new TChannel.Builder("server")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final EchoHandler echo2 = new EchoHandler();
        final SubChannel subServer2 = server2.makeSubChannel("server", Connection.Direction.OUT)
            .register("echo", echo2);
        server2.listen();
        final InetSocketAddress serverAddress2 = new InetSocketAddress(host, server2.getListeningPort());

        // create client
        final TChannel client = new TChannel.Builder("client")
            .setServerHost(host)
            .setInitTimeout(2000)
            .build();
        final SubChannel subClient = client.makeSubChannel("server", Connection.Direction.IN)
            .setPeers(ImmutableList.of(serverAddress1, serverAddress2));
        client.listen();
        final InetSocketAddress clientAddress = new InetSocketAddress(host, client.getListeningPort());

        Connection conn1 = subClient.getPeerManager().connectTo(serverAddress1);
        Connection conn2 = server2.getPeerManager().connectTo(clientAddress);
        conn1.waitForIdentified(2000);
        conn2.waitForIdentified(2000);

        // checking the connections
        assertStats("client", client, 1, 1);
        assertStats("server1", server1, 1, 0);
        assertStats("server2", server2, 0, 1);

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

        assertStats("client", client, 0, 0);
        assertStats("server1", server1, 0, 0);
        assertStats("server2", server2, 0, 0);

    }

    private void assertStats(String label, TChannel channel, int in, int out) {
        assertEquals(
            label + " stats",
            ImmutableMap.of(
                "connections.in", in,
                "connections.out", out
            ),
            channel.getPeerManager().getStats()
        );
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
