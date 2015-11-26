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

package com.uber.tchannel.handlers;

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.*;

public class FrameFragmenterTest {

    private static final int BUFFER_SIZE = 100000;

    @Test
    public void testEncode() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(
            new MessageFragmenter()
        );

        // arg1
        byte[] arg1Bytes = new byte[CallFrame.MAX_ARG1_LENGTH];
        new Random().nextBytes(arg1Bytes);
        ByteBuf arg1 = Unpooled.wrappedBuffer(arg1Bytes);

        // arg2
        byte[] arg2Bytes = new byte[BUFFER_SIZE];
        new Random().nextBytes(arg2Bytes);
        ByteBuf arg2 = Unpooled.wrappedBuffer(arg2Bytes);

        // arg 3
        byte[] arg3Bytes = new byte[BUFFER_SIZE];
        new Random().nextBytes(arg3Bytes);
        ByteBuf arg3 = Unpooled.wrappedBuffer(arg3Bytes);

        RawRequest rawRequest = new RawRequest.Builder("some-service", arg1)
            .setArg2(arg2)
            .setArg3(arg3)
            .setId(0)
            .setTimeout(100)
            .build();

        channel.writeOutbound(rawRequest);

        for (int i = 0; i < 4; i++) {
            CallFrame req = (CallFrame) MessageCodec.decode(
                MessageCodec.decode(
                    (ByteBuf) channel.readOutbound()
                )
            );
            req.release();
            assertNotNull(req);
        }

        ByteBuf buf = channel.readOutbound();
        assertNull(buf);

        rawRequest.release();
    }

    private String payload;

    @Test
    public void testSendReceive() throws Exception {

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

        int size = 100 * 1024;
        StringBuilder sb = new StringBuilder(size * 2);
        for (int i = 0; i < size; i++) {
            sb.append("正");
        }

        payload = sb.toString();

        RawRequest req = new RawRequest.Builder("server", "echo")
            .setHeader(payload)
            .setBody(payload)
            .setTimeout(20000)
            .build();

        ListenableFuture<RawResponse> future = subClient.send(
            req,
            host,
            port
        );

        RawResponse res = future.get();

        assertEquals(payload.length(), res.getHeader().length());
        assertEquals(payload, res.getHeader());
        assertEquals(payload.length(), res.getBody().length());
        assertEquals(payload, res.getBody());
        res.release();

        // checking the connections
        Map<String, Integer> stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 1);

        stats = server.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 1);
        assertEquals((int)stats.get("connections.out"), 0);

        client.shutdown();
        server.shutdown();

        stats = client.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);

        stats = server.getPeerManager().getStats();
        assertEquals((int)stats.get("connections.in"), 0);
        assertEquals((int)stats.get("connections.out"), 0);
    }

    protected  class EchoHandler implements RequestHandler {
        @Override
        public Response handle(Request request) {

            RawRequest rawRequest = (RawRequest) request;
            assertEquals(payload.length(), rawRequest.getHeader().length());
            assertEquals(payload, rawRequest.getHeader());
            assertEquals(payload.length(), rawRequest.getBody().length());
            assertEquals(payload, rawRequest.getBody());
            return new RawResponse.Builder(request)
                .setArg2(request.getArg2().retain())
                .setArg3(request.getArg3().retain())
                .build();
        }
    }
}
