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

import com.uber.tchannel.BaseTest;
import com.uber.tchannel.Fixtures;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.frames.InitFrame;
import com.uber.tchannel.frames.InitRequestFrame;
import com.uber.tchannel.frames.InitResponseFrame;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class InitDefaultRequestHandlerTest extends BaseTest {

    @Rule
    public final ExpectedException expectedClosedChannelException = ExpectedException.none();

    @Test
    public void testInitHandlerRemovesItself() throws Exception {

        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new PeerManager(new Bootstrap()))
        );

        // Assert pipeline starts with InitRequestHandler
        String initName = InitRequestHandler.class.getSimpleName();
        List<String> handlerNames = channel.pipeline().names();
        String firstHandlerName = handlerNames.get(0);
        assertTrue(firstHandlerName.startsWith(initName));

        for (String name: channel.pipeline().names().subList(1, handlerNames.size())){
            assertFalse(name.startsWith(initName));
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(InitFrame.HOST_PORT_KEY, "0.0.0.0:0");
        headers.put(InitFrame.PROCESS_NAME_KEY, "test-process");
        InitRequestFrame initRequestFrame = new InitRequestFrame(
                42,
                InitFrame.DEFAULT_VERSION,
                headers);

        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(
                    PooledByteBufAllocator.DEFAULT, initRequestFrame
                )
            )
        );
        channel.writeOutbound(channel.readInbound());

        // Then
        TFrame tFrame = MessageCodec.decode((ByteBuf) channel.readOutbound());
        InitResponseFrame initResponseFrame = (InitResponseFrame) MessageCodec.decode(tFrame);
        tFrame.release();

        // Assert
        assertNotNull(initResponseFrame);
        assertEquals(initRequestFrame.getId(), initResponseFrame.getId());
        assertEquals(initRequestFrame.getVersion(), initResponseFrame.getVersion());
        assertEquals(initRequestFrame.getHostPort(), initResponseFrame.getHostPort());

        // Assert Pipeline does not contain InitRequestHandler
        for (String name: channel.pipeline().names()){
            assertFalse(name.startsWith(initName));
        }

        // Make sure Messages are still passed through
        channel.writeInbound(initRequestFrame);
        channel.writeOutbound(channel.readInbound());
        InitRequestFrame sameInitRequestFrame = channel.readOutbound();
        assertEquals(initRequestFrame.getId(), sameInitRequestFrame.getId());
        assertEquals(initRequestFrame.getVersion(), sameInitRequestFrame.getVersion());
        assertEquals(initRequestFrame.getHostPort(), sameInitRequestFrame.getHostPort());
    }

    @Test
    public void testValidInitRequest() throws Exception {

        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new PeerManager(new Bootstrap()))
        );
        Map<String, String> headers = new HashMap<>();
        headers.put(InitFrame.HOST_PORT_KEY, "0.0.0.0:0");
        headers.put(InitFrame.PROCESS_NAME_KEY, "test-process");
        InitRequestFrame initRequestFrame = new InitRequestFrame(42,
                InitFrame.DEFAULT_VERSION,
                headers);

        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(PooledByteBufAllocator.DEFAULT, initRequestFrame)
            )
        );
        channel.writeOutbound(channel.readInbound());

        // Then
        TFrame tFrame = MessageCodec.decode((ByteBuf) channel.readOutbound());
        InitResponseFrame initResponseFrame = (InitResponseFrame) MessageCodec.decode(tFrame);
        tFrame.release();

        // Assert
        assertNotNull(initResponseFrame);
        assertEquals(initRequestFrame.getId(), initResponseFrame.getId());
        assertEquals(initRequestFrame.getVersion(), initResponseFrame.getVersion());
        assertEquals(initRequestFrame.getHostPort(), initResponseFrame.getHostPort());

    }

    @Test
    public void testInvalidCallBeforeInitRequest() throws Exception {
        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new PeerManager(new Bootstrap()))
        );

        CallRequestFrame callRequestFrame = Fixtures.callRequest(0, false, Unpooled.EMPTY_BUFFER);

        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(PooledByteBufAllocator.DEFAULT, callRequestFrame)
            )
        );

        TFrame tFrame = MessageCodec.decode((ByteBuf) channel.readOutbound());
        ErrorFrame errorFrame = (ErrorFrame) MessageCodec.decode(tFrame);
        tFrame.release();
        assertNotNull(errorFrame);
        assertThat(errorFrame.getErrorType(), is(ErrorType.FatalProtocolError));
        assertThat(errorFrame.getMessage(), containsString("The first frame should be an Init Request"));

        channel.writeOutbound();
    }

    @Test
    public void testIncorrectProtocolVersion() throws Exception {
        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new PeerManager(new Bootstrap()))
        );
        Map<String, String> headers = new HashMap<>();
        headers.put(InitFrame.HOST_PORT_KEY, "0.0.0.0:0");
        headers.put(InitFrame.PROCESS_NAME_KEY, "test-process");
        InitRequestFrame initRequestFrame = new InitRequestFrame(42,
                1,
                headers);

        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(PooledByteBufAllocator.DEFAULT, initRequestFrame)
            )
        );

        TFrame tFrame = MessageCodec.decode((ByteBuf) channel.readOutbound());
        ErrorFrame errorFrame = (ErrorFrame) MessageCodec.decode(tFrame);

        tFrame.release();
        assertNotNull(errorFrame);
        assertThat(errorFrame.getErrorType(), is(ErrorType.FatalProtocolError));
        assertThat(errorFrame.getMessage(), containsString("Expected Protocol version: 2, got version: 1"));

        channel.writeOutbound();
    }
}
