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

import com.uber.tchannel.Fixtures;
import com.uber.tchannel.channels.ChannelManager;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.CallRequest;
import com.uber.tchannel.messages.ErrorMessage;
import com.uber.tchannel.messages.InitMessage;
import com.uber.tchannel.messages.InitRequest;
import com.uber.tchannel.messages.InitResponse;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.channels.ClosedChannelException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class InitDefaultRequestHandlerTest {

    @Rule
    public final ExpectedException expectedClosedChannelException = ExpectedException.none();

    @Test
    public void testInitHandlerRemovesItself() throws Exception {

        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new ChannelManager())
        );

        assertEquals(3, channel.pipeline().names().size());

        InitRequest initRequest = new InitRequest(
                42,
                InitMessage.DEFAULT_VERSION,
                new HashMap<String, String>() {{
                    put(InitMessage.HOST_PORT_KEY, "0.0.0.0:0");
                    put(InitMessage.PROCESS_NAME_KEY, "test-process");
                }}
        );

        channel.writeInbound(initRequest);
        channel.writeOutbound(channel.readInbound());

        // Then
        InitResponse initResponse = channel.readOutbound();

        // Assert
        assertNotNull(initResponse);
        assertEquals(initRequest.getId(), initResponse.getId());
        assertEquals(initRequest.getVersion(), initResponse.getVersion());
        assertEquals(initRequest.getHostPort(), initResponse.getHostPort());

        // Assert Pipeline is empty
        assertEquals(2, channel.pipeline().names().size());

        // Make sure Messages are still passed through
        channel.writeInbound(initRequest);
        channel.writeOutbound(channel.readInbound());
        InitRequest sameInitRequest = channel.readOutbound();
        assertEquals(initRequest.getId(), sameInitRequest.getId());
        assertEquals(initRequest.getVersion(), sameInitRequest.getVersion());
        assertEquals(initRequest.getHostPort(), sameInitRequest.getHostPort());

    }

    @Test
    public void testValidInitRequest() throws Exception {

        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new ChannelManager())
        );

        InitRequest initRequest = new InitRequest(42,
                InitMessage.DEFAULT_VERSION,
                new HashMap<String, String>() {{
                    put(InitMessage.HOST_PORT_KEY, "0.0.0.0:0");
                    put(InitMessage.PROCESS_NAME_KEY, "test-process");
                }}
        );

        channel.writeInbound(initRequest);
        channel.writeOutbound(channel.readInbound());

        // Then
        InitResponse initResponse = channel.readOutbound();

        // Assert
        assertNotNull(initResponse);
        assertEquals(initRequest.getId(), initResponse.getId());
        assertEquals(initRequest.getVersion(), initResponse.getVersion());
        assertEquals(initRequest.getHostPort(), initResponse.getHostPort());

    }

    @Test
    public void testInvalidCallBeforeInitRequest() throws Exception {
        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new ChannelManager())
        );

        CallRequest callRequest = Fixtures.callRequest(0, false, Unpooled.EMPTY_BUFFER);
        channel.writeInbound(callRequest);
        ErrorMessage error = channel.readOutbound();
        assertNotNull(error);
        assertThat(error.getType(), is(ErrorType.FatalProtocolError));

        this.expectedClosedChannelException.expect(ClosedChannelException.class);
        channel.writeOutbound();

    }

    @Test
    public void testIncorrectProtocolVersion() throws Exception {
        // Given
        EmbeddedChannel channel = new EmbeddedChannel(
                new InitRequestHandler(new ChannelManager())
        );

        InitRequest initRequest = new InitRequest(42,
                1,
                new HashMap<String, String>() {{
                    put(InitMessage.HOST_PORT_KEY, "0.0.0.0:0");
                    put(InitMessage.PROCESS_NAME_KEY, "test-process");
                }}
        );

        channel.writeInbound(initRequest);
        ErrorMessage error = channel.readOutbound();
        assertNotNull(error);
        assertThat(error.getType(), is(ErrorType.FatalProtocolError));
        assertThat(error.getMessage(), containsString("version"));

        this.expectedClosedChannelException.expect(ClosedChannelException.class);
        channel.writeOutbound();

    }
}
