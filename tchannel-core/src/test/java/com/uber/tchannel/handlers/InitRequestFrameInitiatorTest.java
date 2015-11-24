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

import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.channels.ChannelRegistrar;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.frames.InitRequestFrame;
import com.uber.tchannel.frames.InitResponseFrame;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InitRequestFrameInitiatorTest {

    @Test
    public void testValidInitResponse() throws Exception {
        // Given
        PeerManager manager = new PeerManager(new Bootstrap());
        manager.setHostPort(String.format("%s:%d", "127.0.0.1", 8888));
        EmbeddedChannel channel = new EmbeddedChannel(
                new ChannelRegistrar(manager)
        );

        channel.pipeline().addFirst("InitRequestInitiator", new InitRequestInitiator(manager));
        assertEquals(4, channel.pipeline().names().size());

        // Then
        InitRequestFrame initRequestFrame = (InitRequestFrame) MessageCodec.decode((TFrame) channel.readOutbound());

        // Assert
        assertNotNull(initRequestFrame);
        // Headers as expected
        assertEquals(initRequestFrame.getHeaders().get("host_port"), "127.0.0.1:8888");
        assertEquals(initRequestFrame.getHeaders().get("process_name"), "java-process");

        channel.writeInbound(MessageCodec.encode(new InitResponseFrame(
                initRequestFrame.getId(),
                initRequestFrame.getVersion(),
                initRequestFrame.getHeaders()
        )));

        Object obj = channel.readOutbound();
        assertNull(obj);

        assertEquals(3, channel.pipeline().names().size());

    }
}
