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
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import org.junit.Test;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class InitRequestFrameInitiatorTest {

    @Test
    public void testValidInitResponse() throws Exception {
        // Given
        PeerManager manager = new PeerManager(new Bootstrap());
        manager.setHostPort(String.format("%s:%d", "127.0.0.1", 8888));
        EmbeddedChannel channel = new EmbeddedChannel(
                new ChannelRegistrar(manager)
        );

        String initName = InitRequestInitiator.class.getSimpleName() + "CompletelyDifferent";
        channel.pipeline().addFirst(initName, new InitRequestInitiator(manager));
                List<String> handlerNames = channel.pipeline().names();
        String firstHandlerName = handlerNames.get(0);
        assertTrue(firstHandlerName.startsWith(initName));

        for (String name: channel.pipeline().names().subList(1, handlerNames.size())){
            assertFalse(name.startsWith(initName));
        }

        TFrame frame = MessageCodec.decode((ByteBuf) channel.readOutbound());
        // Then
        InitRequestFrame initRequestFrame = (InitRequestFrame) MessageCodec.decode(
            frame
        );

        frame.release();

        // Assert
        assertNotNull(initRequestFrame);
        // Headers as expected
        assertEquals(initRequestFrame.getHeaders().get("host_port"), "127.0.0.1:8888");
        assertEquals(initRequestFrame.getHeaders().get("process_name"), "java-process");

        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(new InitResponseFrame(
                        initRequestFrame.getId(),
                        initRequestFrame.getVersion(),
                        initRequestFrame.getHeaders()
                    )
                )
            )
        );

        Object obj = channel.readOutbound();
        assertNull(obj);

        // Assert Pipeline does not contain InitRequestHandler
        for (String name: channel.pipeline().names()){
            assertFalse(name.startsWith(initName));
        }

    }
}
