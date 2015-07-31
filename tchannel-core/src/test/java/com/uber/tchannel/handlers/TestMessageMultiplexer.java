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
import com.uber.tchannel.fragmentation.DefragmentationState;
import com.uber.tchannel.messages.FullMessage;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.*;

public class TestMessageMultiplexer {

    @Rule
    public final ExpectedException expectedAssertionError = ExpectedException.none();

    @Test
    public void testMergeMessage() {

        MessageMultiplexer mux = new MessageMultiplexer();
        Map<Long, FullMessage> map = mux.getMessageMap();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        channel.writeInbound(Fixtures.callRequest(id, true, Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{0x00, 0x04},
                "arg1".getBytes()
        )));
        assertEquals(map.size(), 1);
        assertNotNull(map.get(id));

        channel.writeInbound(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                // arg2 size
                new byte[]{0x00, 0x04},
                "arg2".getBytes()
        )));
        assertEquals(map.size(), 1);
        assertNotNull(map.get(id));

        channel.writeInbound(Fixtures.callRequestContinue(id, false, Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{0x00, 0x00},
                new byte[]{0x00, 0x04},
                "arg3".getBytes()
        )));
        assertEquals(map.size(), 0);
        assertNull(map.get(id));

        FullMessage fullMessage = channel.readInbound();
        assertNotNull(fullMessage);

        assertEquals(
                ByteBufUtil.hexDump(fullMessage.getArg1()),
                ByteBufUtil.hexDump(Unpooled.wrappedBuffer("arg1".getBytes()))
        );

        assertEquals(
                ByteBufUtil.hexDump(fullMessage.getArg2()),
                ByteBufUtil.hexDump(Unpooled.wrappedBuffer("arg2".getBytes()))
        );

        assertEquals(
                ByteBufUtil.hexDump(fullMessage.getArg3()),
                ByteBufUtil.hexDump(Unpooled.wrappedBuffer("arg3".getBytes()))
        );


        assertNull(channel.readInbound());

    }

    @Test
    public void testReadArgWithAllArgs() throws Exception {
        MessageMultiplexer codec = new MessageMultiplexer();
        Map<Long, DefragmentationState> defragmentationState = codec.getDefragmentationState();

        long id = 42;
        assertEquals(defragmentationState.get(id), null);

        codec.readArg(Fixtures.callRequest(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x06,
                        // this arg has length '6'
                        0x00,
                        0x01,
                        0x02,
                        0x03,
                        0x04,
                        0x05
                }
        )));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_2);

        codec.readArg(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x02,
                        // this arg has length '2'
                        0x00,
                        0x01,
                }
        )));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_2);

        codec.readArg(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x00
                        // this arg has length '0'
                }
        )));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_3);

        codec.readArg(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x02,
                        // this arg has length '2'
                        0x0e,
                        0x0f
                }
        )));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_3);

        codec.readArg(Fixtures.callRequestContinue(id, false, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x00
                        // this arg has length '0'
                }
        )));

        assertEquals(defragmentationState.get(id), null);


    }

}
