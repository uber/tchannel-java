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
import com.uber.tchannel.fragmentation.FragmentationState;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.RawMessage;
import com.uber.tchannel.schemes.TChannelMessage;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
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

        MessageDefragmenter mux = new MessageDefragmenter();
        Map<Long, TChannelMessage> map = mux.getMessageMap();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        CallRequestFrame callRequestFrame = Fixtures.callRequest(id, true, Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{0x00, 0x04},
                "arg1".getBytes()
        ));
        callRequestFrame.getHeaders().put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
        channel.writeInbound(callRequestFrame);
        assertEquals(map.size(), 1);
        assertNotNull(map.get(id));
        assertEquals(1, callRequestFrame.refCnt());

        CallRequestContinueFrame firstCallRequestContinueFrame = Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                // arg2 size
                new byte[]{0x00, 0x04},
                "arg2".getBytes()
        ));
        channel.writeInbound(firstCallRequestContinueFrame);
        assertEquals(map.size(), 1);
        assertNotNull(map.get(id));
        assertEquals(1, callRequestFrame.refCnt());
        assertEquals(1, firstCallRequestContinueFrame.refCnt());

        CallRequestContinueFrame secondCallRequestContinueFrame = Fixtures.callRequestContinue(id, false, Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{0x00, 0x00},
                new byte[]{0x00, 0x04},
                "arg3".getBytes()
        ));
        channel.writeInbound(secondCallRequestContinueFrame);
        assertEquals(map.size(), 0);
        assertNull(map.get(id));
        assertEquals(1, callRequestFrame.refCnt());
        assertEquals(1, firstCallRequestContinueFrame.refCnt());
        assertEquals(1, secondCallRequestContinueFrame.refCnt());

        RawMessage fullMessage = channel.readInbound();
        assertEquals(1, fullMessage.getArg1().refCnt());
        assertEquals(1, fullMessage.getArg2().refCnt());
        assertEquals(1, fullMessage.getArg3().refCnt());
        assertNotNull(fullMessage);

        assertEquals(
            fullMessage.getArg1().toString(CharsetUtil.UTF_8), "arg1"
        );

        assertEquals(
            fullMessage.getArg2().toString(CharsetUtil.UTF_8), "arg2"
        );

        assertEquals(
            fullMessage.getArg3().toString(CharsetUtil.UTF_8), "arg3"
        );

        fullMessage.getArg1().release();
        fullMessage.getArg2().release();
        fullMessage.getArg3().release();

        assertEquals(0, callRequestFrame.refCnt());
        assertEquals(0, firstCallRequestContinueFrame.refCnt());
        assertEquals(0, secondCallRequestContinueFrame.refCnt());

        assertNull(channel.readInbound());

    }

    @Test
    public void testReadArgWithAllArgs() throws Exception {
        MessageDefragmenter codec = new MessageDefragmenter();
        Map<Long, FragmentationState> defragmentationState = codec.getDefragmentationState();

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

        assertEquals(defragmentationState.get(id), FragmentationState.ARG2);

        codec.readArg(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x02,
                        // this arg has length '2'
                        0x00,
                        0x01,
                }
        )));

        assertEquals(defragmentationState.get(id), FragmentationState.ARG2);

        codec.readArg(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x00
                        // this arg has length '0'
                }
        )));

        assertEquals(defragmentationState.get(id), FragmentationState.ARG3);

        codec.readArg(Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x02,
                        // this arg has length '2'
                        0x0e,
                        0x0f
                }
        )));

        assertEquals(defragmentationState.get(id), FragmentationState.ARG3);

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
