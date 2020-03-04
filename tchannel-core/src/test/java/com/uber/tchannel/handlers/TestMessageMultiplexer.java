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
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseContinueFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.messages.RawMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.util.CharsetUtil;
import java.nio.charset.StandardCharsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMessageMultiplexer {

    @Rule
    public final ExpectedException expectedAssertionError = ExpectedException.none();

    @Test
    public void testSuccessfulDecodeRetainsExtraCopyOfFrame() {

        MessageDefragmenter mux = new MessageDefragmenter();
        Map<Long, List<CallFrame>> map = mux.getCallFrames();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        CallRequestFrame callRequestFrame = Fixtures.callRequest(
            id,
            true,
            new HashMap<String, String>() {{
                put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
            }},
            Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{ 0x00, 0x04 },
                "arg1".getBytes(StandardCharsets.UTF_8),
                new byte[]{ 0x00, 0x00 }
            )
        );

        TFrame encoded = MessageCodec.encode(callRequestFrame);
        ByteBuf encodeByteBuf = MessageCodec.encode(encoded);
        channel.writeInbound(encodeByteBuf);

        assertEquals(1, map.size());
        assertNotNull(map.get(id));
        assertEquals(1, callRequestFrame.refCnt());
        assertEquals(1, encodeByteBuf.refCnt());
    }

    @Test
    public void testFailedDecodeDoesntRetainExtraCopyOfFrame() {

        MessageDefragmenter mux = new MessageDefragmenter();
        Map<Long, List<CallFrame>> map = mux.getCallFrames();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        CallRequestFrame callRequestFrame = Fixtures.callRequest(
            id,
            false,
            new HashMap<String, String>() {{
                put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
            }},
            Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{ 0x00, 0x04 },
                new byte[]{ 0x00, 0x00 }
            )
        );

        TFrame encoded = MessageCodec.encode(callRequestFrame);
        ByteBuf encodeByteBuf = MessageCodec.encode(encoded);
        try {
            channel.writeInbound(encodeByteBuf);
            fail();
        } catch (DecoderException e) {
            assertTrue(e.getMessage().contains("wrong read index for args"));
        }

        assertEquals(0, map.size());
        assertEquals(0, callRequestFrame.refCnt());
        assertEquals(0, encodeByteBuf.refCnt());
    }

    @Test
    public void testMergeRequestMessage() {

        MessageDefragmenter mux = new MessageDefragmenter();
        Map<Long, List<CallFrame>> map = mux.getCallFrames();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        CallRequestFrame callRequestFrame = Fixtures.callRequest(id,
            true,
            new HashMap<String, String>(){{
                put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
            }},
            Unpooled.wrappedBuffer(
                // arg1 size
                new byte[]{0x00, 0x04},
                "arg1".getBytes(StandardCharsets.UTF_8),
                new byte[]{0x00, 0x00}
        ));

        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(callRequestFrame)
            )
        );
        assertEquals(1, map.size());
        assertNotNull(map.get(id));
        assertEquals(1, callRequestFrame.refCnt());

        CallRequestContinueFrame firstCallRequestContinueFrame = Fixtures.callRequestContinue(id, true, Unpooled.wrappedBuffer(
            // arg2 size
            new byte[]{0x00, 0x04},
            "arg2".getBytes(StandardCharsets.UTF_8)
        ));
        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(firstCallRequestContinueFrame)
            )
        );
        assertEquals(1, map.size());
        assertNotNull(map.get(id));
        assertEquals(2, map.get(id).size());
        assertEquals(1, callRequestFrame.refCnt());
        assertEquals(1, firstCallRequestContinueFrame.refCnt());

        CallRequestContinueFrame secondCallRequestContinueFrame = Fixtures.callRequestContinue(id, false, Unpooled.wrappedBuffer(
            new byte[]{0x00, 0x00},
            // arg3 size
            new byte[]{0x00, 0x04},
            "arg3".getBytes(StandardCharsets.UTF_8)
        ));
        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(secondCallRequestContinueFrame)
            )
        );
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
    public void testContinueResponseRefNotRetainedIfFailed() {

        MessageDefragmenter mux = new MessageDefragmenter();
        Map<Long, List<CallFrame>> map = mux.getCallFrames();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        CallResponseFrame callResponseFrame = Fixtures.callResponse(id,
            true,
            new HashMap<String, String>() {{
                put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
            }},
            Unpooled.wrappedBuffer(
                // arg1 needs to be empty
                new byte[]{0x00, 0x00},
                new byte[]{0x00, 0x00}
            )
        );
        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(callResponseFrame)
            )
        );

        CallResponseContinueFrame firstCallResponseContinueFrame = Fixtures.callResponseContinue(id, true, Unpooled.wrappedBuffer(
            // arg2 size
            new byte[]{0x00, 0x04}
        ));
        ByteBuf firstEncodedByteBuf = MessageCodec.encode(MessageCodec.encode(firstCallResponseContinueFrame));
        channel.writeInbound(firstEncodedByteBuf);
        assertEquals(1, map.size());
        assertNotNull(map.get(id));
        assertEquals(2, map.get(id).size());
        assertEquals(1, callResponseFrame.refCnt());
        assertEquals(1, firstCallResponseContinueFrame.refCnt());
        assertEquals(1, firstEncodedByteBuf.refCnt());

        CallResponseContinueFrame secondCallResponseContinueFrame = Fixtures.callResponseContinue(id, false, Unpooled.wrappedBuffer(
            new byte[]{0x00, 0x00},
            // arg3 size
            new byte[]{0x00, 0x04},
            "arg3".getBytes(StandardCharsets.UTF_8)
        ));
        ByteBuf secondEncodeByteBuf = MessageCodec.encode(MessageCodec.encode(secondCallResponseContinueFrame));
        try {
            channel.writeInbound(secondEncodeByteBuf);
            fail();
        } catch (DecoderException e) {
            assertTrue(e.getMessage().contains("wrong read index for args"));
        }
        assertEquals(map.size(), 0);
        assertNull(map.get(id));
        assertEquals(0, callResponseFrame.refCnt());
        assertEquals(1, firstCallResponseContinueFrame.refCnt());
        assertEquals(0, secondCallResponseContinueFrame.refCnt());
        assertEquals(0, secondEncodeByteBuf.refCnt());
    }

    @Test
    public void testMergeResponseMessage() {

        MessageDefragmenter mux = new MessageDefragmenter();
        Map<Long, List<CallFrame>> map = mux.getCallFrames();
        EmbeddedChannel channel = new EmbeddedChannel(mux);
        long id = 42;

        CallResponseFrame callResponseFrame = Fixtures.callResponse(id,
            true,
            new HashMap<String, String>() {{
                put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
            }},
            Unpooled.wrappedBuffer(
                // arg1 needs to be empty
                new byte[]{0x00, 0x00},
                new byte[]{0x00, 0x00}
            )
        );
        channel.writeInbound(
            MessageCodec.encode(
                MessageCodec.encode(callResponseFrame)
            )
        );
        assertEquals(1, map.size());
        assertNotNull(map.get(id));
        assertEquals(1, callResponseFrame.refCnt());

        CallResponseContinueFrame firstCallResponseContinueFrame = Fixtures.callResponseContinue(id, true, Unpooled.wrappedBuffer(
            // arg2 size
            new byte[]{0x00, 0x04},
            "arg2".getBytes(StandardCharsets.UTF_8)
        ));
        ByteBuf firstEncodedByteBuf = MessageCodec.encode(MessageCodec.encode(firstCallResponseContinueFrame));
        channel.writeInbound(firstEncodedByteBuf);
        assertEquals(1, map.size());
        assertNotNull(map.get(id));
        assertEquals(2, map.get(id).size());
        assertEquals(1, callResponseFrame.refCnt());
        assertEquals(1, firstCallResponseContinueFrame.refCnt());
        assertEquals(1, firstEncodedByteBuf.refCnt());

        CallResponseContinueFrame secondCallResponseContinueFrame = Fixtures.callResponseContinue(id, false, Unpooled.wrappedBuffer(
            new byte[]{0x00, 0x00},
            // arg3 size
            new byte[]{0x00, 0x04},
            "arg3".getBytes(StandardCharsets.UTF_8)
        ));
        ByteBuf secondEncodeByteBuf = MessageCodec.encode(MessageCodec.encode(secondCallResponseContinueFrame));
        channel.writeInbound(secondEncodeByteBuf);
        assertEquals(map.size(), 0);
        assertNull(map.get(id));
        assertEquals(0, callResponseFrame.refCnt());
        assertEquals(1, firstCallResponseContinueFrame.refCnt());
        assertEquals(1, secondCallResponseContinueFrame.refCnt());
        assertEquals(1, secondEncodeByteBuf.refCnt());

        RawMessage fullMessage = channel.readInbound();

        assertEquals(1, fullMessage.getArg1().refCnt());
        assertEquals(1, fullMessage.getArg2().refCnt());
        assertEquals(1, fullMessage.getArg3().refCnt());
        assertNotNull(fullMessage);

        assertEquals(
            0,
            fullMessage.getArg1().toString(CharsetUtil.UTF_8).length()
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

        assertEquals(0, callResponseFrame.refCnt());
        assertEquals(0, firstCallResponseContinueFrame.refCnt());
        assertEquals(0, secondCallResponseContinueFrame.refCnt());

        assertNull(channel.readInbound());

    }
}
