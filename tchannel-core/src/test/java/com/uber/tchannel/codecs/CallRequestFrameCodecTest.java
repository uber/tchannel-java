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

package com.uber.tchannel.codecs;

import com.uber.tchannel.Fixtures;
import com.uber.tchannel.ResultCaptor;
import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.InitFrame;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class CallRequestFrameCodecTest {

    @Test
    public void testEncodeDecode() throws Exception {

        CallRequestFrame callRequestFrame = Fixtures.callRequest(42, false, Unpooled.wrappedBuffer("Hello, World!".getBytes(StandardCharsets.UTF_8)));
        CallRequestFrame inboundCallRequestFrame =
            (CallRequestFrame) MessageCodec.decode(
                CodecTestUtil.encodeDecode(
                    MessageCodec.encode(
                        ByteBufAllocator.DEFAULT, callRequestFrame
                    )
                )
            );

        assertEquals("Hello, World!", inboundCallRequestFrame.getPayload().toString(CharsetUtil.UTF_8));
        inboundCallRequestFrame.getPayload().release();
    }

    @Test
    public void testEncodeWithError() throws Exception {

        CallRequestFrame callRequestFrame = new CallRequestFrame(
            42,
            (byte) 1,
            0L,
            new Trace(0, 0, 0, (byte) 0x00),
            "service",
            new HashMap<String, String>() {{
                put(InitFrame.HOST_PORT_KEY, null);
                put(InitFrame.PROCESS_NAME_KEY, null);
            }},
            ChecksumType.NoChecksum,
            0,
            null
        );
        ByteBufAllocator spy = spy(ByteBufAllocator.DEFAULT);
        ResultCaptor<ByteBuf> byteBufResultCaptor = new ResultCaptor<>();
        doAnswer(byteBufResultCaptor).when(spy).buffer(anyInt());

        try {
            callRequestFrame.encodeHeader(spy);
            fail();
        } catch (Exception ex) {
            //expected
        }
        assertEquals(0, byteBufResultCaptor.getResult().refCnt());
    }
}
