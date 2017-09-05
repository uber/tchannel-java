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
import com.uber.tchannel.frames.CallRequestFrame;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CallRequestFrameCodecTest {

    @Test
    public void testEncodeDecode() throws Exception {

        CallRequestFrame callRequestFrame = Fixtures.callRequest(42, false, Unpooled.wrappedBuffer("Hello, World!".getBytes()));
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

}
