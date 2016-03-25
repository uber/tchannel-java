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

import com.uber.tchannel.BaseTest;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ErrorCodecTest extends BaseTest {

    @Test
    public void testEncodeDecode() throws Exception {

        ErrorFrame errorFrame = new ErrorFrame(
                42,
                ErrorType.FatalProtocolError,
                new Trace(0, 0, 0, (byte) 0),
                "I'm sorry Dave, I can't do that."
        );

        TFrame tFrame = MessageCodec.encode(ByteBufAllocator.DEFAULT, errorFrame);
        tFrame = CodecTestUtil.encodeDecode(tFrame);

        ErrorFrame newErrorFrame =
            (ErrorFrame) MessageCodec.decode(
                tFrame
            );

        assertEquals(errorFrame.getId(), newErrorFrame.getId());
        assertEquals(errorFrame.getMessage(), newErrorFrame.getMessage());
        tFrame.release();
    }
}
