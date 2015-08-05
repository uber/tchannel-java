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

package com.uber.tchannel.misc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteBufTests {

    @Test
    public void testCompositeByteBufRetainRelease() {
        ByteBuf arg1 = ByteBufAllocator.DEFAULT.buffer();
        arg1.writeByte((byte) 0xDE);
        arg1.writeByte((byte) 0xAD);
        arg1.writeByte((byte) 0xBE);
        arg1.writeByte((byte) 0xEF);
        ByteBuf arg2 = ByteBufAllocator.DEFAULT.buffer();
        arg2.writeInt(42);
        ByteBuf arg3 = ByteBufAllocator.DEFAULT.buffer();
        ByteBufUtil.writeUtf8(arg3, "Hello, World!");

        arg1.retain();
        CompositeByteBuf payload = (CompositeByteBuf) Unpooled.wrappedBuffer(arg1, arg2, arg3);
        assertEquals(1, payload.refCnt());
        assertEquals(2, arg1.refCnt());
        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());

        payload.release();
        assertEquals(0, payload.refCnt());
        assertEquals(1, arg1.refCnt());
        assertEquals(0, arg2.refCnt());
        assertEquals(0, arg3.refCnt());
    }
}
