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

import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CodecUtilsTest {

    @Test
    public void testEncodeDecodeString() throws Exception {
        String str = "Hello, TChannel!";
        ByteBuf buf = Unpooled.buffer();
        CodecUtils.encodeString(str, buf);
        String newStr = CodecUtils.decodeString(buf);
        assertEquals(str, newStr);
    }

    @Test
    public void testEncodeDecodeUnicodeString() throws Exception {
        String str = "チャンネル";
        ByteBuf buf = Unpooled.buffer();
        CodecUtils.encodeString(str, buf);
        String newStr = CodecUtils.decodeString(buf);
        assertEquals(str, newStr);
    }

    @Test
    public void testEncodeDecodeEmojiString() throws Exception {
        String str = "\uD83C\uDF89\uD83C\uDF7B";
        ByteBuf buf = Unpooled.buffer();
        CodecUtils.encodeString(str, buf);
        String newStr = CodecUtils.decodeString(buf);
        assertEquals(str, newStr);
    }

    @Test
    public void testEncodeDecodeSmallString() throws Exception {
        String str = "Hello, TChannel!";
        ByteBuf buf = Unpooled.buffer();
        CodecUtils.encodeSmallString(str, buf);
        String newStr = CodecUtils.decodeSmallString(buf);
        assertEquals(str, newStr);
    }

    @Test
    public void testEncodeDecodeUnicodeSmallString() throws Exception {
        String str = "チャンネル";
        ByteBuf buf = Unpooled.buffer();
        CodecUtils.encodeSmallString(str, buf);
        String newStr = CodecUtils.decodeSmallString(buf);
        assertEquals(str, newStr);
    }

    @Test
    public void testEncodeDecodeHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        ByteBuf buf = Unpooled.buffer();

        headers.put("Hello", "TChannel");
        headers.put("您好", "通道");
        headers.put("こんにちは", "世界");

        CodecUtils.encodeHeaders(headers, buf);

        Map<String, String> newHeaders = CodecUtils.decodeHeaders(buf);
        assertEquals(headers, newHeaders);

    }

    @Test
    public void testEncodeDecodeSmallHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        ByteBuf buf = Unpooled.buffer();

        headers.put("Hello", "TChannel");
        headers.put("您好", "通道");
        headers.put("こんにちは", "世界");

        CodecUtils.encodeSmallHeaders(headers, buf);

        Map<String, String> newHeaders = CodecUtils.decodeSmallHeaders(buf);
        assertEquals(headers, newHeaders);
    }

    @Test
    public void testEncodeDecodeTrace() throws Exception {

        Trace trace = new Trace(1, 2, 3, (byte) 0x04);
        ByteBuf buf = Unpooled.buffer();
        CodecUtils.encodeTrace(trace, buf);
        Trace newTrace = CodecUtils.decodeTrace(buf);
        assertEquals(trace.parentId, newTrace.parentId);
        assertEquals(trace.spanId, newTrace.spanId);
        assertEquals(trace.traceId, newTrace.traceId);
        assertEquals(trace.traceFlags, newTrace.traceFlags);

    }

    @Test
    public void testWriteArgsNoError() {
        ByteBuf allocatedByteBuf = Unpooled.buffer(TFrame.FRAME_SIZE_LENGTH);
        UnpooledByteBufAllocator allocatorDelegate = new UnpooledByteBufAllocator(false);
        ByteBufAllocator allocator = Mockito.mock(ByteBufAllocator.class);
        when(allocator.buffer(TFrame.FRAME_SIZE_LENGTH)).thenReturn(allocatedByteBuf);
        when(allocator.compositeBuffer()).thenReturn(allocatorDelegate.compositeBuffer());
        List<ByteBuf> args = new ArrayList<>();
        args.add(Unpooled.wrappedBuffer("arg1".getBytes()));
        CodecUtils.writeArgs(allocator, Unpooled.wrappedBuffer("header".getBytes()), args);

        verify(allocator, times(1)).buffer(TFrame.FRAME_SIZE_LENGTH);
        assertEquals(1, allocatedByteBuf.refCnt());
    }

    @Test
    public void testWriteArgsSecondArgWriteFails() {
        ByteBuf allocatedByteBuf1 = Unpooled.buffer(TFrame.FRAME_SIZE_LENGTH);
        ByteBuf allocatedByteBuf2 = Unpooled.buffer(TFrame.FRAME_SIZE_LENGTH);
        ByteBufAllocator allocator = Mockito.mock(ByteBufAllocator.class);
        when(allocator.buffer(TFrame.FRAME_SIZE_LENGTH)).thenReturn(allocatedByteBuf1).thenReturn(allocatedByteBuf2);
        ByteBuf arg1 = Unpooled.wrappedBuffer("arg1".getBytes());
        ByteBuf arg2 = Mockito.mock(ByteBuf.class);
        when(arg2.readableBytes()).thenReturn(10);
        when(arg2.readSlice(anyInt())).thenThrow(new RuntimeException("Can't read"));

        List<ByteBuf> args = new ArrayList<>();
        args.add(arg1);
        args.add(arg2);
        try {
            CodecUtils.writeArgs(allocator, Unpooled.wrappedBuffer("header".getBytes()),args);
            fail();
        } catch (Exception e) {
            assertEquals("Can't read", e.getMessage());
        }

        verify(allocator, times(2)).buffer(TFrame.FRAME_SIZE_LENGTH);
        assertEquals(0, allocatedByteBuf1.refCnt());
        assertEquals(0, allocatedByteBuf2.refCnt());
    }

}
