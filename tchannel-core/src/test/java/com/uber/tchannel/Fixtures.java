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
package com.uber.tchannel;

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseContinueFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public final class Fixtures {

    private Fixtures() {}

    public static CallRequestFrame callRequest(long id, boolean moreFragments, ByteBuf payload) {
        return callRequest(id, moreFragments, new HashMap<String, String>(), payload);
    }

    public static CallRequestFrame callRequest(long id,
                                               boolean moreFragments,
                                               Map<String, String> headers,
                                               ByteBuf payload) {
        CallRequestFrame callRequestFrame = new CallRequestFrame(
            id,
            moreFragments ? (byte) 1 : (byte) 0,
            0L,
            new Trace(0, 0, 0, (byte) 0x00),
            "service",
            headers,
            ChecksumType.NoChecksum,
            0,
            null
        );

        ByteBuf byteBuf = callRequestFrame.encodeHeader(PooledByteBufAllocator.DEFAULT);
        callRequestFrame.setPayload(Unpooled.wrappedBuffer(byteBuf, payload));

        checkState(byteBuf.refCnt() == 1);
        return callRequestFrame;
    }

    public static CallRequestContinueFrame callRequestContinue(long id, boolean moreFragments, ByteBuf payload) {
        CallRequestContinueFrame callRequestContinueFrame = new CallRequestContinueFrame(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ChecksumType.NoChecksum,
                0,
                null
        );

        ByteBuf byteBuf = callRequestContinueFrame.encodeHeader(PooledByteBufAllocator.DEFAULT);
        callRequestContinueFrame.setPayload(Unpooled.wrappedBuffer(byteBuf, payload));
        checkState(byteBuf.refCnt() == 1);
        return callRequestContinueFrame;
    }

    public static CallResponseFrame callResponse(long id, boolean moreFragments, ByteBuf payload) {
        return callResponse(id, moreFragments, new HashMap<String, String>(), payload);
    }

    public static CallResponseFrame callResponse(long id,
                                                 boolean moreFragments,
                                                 Map<String, String> headers,
                                                 ByteBuf payload) {
        CallResponseFrame callResponseFrame = new CallResponseFrame(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ResponseCode.OK,
                new Trace(0, 0, 0, (byte) 0x00),
                headers,
                ChecksumType.NoChecksum,
                0,
                null
        );

        ByteBuf byteBuf = callResponseFrame.encodeHeader(PooledByteBufAllocator.DEFAULT);
        callResponseFrame.setPayload(Unpooled.wrappedBuffer(byteBuf, payload));

        checkState(byteBuf.refCnt() == 1);
        return callResponseFrame;
    }

    public static CallResponseContinueFrame callResponseContinue(long id, boolean moreFragments, ByteBuf payload) {
        CallResponseContinueFrame callResponseContinueFrame = new CallResponseContinueFrame(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ChecksumType.NoChecksum,
                0,
                payload
        );

        ByteBuf byteBuf = callResponseContinueFrame.encodeHeader(PooledByteBufAllocator.DEFAULT);
        callResponseContinueFrame.setPayload(Unpooled.wrappedBuffer(byteBuf, payload));
        checkState(byteBuf.refCnt() == 1);
        return callResponseContinueFrame;
    }
}
