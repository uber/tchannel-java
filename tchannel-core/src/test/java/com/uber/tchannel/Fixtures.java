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
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.CallResponseContinue;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;

public class Fixtures {

    public static CallRequestFrame callRequest(long id, boolean moreFragments, ByteBuf payload) {
        return new CallRequestFrame(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                0L,
                new Trace(0, 0, 0, (byte) 0x00),
                "service",
                new HashMap<String, String>(),
                ChecksumType.NoChecksum,
                0,
                payload
        );
    }

    public static CallRequestContinueFrame callRequestContinue(long id, boolean moreFragments, ByteBuf payload) {
        return new CallRequestContinueFrame(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ChecksumType.NoChecksum,
                0,
                payload
        );
    }

    public static CallResponseFrame callResponse(long id, boolean moreFragments, ByteBuf payload) {
        return new CallResponseFrame(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ResponseCode.OK,
                new Trace(0, 0, 0, (byte) 0x00),
                new HashMap<String, String>(),
                ChecksumType.NoChecksum,
                0,
                payload
        );
    }

    public static CallResponseContinue callResponseContinue(long id, boolean moreFragments, ByteBuf payload) {
        return new CallResponseContinue(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ChecksumType.NoChecksum,
                0,
                payload
        );
    }

}
