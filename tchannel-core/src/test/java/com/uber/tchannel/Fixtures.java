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

import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.messages.CallRequest;
import com.uber.tchannel.messages.CallRequestContinue;
import com.uber.tchannel.messages.CallResponse;
import com.uber.tchannel.messages.CallResponseContinue;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;

public class Fixtures {

    public static CallRequest callRequest(long id, boolean moreFragments, ByteBuf payload) {
        return new CallRequest(
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

    public static CallRequestContinue callRequestContinue(long id, boolean moreFragments, ByteBuf payload) {
        return new CallRequestContinue(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                ChecksumType.NoChecksum,
                0,
                payload
        );
    }

    public static CallResponse callResponse(long id, boolean moreFragments, ByteBuf payload) {
        return new CallResponse(
                id,
                moreFragments ? (byte) 1 : (byte) 0,
                CallResponse.CallResponseCode.OK,
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
