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

import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.FrameType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;

public final class CallRequestContinueCodec {

    public static TFrame encode(ByteBufAllocator allocator, CallRequestContinueFrame msg) {
        /**
         * Allocate a buffer for the rest of the pipeline
         *
         * TODO: Figure out sane initial buffer size allocation. We could calculate this dynamically based off of the
         * average payload size of the current connection.
         */
        ByteBuf buffer = allocator.buffer(CallFrame.MAX_ARG1_LENGTH, TFrame.MAX_FRAME_LENGTH);

        // flags:1
        buffer.writeByte(msg.getFlags());

        // csumtype:1
        buffer.writeByte(msg.getChecksumType().byteValue());

        // checksum -> (csum:4){0,1}
        CodecUtils.encodeChecksum(msg.getChecksum(), msg.getChecksumType(), buffer);

        // {continuation}
        buffer.writeBytes(msg.getPayload());

        TFrame frame = new TFrame(buffer.writerIndex(), FrameType.CallRequestContinue, msg.getId(), buffer);
        msg.release();
        return frame;
    }

    public static CallRequestContinueFrame decode(TFrame frame) {
        // flags:1
        byte flags = frame.payload.readByte();

        // csumtype:1
        ChecksumType checksumType = ChecksumType.fromByte(frame.payload.readByte());

        // (csum:4){0,1}
        int checksum = CodecUtils.decodeChecksum(checksumType, frame.payload);

        // {continuation}
        int payloadSize = frame.size - frame.payload.readerIndex();
        ByteBuf payload = frame.payload.readSlice(payloadSize);
//        payload.retain();

        CallRequestContinueFrame req = new CallRequestContinueFrame(frame.id, flags, checksumType, checksum, payload);
        return req;
    }

}
