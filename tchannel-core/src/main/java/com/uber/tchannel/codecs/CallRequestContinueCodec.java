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
import com.uber.tchannel.framing.TFrame;
import com.uber.tchannel.messages.CallRequestContinue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

public class CallRequestContinueCodec extends MessageToMessageCodec<TFrame, CallRequestContinue> {

    @Override
    protected void encode(ChannelHandlerContext ctx, CallRequestContinue msg, List<Object> out) throws Exception {

    }

    @Override
    protected void decode(ChannelHandlerContext ctx, TFrame frame, List<Object> out) throws Exception {
        // flags:1
        byte flags = frame.payload.readByte();

        // csumtype:1
        ChecksumType checksumType = ChecksumType.fromByte(frame.payload.readByte()).get();

        // (csum:4){0,1}
        int checksum = 0;
        switch (checksumType) {
            case NoChecksum:
                break;
            case Adler32:
            case FarmhashFingerPrint32:
            case CRC32C:
                checksum = frame.payload.readInt();
                break;
        }

        // {continuation}
        int payloadSize = frame.size - frame.payload.readerIndex();
        ByteBuf payload = frame.payload.readSlice(payloadSize);
        payload.retain();

        CallRequestContinue req = new CallRequestContinue(frame.id, flags, checksumType, checksum, payload);
        out.add(req);
    }

}
