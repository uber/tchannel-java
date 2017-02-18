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

package com.uber.tchannel.frames;

import com.uber.tchannel.api.errors.TChannelCodec;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.api.errors.TChannelProtocol;
import com.uber.tchannel.codecs.TFrame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public abstract class Frame {
    protected long id = -1;

    public final long getId() {
        return id;
    }

    public abstract FrameType getType();

    public abstract ByteBuf encodeHeader(ByteBufAllocator allocator);

    public abstract void decode(TFrame tFrame);

    public static Frame create(TFrame tFrame) throws TChannelError {
        FrameType type = FrameType.fromByte(tFrame.type);
        if (type == null) {
            throw new TChannelProtocol("Cannot read the frame type");
        }

        switch (type) {
            case CallRequest:
                return new CallRequestFrame(tFrame.id);
            case CallRequestContinue:
                return new CallRequestContinueFrame(tFrame.id);
            case CallResponse:
                return new CallResponseFrame(tFrame.id);
            case CallResponseContinue:
                return new CallResponseContinueFrame(tFrame.id);
            case Cancel:
                return new CancelFrame(tFrame.id);
            case Claim:
                return new ClaimFrame(tFrame.id);
            case Error:
                return new ErrorFrame(tFrame.id);
            case InitRequest:
                return new InitRequestFrame(tFrame.id);
            case InitResponse:
                return new InitResponseFrame(tFrame.id);
            case PingRequest:
                return new PingRequestFrame(tFrame.id);
            case PingResponse:
                return new PingResponseFrame(tFrame.id);
            default:
                throw new TChannelCodec(String.format("Unknown FrameType: %s", type));
        }
    }
}
