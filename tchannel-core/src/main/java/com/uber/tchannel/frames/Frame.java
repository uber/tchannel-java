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

        Frame frame = null;
        switch (type) {
            case CallRequest:
                frame = new CallRequestFrame(tFrame.id);
                break;
            case CallRequestContinue:
                frame = new CallRequestContinueFrame(tFrame.id);
                break;
            case CallResponse:
                frame = new CallResponseFrame(tFrame.id);
                break;
            case CallResponseContinue:
                frame = new CallResponseContinueFrame(tFrame.id);
                break;
            case Cancel:
                frame = new CancelFrame(tFrame.id);
                break;
            case Claim:
                frame = new ClaimFrame(tFrame.id);
                break;
            case Error:
                frame = new ErrorFrame(tFrame.id);
                break;
            case InitRequest:
                frame = new InitRequestFrame(tFrame.id);
                break;
            case InitResponse:
                frame = new InitResponseFrame(tFrame.id);
                break;
            case PingRequest:
                frame = new PingRequestFrame(tFrame.id);
                break;
            case PingResponse:
                frame = new PingResponseFrame(tFrame.id);
                break;
            default:
                throw new TChannelCodec(String.format("Unknown FrameType: %s", type));
        }

        return frame;
    }
}
