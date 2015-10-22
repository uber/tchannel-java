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

import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.CallResponseContinue;
import com.uber.tchannel.frames.CancelFrame;
import com.uber.tchannel.frames.ClaimFrame;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.frames.InitRequestFrame;
import com.uber.tchannel.frames.InitResponseFrame;
import com.uber.tchannel.frames.PingRequestFrame;
import com.uber.tchannel.frames.PingResponseFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

public final class MessageCodec extends MessageToMessageCodec<TFrame, Frame> {

    // TODO: There has to be a better way to do this...
    private final CallRequestCodec callRequestCodec = new CallRequestCodec();
    private final CallRequestContinueCodec callRequestContinue = new CallRequestContinueCodec();
    private final CallResponseCodec callResponseCodec = new CallResponseCodec();
    private final CallResponseContinueCodec callResponseContinueCodec = new CallResponseContinueCodec();
    private final CancelCodec cancelCodec = new CancelCodec();
    private final ClaimCodec claimCodec = new ClaimCodec();
    private final ErrorCodec errorCodec = new ErrorCodec();
    private final InitRequestCodec initRequestCodec = new InitRequestCodec();
    private final InitResponseCodec initResponseCodec = new InitResponseCodec();
    private final PingRequestCodec pingRequestCodec = new PingRequestCodec();
    private final PingResponseCodec pingResponseCodec = new PingResponseCodec();

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame msg, List<Object> out) throws Exception {
        switch (msg.getMessageType()) {
            case CallRequest:
                this.callRequestCodec.encode(ctx, (CallRequestFrame) msg, out);
                break;
            case CallRequestContinue:
                this.callRequestContinue.encode(ctx, (CallRequestContinueFrame) msg, out);
                break;
            case CallResponse:
                this.callResponseCodec.encode(ctx, (CallResponseFrame) msg, out);
                break;
            case CallResponseContinue:
                this.callResponseContinueCodec.encode(ctx, (CallResponseContinue) msg, out);
                break;
            case Cancel:
                this.cancelCodec.encode(ctx, (CancelFrame) msg, out);
                break;
            case Claim:
                this.claimCodec.encode(ctx, (ClaimFrame) msg, out);
                break;
            case Error:
                this.errorCodec.encode(ctx, (ErrorFrame) msg, out);
                break;
            case InitRequest:
                this.initRequestCodec.encode(ctx, (InitRequestFrame) msg, out);
                break;
            case InitResponse:
                this.initResponseCodec.encode(ctx, (InitResponseFrame) msg, out);
                break;
            case PingRequest:
                this.pingRequestCodec.encode(ctx, (PingRequestFrame) msg, out);
                break;
            case PingResponse:
                this.pingResponseCodec.encode(ctx, (PingResponseFrame) msg, out);
                break;
            default:
                throw new Exception(String.format("Unknown FrameType: %s", msg.getMessageType()));

        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, TFrame frame, List<Object> out) throws Exception {
        FrameType type = FrameType.fromByte(frame.type);

        if (type == null) {
            throw new Exception("protocol exception");
        }

        switch (type) {
            case CallRequest:
                this.callRequestCodec.decode(ctx, frame, out);
                break;
            case CallRequestContinue:
                this.callRequestContinue.decode(ctx, frame, out);
                break;
            case CallResponse:
                this.callResponseCodec.decode(ctx, frame, out);
                break;
            case CallResponseContinue:
                this.callResponseContinueCodec.decode(ctx, frame, out);
                break;
            case Cancel:
                this.cancelCodec.decode(ctx, frame, out);
                break;
            case Claim:
                this.claimCodec.decode(ctx, frame, out);
                break;
            case Error:
                this.errorCodec.decode(ctx, frame, out);
                break;
            case InitRequest:
                this.initRequestCodec.decode(ctx, frame, out);
                break;
            case InitResponse:
                this.initResponseCodec.decode(ctx, frame, out);
                break;
            case PingRequest:
                this.pingRequestCodec.decode(ctx, frame, out);
                break;
            case PingResponse:
                this.pingResponseCodec.decode(ctx, frame, out);
                break;
            default:
                throw new Exception(String.format("Unknown FrameType: %s", type));
        }
    }
}
