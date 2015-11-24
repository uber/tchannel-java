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

import com.uber.tchannel.api.errors.TChannelCodec;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.api.errors.TChannelProtocol;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.messages.ErrorResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.messages.TChannelMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

public final class MessageCodec {

    public static ChannelFuture write(ChannelHandlerContext ctx, Frame frame) {
        // TODO: release frame?
        ChannelFuture f = ctx.writeAndFlush(encode(ctx.alloc(), frame));
        f.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        return f;
    }

    public static TFrame encode(Frame msg) {
        return encode(PooledByteBufAllocator.DEFAULT, msg);
    }

    public static TFrame encode(ByteBufAllocator allocator, Frame msg) {
        ByteBuf buffer = null;
        if (msg instanceof CallFrame) {
            buffer = ((CallFrame)msg).getPayload();
        } else {
            buffer = msg.encodeHeader(allocator);
        }

        TFrame frame = new TFrame(buffer.writerIndex(), msg.getType(), msg.getId(), buffer);
        return frame;
    }

    public static Frame decode(TFrame frame) throws TChannelError {
        FrameType type = FrameType.fromByte(frame.type);

        if (type == null) {
            throw new TChannelProtocol("Cannot read the frame type");
        }

        switch (type) {
            case CallRequest:
                return CallRequestCodec.decode(frame);
            case CallRequestContinue:
                return CallRequestContinueCodec.decode(frame);
            case CallResponse:
                return CallResponseCodec.decode(frame);
            case CallResponseContinue:
                return CallResponseContinueCodec.decode(frame);
            case Cancel:
                return CancelCodec.decode(frame);
            case Claim:
                return ClaimCodec.decode(frame);
            case Error:
                return ErrorCodec.decode(frame);
            case InitRequest:
                return InitRequestCodec.decode(frame);
            case InitResponse:
                return InitResponseCodec.decode(frame);
            case PingRequest:
                return PingRequestCodec.decode(frame);
            case PingResponse:
                return PingResponseCodec.decode(frame);
            default:
                throw new TChannelCodec(String.format("Unknown FrameType: %s", type));
        }
    }

    public static TChannelMessage decodeCallFrames(List<CallFrame> frames) {
        if (frames.isEmpty()) {
            return null;
        }

        CallFrame first = frames.get(0);
        if (first.getType() == FrameType.CallRequest) {
            return decodeCallRequest(frames);
        } else {
            return decodeCallResponse(frames);
        }
    }

    public static ErrorResponse decodeErrorResponse(ErrorFrame frame) {
        return new ErrorResponse(
            frame.getId(),
            frame.getErrorType(),
            frame.getMessage()
        );
    }

    public static Request decodeCallRequest(List<CallFrame> frames) {

        if (frames.isEmpty()) {
            return null;
        }

        CallRequestFrame first = (CallRequestFrame) frames.get(0);
        ArgScheme scheme = ArgScheme.toScheme(
            first.getHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        if (!ArgScheme.isSupported(scheme)) {
            return null;
        }

        List<ByteBuf> args = new ArrayList<>();
        for (CallFrame frame : frames) {
            CodecUtils.readArgs(args, frame.getPayload());
            frame.release();
        }

        if (args.size() != 3) {
            for (ByteBuf arg : args) {
                arg.release();
            }

            throw new UnsupportedOperationException("The arg count is not should be 3 instead of " + args.size());
        }

        return Request.build(
            scheme,
            first.getId(),
            first.getTTL(),
            first.getService(),
            first.getHeaders(),
            args.get(0),
            args.get(1),
            args.get(2));
    }

    public static Response decodeCallResponse(List<CallFrame> frames) {

        if (frames.isEmpty()) {
            return null;
        }

        CallResponseFrame first = (CallResponseFrame) frames.get(0);
        ArgScheme scheme = ArgScheme.toScheme(
            first.getHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        if (!ArgScheme.isSupported(scheme)) {
            return null;
        }

        List<ByteBuf> args = new ArrayList<>();
        for (CallFrame frame : frames) {
            CodecUtils.readArgs(args, frame.getPayload());
            frame.release();
        }

        if (args.size() != 3) {
            for (ByteBuf arg : args) {
                arg.release();
            }

            throw new UnsupportedOperationException("The arg count is not should be 3 instead of " + args.size());
        }

        args.get(0).release();
        return Response.build(
            scheme,
            first.getId(),
            first.getResponseCode(),
            first.getHeaders(),
            args.get(1),
            args.get(2));
    }
}
