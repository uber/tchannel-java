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
package com.uber.tchannel.handlers;

import com.uber.tchannel.api.errors.TChannelProtocol;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.messages.TChannelMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.uber.tchannel.frames.ErrorFrame.sendError;

public class MessageDefragmenter extends MessageToMessageDecoder<ByteBuf> {

    private static final Logger logger = LoggerFactory.getLogger(MessageDefragmenter.class);

    // TODO: reaping the timeouts
    private final Map<Long, List<CallFrame>> callFrames = new ConcurrentHashMap<>();

    public Map<Long, List<CallFrame>> getCallFrames() {
        return callFrames;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {

        Frame frame = MessageCodec.decode(
            MessageCodec.decode(buf)
        );

        TChannelMessage msg = null;
        switch (frame.getType()) {
            case CallRequest:
            case CallResponse:
                msg = decodeCallFrame(ctx, (CallFrame) frame);
                break;
            case CallRequestContinue:
            case CallResponseContinue:
                msg = decodeCallContinueFrame((CallFrame) frame);
                break;
            case Error:
                msg = MessageCodec.decodeErrorResponse((ErrorFrame) frame);
                break;
            default:
                break;
        }

        if (msg != null) {
            out.add(msg);
        }
    }

    private boolean hasMore(Frame frame) {
        if (frame instanceof CallFrame) {
            return ((CallFrame) frame).moreFragmentsFollow();
        }

        return false;
    }

    private TChannelMessage decodeCallFrame(ChannelHandlerContext ctx, CallFrame frame) {

        ArgScheme scheme;
        if (frame.getType() == FrameType.CallRequest) {
            scheme = ArgScheme.toScheme(((CallRequestFrame) frame).getHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        } else {
            scheme = ArgScheme.toScheme(((CallResponseFrame) frame).getHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        }

        if (!ArgScheme.isSupported(scheme)) {

            if (frame.getType() == FrameType.CallRequest) {
                sendError(ErrorType.BadRequest,
                    "Arg Scheme not specified or unsupported", frame.getId(), ctx);
            } else {
                logger.error("Arg Scheme not specified or unsupported: {}", scheme);
            }

            return null;
        }

        List<CallFrame> frames = new ArrayList<>();
        frames.add(frame);
        frame.retain();

        if (!hasMore(frame)) {
            return MessageCodec.decodeCallFrames(frames);
        } else {
            callFrames.put(frame.getId(), frames);
            return null;
        }
    }

    private TChannelMessage decodeCallContinueFrame(CallFrame frame)
        throws TChannelProtocol {

        List<CallFrame> frames = callFrames.get(frame.getId());
        if (frames == null) {
            throw new TChannelProtocol("Call continue frame recieved before call frame"); // FIXME typo
        }

        frames.add(frame);
        frame.retain();

        if (!hasMore(frame)) {
            callFrames.remove(frame.getId());
            return MessageCodec.decodeCallFrames(frames);
        } else {
            return null;
        }
    }

}
