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

import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.fragmentation.FragmentationState;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseContinue;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.ErrorResponse;
import com.uber.tchannel.schemes.Response;
import com.uber.tchannel.schemes.Request;
import com.uber.tchannel.schemes.TChannelMessage;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.uber.tchannel.frames.ErrorFrame.sendError;

public class MessageDefragmenter extends MessageToMessageDecoder<Frame> {

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int MAX_BUFFER_SIZE = TFrame.MAX_FRAME_LENGTH - TFrame.FRAME_HEADER_LENGTH;

    // Maintains a mapping of MessageId -> Partial RawMessage
    private final Map<Long, TChannelMessage> messageMap = new HashMap<>();

    private final Map<Long, ErrorFrame> errorMap = new HashMap<>();

    // Maintains a mapping of MessageId -> Frame Defragmentation State */
    private final Map<Long, FragmentationState> defragmentationState = new HashMap<Long, FragmentationState>();

    protected Map<Long, TChannelMessage> getMessageMap() {
        return this.messageMap;
    }

    protected Map<Long, FragmentationState> getDefragmentationState() {
        return this.defragmentationState;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {

        if (frame instanceof CallRequestFrame) {
            this.decodeCallRequest(ctx, (CallRequestFrame) frame, out);
        } else if (frame instanceof CallResponseFrame) {
            this.decodeCallResponse(ctx, (CallResponseFrame) frame, out);
        } else if (frame instanceof CallRequestContinueFrame) {
            this.decodeCallRequestContinue(ctx, (CallRequestContinueFrame) frame, out);
        } else if (frame instanceof CallResponseContinue) {
            this.decodeCallResponseContinue(ctx, (CallResponseContinue) frame, out);
        } else if (frame instanceof ErrorFrame) {
            this.decodeErrorResponse(ctx, (ErrorFrame) frame, out);
        }

        if (this.errorMap.remove(frame.getId()) != null) {
            return;
        }

        if (!hasMore(frame)) {
            TChannelMessage msg = this.messageMap.remove(frame.getId());
            out.add(msg);
        }

        // TODO: check where frame's playload is released
    }

    private boolean hasMore(Frame frame) {
        if (frame instanceof CallFrame) {
            return ((CallFrame) frame).moreFragmentsFollow();
        }

        return false;
    }

    private void decodeErrorResponse(ChannelHandlerContext ctx, ErrorFrame frame, List<Object> out) {

        this.messageMap.put(frame.getId(), new ErrorResponse(
            frame.getId(),
            frame.getType(),
            frame.getMessage()
        ));
    }

    private void decodeCallRequest(ChannelHandlerContext ctx, CallRequestFrame frame, List<Object> out) {

        ArgScheme scheme = ArgScheme.toScheme(frame.getHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        if (!ArgScheme.isSupported(scheme)) {
            ErrorFrame error = sendError(ErrorType.BadRequest, "Invalid arg schema", frame.getId(), ctx);
            this.errorMap.put(frame.getId(), error);
            return;
        }

        ByteBuf arg1 = this.readArg(frame);
        ByteBuf arg2 = this.readArg(frame);
        ByteBuf arg3 = this.readArg(frame);

        Request req = Request.build(
            scheme,
            frame.getId(),
            frame.getTTL(),
            frame.getService(),
            frame.getHeaders(),
            arg1,
            arg2,
            arg3);

        this.messageMap.put(frame.getId(), req);
    }

    private void decodeCallResponse(ChannelHandlerContext ctx, CallResponseFrame frame, List<Object> out) {

        ArgScheme scheme = ArgScheme.toScheme(frame.getHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        if (!ArgScheme.isSupported(scheme)) {
            // TODO: logging, or more?
            return;
        }

        // ignore arg1
        ByteBuf arg1 = this.readArg(frame);
        arg1.release();
        ByteBuf arg2 = this.readArg(frame);
        ByteBuf arg3 = this.readArg(frame);

        Response res = Response.build(
            scheme,
            frame.getId(),
            frame.getResponseCode(),
            frame.getHeaders(),
            arg2,
            arg3);

        this.messageMap.put(frame.getId(), res);
    }

    private void decodeCallRequestContinue(ChannelHandlerContext ctx, CallRequestContinueFrame frame, List<Object> out) {

        Request req = (Request) this.messageMap.get(frame.getId());
        if (req == null) {
            ErrorFrame error = sendError(ErrorType.BadRequest, "call request continue without call request", frame.getId(), ctx);
            this.errorMap.put(frame.getId(), error);
            return;
        }

        ByteBuf arg2 = this.readArg(frame);
        ByteBuf arg3 = this.readArg(frame);
        req.appendArg2(arg2);
        req.appendArg3(arg3);
    }

    private void decodeCallResponseContinue(ChannelHandlerContext ctx, CallResponseContinue msg, List<Object> out) {

        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        Response res = (Response) this.messageMap.get(msg.getId());
        if (res == null) {
            // TODO: just ignore?
            // TODO: logging
            return;
        }

        res.appendArg2(arg2);
        res.appendArg3(arg3);
    }

    protected ByteBuf readArg(CallFrame msg) {

        /**
         * Get De-fragmentation State for this MessageID.
         *
         * Initialize it to ARG1 or ARG2 depending on if this is a Call{Request,Response} or a
         * Call{Request,Response}Continue message. Call{R,R}Continue frames don't carry arg1 payloads, so we skip
         * ahead to the arg2 processing state.
         */

        if (!this.defragmentationState.containsKey(msg.getId())) {
            if (msg instanceof CallRequestFrame || msg instanceof CallResponseFrame) {
                this.defragmentationState.put(msg.getId(), FragmentationState.ARG1);
            } else {
                this.defragmentationState.put(msg.getId(), FragmentationState.ARG2);
            }
        }

        FragmentationState currentState = this.defragmentationState.get(msg.getId());

        int argLength;
        switch (currentState) {

            case ARG1:

                /* arg1~2. CANNOT be fragmented. MUST be < 16k */
                argLength = msg.getPayload().readUnsignedShort();
                assert argLength <= CallFrame.MAX_ARG1_LENGTH;
                assert msg.getPayload().readerIndex() == 2;

                /* Read a slice, retain a copy */
                ByteBuf arg1 = msg.getPayload().readSlice(argLength);
                arg1.retain();

                /* Move to the next state... */
                this.defragmentationState.put(msg.getId(), FragmentationState.nextState(currentState));
                return arg1;

            case ARG2:

                if (msg.getPayloadSize() == 0) {
                    return Unpooled.EMPTY_BUFFER;
                }

                /* arg2~2. MAY be fragmented. No size limit */
                argLength = msg.getPayload().readUnsignedShort();

                if (argLength == 0) {
                    /* arg2 is done when it's 0 bytes */
                    this.defragmentationState.put(msg.getId(), FragmentationState.nextState(currentState));
                    return Unpooled.EMPTY_BUFFER;
                }

                /* Read a slice, retain a copy */
                ByteBuf arg2 = msg.getPayload().readSlice(argLength);
                arg2.retain();

                return arg2;

            case ARG3:

                if (msg.getPayloadSize() == 0) {
                    return Unpooled.EMPTY_BUFFER;
                }

                /* arg3~2. MAY be fragmented. No size limit */
                argLength = msg.getPayload().readUnsignedShort();

                if (argLength == 0) {
                    /* arg3 is done when 'No Frames Remaining' flag is set, or 0 bytes remain */
                    this.defragmentationState.remove(msg.getId());
                    return Unpooled.EMPTY_BUFFER;
                }

                /* Read a slice, retain a copy */
                ByteBuf arg3 = msg.getPayload().readSlice(argLength);
                arg3.retain();

                /* If 'No Frames Remaining', we're done with this MessageId */
                if (!msg.moreFragmentsFollow()) {
                    this.defragmentationState.remove(msg.getId());
                }
                return arg3;

            default:
                throw new RuntimeException(String.format("Unexpected 'FragmentationState': %s", currentState));
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // TODO: logging instead of print
        TChannelUtilities.PrintException(cause);
    }
}
