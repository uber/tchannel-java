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

import com.uber.tchannel.fragmentation.FragmentationState;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseContinue;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.ErrorFrame;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.schemes.ErrorResponse;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.TChannelMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageDefragmenter extends MessageToMessageDecoder<Frame> {

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int MAX_BUFFER_SIZE = TFrame.MAX_FRAME_LENGTH - TFrame.FRAME_HEADER_LENGTH;

    // Maintains a mapping of MessageId -> Partial RawMessage
    private final Map<Long, TChannelMessage> messageMap = new HashMap<>();

    // Maintains a mapping of MessageId -> Frame Defragmentation State */
    private final Map<Long, FragmentationState> defragmentationState = new HashMap<Long, FragmentationState>();

    protected Map<Long, TChannelMessage> getMessageMap() {
        return this.messageMap;
    }

    protected Map<Long, FragmentationState> getDefragmentationState() {
        return this.defragmentationState;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Frame msg, List<Object> out) throws Exception {

        if (msg instanceof CallRequestFrame) {
            this.decodeCallRequest(ctx, (CallRequestFrame) msg, out);
        } else if (msg instanceof CallResponseFrame) {
            this.decodeCallResponse(ctx, (CallResponseFrame) msg, out);
        } else if (msg instanceof CallRequestContinueFrame) {
            this.decodeCallRequestContinue(ctx, (CallRequestContinueFrame) msg, out);
        } else if (msg instanceof CallResponseContinue) {
            this.decodeCallResponseContinue(ctx, (CallResponseContinue) msg, out);
        } else if (msg instanceof ErrorFrame) {
            this.decodeErrorResponse(ctx, (ErrorFrame) msg, out);
        }

        if (!hasMore(msg)) {
            TChannelMessage completeResponse = this.messageMap.remove(msg.getId());
            out.add(completeResponse);
        }
    }

    private boolean hasMore(Frame msg) {
        if (msg instanceof CallFrame) {
            return ((CallFrame) msg).moreFragmentsFollow();
        }

        return false;
    }

    private void decodeErrorResponse(ChannelHandlerContext ctx, ErrorFrame msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) == null;
        assert this.defragmentationState.get(msg.getId()) == null;

        this.messageMap.put(msg.getId(), new ErrorResponse(
            msg.getId(),
            msg.getType(),
            msg.getMessage()
        ));
    }

    private void decodeCallRequest(ChannelHandlerContext ctx, CallRequestFrame msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) == null;
        assert this.defragmentationState.get(msg.getId()) == null;

        ByteBuf arg1 = this.readArg(msg);
        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        this.messageMap.put(msg.getId(), new RawRequest(
                msg.getId(),
                msg.getTTL(),
                msg.getService(),
                msg.getHeaders(),
                arg1,
                arg2,
                arg3
        ));

    }

    private void decodeCallResponse(ChannelHandlerContext ctx, CallResponseFrame msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) == null;
        assert this.defragmentationState.get(msg.getId()) == null;

        ByteBuf arg1 = this.readArg(msg);
        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        this.messageMap.put(msg.getId(), new RawResponse(
                msg.getId(),
                msg.getResponseCode(),
                msg.getHeaders(),
                arg1,
                arg2,
                arg3
        ));
    }

    private void decodeCallRequestContinue(ChannelHandlerContext ctx, CallRequestContinueFrame msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) != null;
        assert this.defragmentationState.get(msg.getId()) != null;

        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        RawRequest partialRequest = (RawRequest) this.messageMap.get(msg.getId());

        RawRequest updatedRequest = new RawRequest(
                partialRequest.getId(),
                partialRequest.getTTL(),
                partialRequest.getService(),
                partialRequest.getTransportHeaders(),
                partialRequest.getArg1(),
                Unpooled.wrappedBuffer(partialRequest.getArg2(), arg2),
                Unpooled.wrappedBuffer(partialRequest.getArg3(), arg3)
        );

        this.messageMap.put(msg.getId(), updatedRequest);
    }

    private void decodeCallResponseContinue(ChannelHandlerContext ctx, CallResponseContinue msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) != null;
        assert this.defragmentationState.get(msg.getId()) != null;

        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        RawResponse partialResponse = (RawResponse) this.messageMap.get(msg.getId());

        RawResponse updatedResponse = new RawResponse(
                partialResponse.getId(),
                partialResponse.getResponseCode(),
                partialResponse.getTransportHeaders(),
                partialResponse.getArg1(),
                Unpooled.wrappedBuffer(partialResponse.getArg2(), arg2),
                Unpooled.wrappedBuffer(partialResponse.getArg3(), arg3)

        );

        this.messageMap.put(msg.getId(), updatedResponse);
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

}
