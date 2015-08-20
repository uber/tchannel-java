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

import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.fragmentation.DefragmentationState;
import com.uber.tchannel.framing.TFrame;
import com.uber.tchannel.messages.CallMessage;
import com.uber.tchannel.messages.CallRequest;
import com.uber.tchannel.messages.CallRequestContinue;
import com.uber.tchannel.messages.CallResponse;
import com.uber.tchannel.messages.CallResponseContinue;
import com.uber.tchannel.schemes.RawMessage;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageMultiplexer extends MessageToMessageCodec<CallMessage, RawMessage> {

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int MAX_BUFFER_SIZE = TFrame.MAX_FRAME_LENGTH - TFrame.FRAME_HEADER_LENGTH;

    // Maintains a mapping of MessageId -> Partial RawMessage
    private final Map<Long, RawMessage> messageMap = new HashMap<>();

    // Maintains a mapping of MessageId -> Message Defragmentation State */
    private final Map<Long, DefragmentationState> defragmentationState = new HashMap<Long, DefragmentationState>();

    protected Map<Long, RawMessage> getMessageMap() {
        return this.messageMap;
    }

    protected Map<Long, DefragmentationState> getDefragmentationState() {
        return this.defragmentationState;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, RawMessage msg, List<Object> out) throws Exception {

        ByteBuf buffer = ctx.alloc().buffer(DEFAULT_BUFFER_SIZE, MAX_BUFFER_SIZE);
        // arg1~2
        this.writeArg(msg.getArg1(), buffer);

        // arg2~2
        this.writeArg(msg.getArg2(), buffer);

        // arg3~2
        this.writeArg(msg.getArg3(), buffer);

        if (msg instanceof RawRequest) {
            RawRequest rawRequest = (RawRequest) msg;

            CallRequest callRequest = new CallRequest(
                    rawRequest.getId(),
                    (byte) 0x00,
                    0,
                    new Trace(0, 0, 0, (byte) 0x00),
                    rawRequest.getService(),
                    rawRequest.getTransportHeaders(),
                    ChecksumType.NoChecksum,
                    0,
                    buffer
            );

            out.add(callRequest);
        } else if (msg instanceof RawResponse) {
            RawResponse rawResponse = (RawResponse) msg;

            CallResponse callResponse = new CallResponse(
                    rawResponse.getId(),
                    (byte) 0x00,
                    CallResponse.CallResponseCode.OK,
                    new Trace(0, 0, 0, (byte) 0x00),
                    rawResponse.getTransportHeaders(),
                    ChecksumType.NoChecksum,
                    0,
                    buffer
            );

            out.add(callResponse);
        }

    }

    @Override
    protected void decode(ChannelHandlerContext ctx, CallMessage msg, List<Object> out) throws Exception {

        if (msg instanceof CallRequest) {
            this.decodeCallRequest(ctx, (CallRequest) msg, out);
        } else if (msg instanceof CallResponse) {
            this.decodeCallResponse(ctx, (CallResponse) msg, out);
        } else if (msg instanceof CallRequestContinue) {
            this.decodeCallRequestContinue(ctx, (CallRequestContinue) msg, out);
        } else if (msg instanceof CallResponseContinue) {
            this.decodeCallResponseContinue(ctx, (CallResponseContinue) msg, out);
        }

        if (!msg.moreFragmentsFollow()) {
            RawMessage completeResponse = this.messageMap.remove(msg.getId());
            out.add(completeResponse);
        }

    }

    private void decodeCallRequest(ChannelHandlerContext ctx, CallRequest msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) == null;
        assert this.defragmentationState.get(msg.getId()) == null;

        ByteBuf arg1 = this.readArg(msg);
        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        this.messageMap.put(msg.getId(), new RawRequest(
                msg.getId(),
                msg.getService(),
                msg.getHeaders(),
                arg1,
                arg2,
                arg3
        ));

    }

    private void decodeCallResponse(ChannelHandlerContext ctx, CallResponse msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) == null;
        assert this.defragmentationState.get(msg.getId()) == null;

        ByteBuf arg1 = this.readArg(msg);
        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        this.messageMap.put(msg.getId(), new RawResponse(
                msg.getId(),
                msg.getHeaders(),
                arg1,
                arg2,
                arg3
        ));
    }

    private void decodeCallRequestContinue(ChannelHandlerContext ctx, CallRequestContinue msg, List<Object> out) {

        assert this.messageMap.get(msg.getId()) != null;
        assert this.defragmentationState.get(msg.getId()) != null;

        ByteBuf arg2 = this.readArg(msg);
        ByteBuf arg3 = this.readArg(msg);

        RawRequest partialRequest = (RawRequest) this.messageMap.get(msg.getId());

        RawRequest updatedRequest = new RawRequest(
                partialRequest.getId(),
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
                partialResponse.getTransportHeaders(),
                partialResponse.getArg1(),
                Unpooled.wrappedBuffer(partialResponse.getArg2(), arg2),
                Unpooled.wrappedBuffer(partialResponse.getArg3(), arg3)

        );

        this.messageMap.put(msg.getId(), updatedResponse);
    }

    /**
     * @param arg    the arg to write to the buffer
     * @param buffer the out buffer
     * @return the number of bytes read from the arg and written to the buffer, not including the `argSize` header
     */
    protected int writeArg(ByteBuf arg, ByteBuf buffer) {

        // Mark where we *should* write the `argSize` header
        int index = buffer.writerIndex();

        // zero-fill the `argSize` header bytes
        buffer.writeZero(2);

        // Actually write the contents of `arg`
        buffer.writeBytes(arg);

        // Read how many bytes were written after the `argSize` header
        int argSize = buffer.writerIndex() - (index + 2);

        if (argSize == 0) {
            // Backtrack and reset the index because we didn't write anything.
            buffer.writerIndex(index);
        } else {
            // Go back and write the size of `arg`
            buffer.setShort(index, argSize);
        }

        // Release the arg back to the pool
        arg.release();

        return argSize;
    }

    protected ByteBuf readArg(CallMessage msg) {

        /**
         * Get De-fragmentation State for this MessageID.
         *
         * Initialize it to PROCESSING_ARG_1 or PROCESSING_ARG_2 depending on if this is a Call{Request,Response} or a
         * Call{Request,Response}Continue message. Call{R,R}Continue messages don't carry arg1 payloads, so we skip
         * ahead to the arg2 processing state.
         */

        if (!this.defragmentationState.containsKey(msg.getId())) {
            if (msg instanceof CallRequest || msg instanceof CallResponse) {
                this.defragmentationState.put(msg.getId(), DefragmentationState.PROCESSING_ARG_1);
            } else {
                this.defragmentationState.put(msg.getId(), DefragmentationState.PROCESSING_ARG_2);
            }
        }

        DefragmentationState currentState = this.defragmentationState.get(msg.getId());

        int argLength;
        switch (currentState) {

            case PROCESSING_ARG_1:

                /* arg1~2. CANNOT be fragmented. MUST be < 16k */
                argLength = msg.getPayload().readUnsignedShort();
                assert argLength <= CallMessage.MAX_ARG1_LENGTH;
                assert msg.getPayload().readerIndex() == 2;

                /* Read a slice, retain a copy */
                ByteBuf arg1 = msg.getPayload().readSlice(argLength);
                arg1.retain();

                /* Move to the next state... */
                this.defragmentationState.put(msg.getId(), DefragmentationState.nextState(currentState));
                return arg1;

            case PROCESSING_ARG_2:

                if (msg.getPayloadSize() == 0) {
                    return Unpooled.EMPTY_BUFFER;
                }

                /* arg2~2. MAY be fragmented. No size limit */
                argLength = msg.getPayload().readUnsignedShort();

                if (argLength == 0) {
                    /* arg2 is done when it's 0 bytes */
                    this.defragmentationState.put(msg.getId(), DefragmentationState.nextState(currentState));
                    return Unpooled.EMPTY_BUFFER;
                }

                /* Read a slice, retain a copy */
                ByteBuf arg2 = msg.getPayload().readSlice(argLength);
                arg2.retain();

                return arg2;

            case PROCESSING_ARG_3:

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
                throw new RuntimeException(String.format("Unexpected 'DefragmentationState': %s", currentState));
        }

    }

}
