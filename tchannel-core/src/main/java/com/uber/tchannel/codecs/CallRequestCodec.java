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
import com.uber.tchannel.fragmentation.DefragmentationState;
import com.uber.tchannel.framing.TFrame;
import com.uber.tchannel.messages.CallMessage;
import com.uber.tchannel.messages.CallRequest;
import com.uber.tchannel.messages.CallRequestContinue;
import com.uber.tchannel.messages.MessageType;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.util.ReferenceCountUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CallRequestCodec extends MessageToMessageCodec<TFrame, CallMessage> {

    private final Map<Long, DefragmentationState> defragmentationState = new HashMap<Long, DefragmentationState>();

    protected Map<Long, DefragmentationState> getDefragmentationState() {
        return defragmentationState;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, CallMessage msg, List<Object> out) throws Exception {
        MessageType type = msg.getMessageType();
        switch (type) {
            case CallRequest:
                this.encodeCallRequest(ctx, (CallRequest) msg, out);
                break;
            case CallRequestContinue:
                this.encodeCallRequestContinue(ctx, (CallRequestContinue) msg, out);
                break;
        }

    }

    @Override
    protected void decode(ChannelHandlerContext ctx, TFrame frame, List<Object> out) {
        MessageType type = MessageType.fromByte(frame.type).get();
        switch (type) {
            case CallRequest:
                this.decodeCallRequest(ctx, frame, out);
                break;
            case CallRequestContinue:
                this.decodeCallRequestContinue(ctx, frame, out);
                break;
        }
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {

        // Will accept any TFrame, we only want CallRequest or CallRequestContinue messages
        if (super.acceptInboundMessage(msg)) {
            TFrame frame = (TFrame) msg;
            MessageType type = MessageType.fromByte(frame.type).get();
            switch (type) {
                case CallRequest:
                case CallRequestContinue:
                    return true;
                default:
                    return false;
            }
        } else {
            return false;
        }

    }

    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {

        // Will accept any CallMessage, we only want CallRequest or CallRequestContinue messages
        if (super.acceptOutboundMessage(msg)) {
            return (msg instanceof CallRequest || msg instanceof CallRequestContinue);
        } else {
            return false;
        }

    }

    protected void encodeCallRequest(ChannelHandlerContext ctx, CallRequest msg, List<Object> out) {
        /* Allocate a buffer for the rest of the pipeline */
        /* TODO: Figure out sane initial buffer size allocation */
        ByteBuf buffer = ctx.alloc().buffer(CallMessage.MAX_ARG1_LENGTH, TFrame.MAX_FRAME_LENGTH);

        // flags:1
        buffer.writeByte(msg.getFlags());

        // ttl:4
        buffer.writeInt((int) msg.getTtl());

        // tracing:25
        CodecUtils.encodeTrace(msg.getTracing(), buffer);

        // service~1
        CodecUtils.encodeSmallString(msg.getService(), buffer);

        // nh:1 (hk~1, hv~1){nh}
        CodecUtils.encodeSmallHeaders(msg.getHeaders(), buffer);

        // csumtype:1
        buffer.writeByte(msg.getChecksumType().byteValue());

        // (csum:4){0,1}
        switch (msg.getChecksumType()) {
            case Adler32:
            case FarmhashFingerPrint32:
            case CRC32C:
                buffer.writeInt(msg.getChecksum());
                break;
            case NoChecksum:
            default:
                break;
        }

        // arg1~2
        int bytesWritten = this.writeArg(msg.getArg1(), buffer);
        assert bytesWritten <= CallMessage.MAX_ARG1_LENGTH;
        if (bytesWritten == 0 && !msg.moreFragmentsFollow()) {
            this.writeEmptyArg(buffer);
        }

        // arg2~2
        bytesWritten = this.writeArg(msg.getArg2(), buffer);

        // If this is the last frame, we need to close out arg2
        if (bytesWritten == 0 && !msg.moreFragmentsFollow()) {
            this.writeEmptyArg(buffer);
        }

        // arg3~2
        bytesWritten = this.writeArg(msg.getArg3(), buffer);

        // If this is the last frame, we need to close out arg3
        if (bytesWritten == 0 && !msg.moreFragmentsFollow()) {
            this.writeEmptyArg(buffer);
        }

        TFrame frame = new TFrame(buffer.writerIndex(), MessageType.CallRequest, msg.getId(), buffer);
        out.add(frame);

    }

    protected void writeEmptyArg(ByteBuf buffer) {
        // zero-fill the `argSize` header bytes
        buffer.writeZero(2);
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
        ReferenceCountUtil.release(arg);

        return argSize;
    }

    protected void encodeCallRequestContinue(ChannelHandlerContext ctx, CallRequestContinue msg, List<Object> out) {
    }

    protected void decodeCallRequest(ChannelHandlerContext ctx, TFrame frame, List<Object> out) {
        ByteBuf buffer = frame.payload.readSlice(frame.size);
        buffer.retain();

        // flags:1
        byte flags = buffer.readByte();

        // ttl:4
        long ttl = buffer.readUnsignedInt();

        // tracing:25
        Trace trace = CodecUtils.decodeTrace(buffer);

        // service~1
        String service = CodecUtils.decodeSmallString(buffer);

        // nh:1 (hk~1, hv~1){nh}
        Map<String, String> headers = CodecUtils.decodeSmallHeaders(buffer);

        // csumtype:1
        ChecksumType checksumType = ChecksumType.fromByte(buffer.readByte()).get();

        // (csum:4){0,1}
        int checksum = 0;
        switch (checksumType) {
            case Adler32:
            case FarmhashFingerPrint32:
            case CRC32C:
                checksum = buffer.readInt();
                break;
            case NoChecksum:
            default:
                break;
        }

        // arg1~2 arg2~2 arg3~2
        ByteBuf arg1 = this.readArg(frame.id, frame.size, this.moreFragmentsFollow(flags), buffer);
        ByteBuf arg2 = this.readArg(frame.id, frame.size, this.moreFragmentsFollow(flags), buffer);
        ByteBuf arg3 = this.readArg(frame.id, frame.size, this.moreFragmentsFollow(flags), buffer);

        CallRequest req = new CallRequest(
                frame.id, flags, ttl, trace, service, headers, checksumType, checksum, arg1, arg2, arg3
        );

        out.add(req);
        ReferenceCountUtil.release(buffer);
    }

    protected void decodeCallRequestContinue(ChannelHandlerContext ctx, TFrame frame, List<Object> out) {
        ByteBuf buffer = frame.payload.readSlice(frame.size);
        buffer.retain();

        // flags:1
        byte flags = buffer.readByte();

        // csumtype:1
        ChecksumType checksumType = ChecksumType.fromByte(buffer.readByte()).get();

        // (csum:4){0,1}
        int checksum = 0;
        switch (checksumType) {
            case NoChecksum:
                break;
            case Adler32:
            case FarmhashFingerPrint32:
            case CRC32C:
                checksum = buffer.readInt();
                break;
        }

        // {continuation}
        ByteBuf arg2 = this.readArg(frame.id, frame.size, this.moreFragmentsFollow(flags), buffer);
        ByteBuf arg3 = this.readArg(frame.id, frame.size, this.moreFragmentsFollow(flags), buffer);

        CallRequestContinue req = new CallRequestContinue(
                frame.id, flags, checksumType, checksum, arg2, arg3
        );

        out.add(req);
        ReferenceCountUtil.release(buffer);

    }

    protected int bytesRemaining(ByteBuf buffer, int size) {
        return size - buffer.readerIndex();
    }

    protected boolean moreFragmentsFollow(byte flags) {
        return ((flags & CallMessage.MORE_FRAGMENTS_REMAIN_MASK) == 0);
    }

    protected ByteBuf readArg(long id, int size, boolean moreFragmentsFollow, ByteBuf buffer) {

        /* Get Defragmentation State for this MessageID, or initialize it to PROCESSING_ARG_1 */
        DefragmentationState currentState = this.defragmentationState.getOrDefault(
                id,
                DefragmentationState.PROCESSING_ARG_1
        );

        /* Return early if there are no bytes remaining in the frame */
        if (this.bytesRemaining(buffer, size) <= 0) {
            return Unpooled.EMPTY_BUFFER;
        }

        int argLength;
        switch (currentState) {

            case PROCESSING_ARG_1:

                /* arg1~2. CANNOT be fragmented. MUST be < 16k */
                argLength = buffer.readUnsignedShort();
                assert argLength <= CallMessage.MAX_ARG1_LENGTH;

                /* Read a slice, retain a copy */
                ByteBuf arg1 = buffer.readSlice(argLength);
                arg1.retain();

                /* Move to the next state... */
                this.defragmentationState.put(id, DefragmentationState.nextState(currentState));
                return arg1;

            case PROCESSING_ARG_2:

                /* arg2~2. MAY be fragmented. No size limit */
                argLength = buffer.readUnsignedShort();

                if (argLength == 0) {
                    /* arg2 is done when it's 0 bytes */
                    this.defragmentationState.put(id, DefragmentationState.nextState(currentState));
                    return Unpooled.EMPTY_BUFFER;
                }

                /* Read a slice, retain a copy */
                ByteBuf arg2 = buffer.readSlice(argLength);
                arg2.retain();

                return arg2;

            case PROCESSING_ARG_3:

                /* arg3~2. MAY be fragmented. No size limit */
                argLength = buffer.readUnsignedShort();

                if (argLength == 0) {
                    /* arg3 is done when 'No Frames Remaining' flag is set, or 0 bytes remain */
                    this.defragmentationState.remove(id);
                    return Unpooled.EMPTY_BUFFER;
                }

                /* Read a slice, retain a copy */
                ByteBuf arg3 = buffer.readSlice(argLength);
                arg3.retain();

                /* If 'No Frames Remaining', we're done with this MessageId */
                if (!moreFragmentsFollow) {
                    this.defragmentationState.remove(id);
                }
                return arg3;

            default:
                throw new RuntimeException(String.format("Unexpected 'DefragmentationState': %s", currentState));
        }

    }

}
