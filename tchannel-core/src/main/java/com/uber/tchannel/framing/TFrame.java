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
package com.uber.tchannel.framing;

import com.uber.tchannel.messages.MessageType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

/**
 * {@link TFrame} represents a common TChannel frame that is a primitive carrier for all messages in the TChannel
 * protocol. The {@linkplain TFrame} has a simple 16-byte header that contains information on the `size` of the payload,
 * the TChannel message `type` and the `id` of the message.
 * <h3>From the Docs</h3>
 * Each message is encapsulated in a frame with some additional information that is common across all message types.
 * Part of that framing information is an id. This id is chosen by the requestor when sending a request message.
 * When responding to a request, the responding node uses the message id in the request frame for the response.
 * Each frame has a type which describes the format of the frame's body. Depending on the frame type, some bodies
 * are 0 bytes.
 */
public class TFrame implements ByteBufHolder {

    public static final int MAX_FRAME_LENGTH = 65536;
    public static final int FRAME_HEADER_LENGTH = 16;

    /**
     * Payload size
     * <p>
     * Does *not* include the 16 bytes for the frame header
     */
    public final int size;

    // Payload message type
    public final byte type;

    // Message id
    public final long id;

    // Contents of the payload
    public final ByteBuf payload;

    public TFrame(int size, byte type, long id, ByteBuf payload) {
        this.size = size;
        this.type = type;
        this.id = id;
        this.payload = payload;
    }

    public TFrame(int size, MessageType messageType, long id, ByteBuf payload) {
        this(size, messageType.byteValue(), id, payload);
    }

    @Override
    public String toString() {
        return String.format(
                "<%s size=%d type=0x%d id=%d payload=%s>",
                this.getClass().getSimpleName(),
                this.size,
                this.type,
                this.id,
                this.payload
        );
    }

    public ByteBuf content() {
        return this.payload;
    }

    public ByteBufHolder copy() {
        return new TFrame(this.size, this.type, this.id, this.payload.copy());
    }

    public ByteBufHolder duplicate() {
        return new TFrame(this.size, this.type, this.id, this.payload.duplicate());
    }

    public ByteBufHolder retain() {
        this.payload.retain();
        return this;
    }

    public ByteBufHolder retain(int i) {
        this.payload.retain(i);
        return this;
    }

    public ByteBufHolder touch() {
        this.payload.touch();
        return this;
    }

    public ByteBufHolder touch(Object o) {
        this.payload.touch(o);
        return this;
    }

    public int refCnt() {
        return this.payload.refCnt();
    }

    public boolean release() {
        return this.payload.release();
    }

    public boolean release(int i) {
        return this.payload.release(i);
    }
}
