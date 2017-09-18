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

package com.uber.tchannel.messages;

import com.uber.tchannel.frames.FrameType;

/**
 * Represents a TChannel response message with `raw` arg scheme encoding.
 * <p>
 * All RPC frames over TChannel contain 3 opaque byte payloads, namely, arg{1,2,3}. TChannel makes no assumptions
 * about the contents of these frames. In order to make sense of these arg payloads, TChannel has the notion of
 * `arg messages` which define standardized schemas and serialization formats over the raw arg{1,2,3} payloads. The
 * supported `arg messages` are `thrift`, `json`, `http` and `sthrift`. These request / response frames will be built
 * on top of {@link RawRequest} and {@link ResponseMessage} frames.
 * <p>
 * <h3>From the Docs</h3>
 * The `raw` encoding is intended for any custom encodings you want to do that
 * are not part of TChannel but are application specific.
 */
public abstract class ResponseMessage implements TChannelMessage {

    protected FrameType type;

    public boolean isError() {
        return this.type == FrameType.Error;
    }

    @Override
    public FrameType getType() {
        return this.type;
    }

    @Override
    public void close() {
        release();
    }
}
