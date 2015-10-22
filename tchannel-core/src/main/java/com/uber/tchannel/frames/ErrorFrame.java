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

import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.tracing.Trace;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public final class ErrorFrame implements Frame {

    private final long id;
    private final ErrorType errorType;
    private final Trace tracing;
    private final String message;

    /**
     * Designated Constructor
     *
     * @param id        unique of the message
     * @param errorType the type of error this represents
     * @param tracing   tracing information
     * @param message   human readable string meant for logs
     */
    public ErrorFrame(long id, ErrorType errorType, Trace tracing, String message) {
        this.id = id;
        this.errorType = errorType;
        this.tracing = tracing;
        this.message = message;
    }

    public long getId() {
        return this.id;
    }

    public FrameType getMessageType() {
        return FrameType.Error;
    }

    public ErrorType getType() {
        return errorType;
    }

    public Trace getTracing() {
        return tracing;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format(
                "<%s id=%d message=%s>",
                this.getClass().getSimpleName(),
                this.getId(),
                this.message
        );
    }

    public static void sendError(ErrorType type, String message, RawRequest request, ChannelHandlerContext ctx) {
        ErrorFrame errorFrame = new ErrorFrame(
            request.getId(),
            type,
            // TODO: get trace from request
            new Trace(0, 0, 0, (byte) 0x00),
            message);

        ChannelFuture f = ctx.writeAndFlush(errorFrame);

        // TODO: log the errorFrame instead of firing an exception ...
        f.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

        request.release();
    }
}
