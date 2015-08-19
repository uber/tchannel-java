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

package com.uber.tchannel.errors;

import com.uber.tchannel.messages.ErrorMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

public class ProtocolErrorProcessor {

    public static void handleError(ChannelHandlerContext ctx, ProtocolError protocolError) {
        ChannelFuture f = ctx.writeAndFlush(new ErrorMessage(
                protocolError.getId(),
                protocolError.getErrorType(),
                protocolError.getTrace(),
                protocolError.getMessage()
        ));
        switch (protocolError.getErrorType()) {

            case Invalid:
            case Timeout:
            case Cancelled:
            case Busy:
            case Declined:
            case UnexpectedError:
            case BadRequest:
            case NetworkError:
            case Unhealthy:
                break;
            case FatalProtocolError:
                f.addListener(ChannelFutureListener.CLOSE);
                break;
            default:
                break;
        }

    }
}
