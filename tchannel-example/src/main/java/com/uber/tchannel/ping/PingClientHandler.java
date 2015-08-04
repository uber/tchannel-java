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
package com.uber.tchannel.ping;

import com.uber.tchannel.api.RawRequest;
import com.uber.tchannel.messages.FullMessage;
import com.uber.tchannel.messages.InitMessage;
import com.uber.tchannel.messages.InitRequest;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PingClientHandler extends ChannelHandlerAdapter {

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        InitRequest initRequest = new InitRequest(0,
                InitMessage.DEFAULT_VERSION,
                new HashMap<String, String>() {
                    {
                        put(InitMessage.HOST_PORT_KEY, "0.0.0.0:0");
                        put(InitMessage.PROCESS_NAME_KEY, "test-process");
                    }
                }
        );
        ChannelFuture f = ctx.writeAndFlush(initRequest);
        f.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if (this.counter.getAndIncrement() < 100000) {
            if (this.counter.get() % 1000 == 0) {
                System.out.println(msg);
            }

            RawRequest request = new RawRequest(
                    this.counter.get(),
                    "service",
                    new HashMap<String, String>() {
                        {
                            put("as", "raw");
                        }
                    },
                    Unpooled.wrappedBuffer("endpoint".getBytes()),
                    Unpooled.wrappedBuffer("headers".getBytes()),
                    Unpooled.wrappedBuffer(String.format("payload #%d", this.counter.get()).getBytes())
            );
            ChannelFuture f = ctx.writeAndFlush(request);
            f.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        } else {
            ctx.close();
        }
        if (msg instanceof FullMessage) {
            ((FullMessage) msg).getArg1().release();
            ((FullMessage) msg).getArg2().release();
            ((FullMessage) msg).getArg3().release();
        }
        ReferenceCountUtil.release(msg);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
