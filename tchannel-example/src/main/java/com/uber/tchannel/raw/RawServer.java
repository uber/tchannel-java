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

package com.uber.tchannel.raw;

import com.uber.tchannel.api.TChannel;
import io.netty.channel.ChannelFuture;

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.handlers.JSONRequestHandler;

import java.net.InetAddress;

public class RawServer {
    public static void main(String[] args) throws Exception {
        final TChannel tchannel = new TChannel.Builder("server")
                .register("func", new RawServer.RawDefaultRequestHandler())
                .setServerHost(InetAddress.getByName("127.0.0.1"))
                .setServerPort(8888)
                .build();

        ChannelFuture f = tchannel.listen();

        f.channel().closeFuture().sync();

        tchannel.shutdown();
    }

    protected static class RawDefaultRequestHandler extends JSONRequestHandler<String, String> {
        public Response<String> handleImpl(Request<String> request) {
            System.out.println(request);
            return new Response.Builder<>("Hi", request.getEndpoint(), ResponseCode.OK).build();
        }
    }
}

