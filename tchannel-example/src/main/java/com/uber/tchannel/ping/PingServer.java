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

import com.uber.tchannel.api.RawRequestHandler;
import com.uber.tchannel.api.RawResponse;
import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PingServer {

    private int port;

    public PingServer(int port) {
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        int port = 8888;
        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }

        System.out.println(String.format("Starting server on port: %d", port));
        new PingServer(port).run();
        System.out.println("Stopping server...");
    }

    public void run() throws Exception {
        TChannel tchannel = new TChannel("server").register("endpoint", new RawRequestHandler() {
            public Response<ByteBuf> handle(Request<ByteBuf> request) {
                RawResponse response = new RawResponse(
                        request.getId(),
                        request.getHeaders(),
                        request.getArg1(),
                        request.getArg2(),
                        Unpooled.wrappedBuffer("This is a response!".getBytes())
                );

                request.getArg3().release();
                return response;
            }
        });
        tchannel.start(this.port);
    }

}
