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

package com.uber.tchannel.basic;

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.RawRequestHandler;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;

import java.net.InetAddress;

public final class SyncRequest {

    private SyncRequest() {}

    public static void main(String[] args) throws Exception {
        TChannel server = createServer();
        TChannel client = createClient();

        SubChannel subChannel = client.makeSubChannel("server");

        final long start = System.currentTimeMillis();

        // send three requests
        for (int i = 0; i < 3; i++) {
            RawRequest request = new RawRequest.Builder("server", "pong")
                .setHeader("Marco")
                .setBody("Ping!")
                .build();
            TFuture<RawResponse> future = subChannel.send(request,
                InetAddress.getByName(null),
                8888
            );

            // Use the try-with-resources Statement to release resources when done
            try (RawResponse response = future.get()) {
                if (!response.isError()) {
                    System.out.println(String.format("Response received: response code: %s, header: %s, body: %s",
                        response.getResponseCode(),
                        response.getHeader(),
                        response.getBody()));
                } else {
                    System.out.println(String.format("Got error response: %s",
                        response.toString()));
                }
            }

            System.out.println();
        }

        System.out.println(String.format("\nTime cost: %dms", System.currentTimeMillis() - start));

        // close channels asynchronously
        server.shutdown(false);
        client.shutdown(false);
    }

    protected static TChannel createServer() throws Exception {

        // create TChannel
        TChannel tchannel = new TChannel.Builder("server")
            .setServerHost(InetAddress.getByName(null))
            .setServerPort(8888)
            .build();

        // create sub channel to register the service and endpoint handler
        tchannel.makeSubChannel("server")
            .register("pong", new RawRequestHandler() {
                private int count = 0;

                @Override
                public RawResponse handleImpl(RawRequest request) {
                    System.out.println(String.format("Request received: header: %s, body: %s",
                        request.getHeader(),
                        request.getBody()));

                    count++;
                    switch (count) {
                        case 1:
                            return new RawResponse.Builder(request)
                                .setTransportHeaders(request.getTransportHeaders())
                                .setHeader("Polo")
                                .setBody("Pong!")
                                .build();
                        case 2:
                            return new RawResponse.Builder(request)
                                .setTransportHeaders(request.getTransportHeaders())
                                .setResponseCode(ResponseCode.Error)
                                .setHeader("Polo")
                                .setBody("I feel bad ...")
                                .build();
                        default:
                            throw new UnsupportedOperationException("I feel very bad!");
                    }
                }
            });

        tchannel.listen();

        return tchannel;
    }

    protected static TChannel createClient() throws Exception {

        // create TChannel
        TChannel tchannel = new TChannel.Builder("client")
            .build();

        // create sub channel to talk to server
        tchannel.makeSubChannel("server");
        return tchannel;
    }
}
