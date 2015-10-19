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
package com.uber.tchannel.hyperbahn;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.hyperbahn.api.HyperbahnClient;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;
import com.uber.tchannel.ping.PingRequestHandler;

// Instructions:
//      1. run Hyperbahn: node server.js --port 21300 2>&1 | jq .
//      2. run HyperbahnExample.java
//      3. tcurl -p 127.0.0.1:21300 javaServer ping -j -2 "{}" -3 '{"request":"hello"}' | jq .

public class HyperbahnExample {
    public static void main(String[] args) throws Exception {
        TChannel tchannel = new TChannel.Builder("javaServer")
                .register("ping", new PingRequestHandler())
                .setServerHost(InetAddress.getByName("127.0.0.1"))
                .setServerPort(8888)
                .build();

        tchannel.listen();

        List<InetSocketAddress> routers = new ArrayList<InetSocketAddress>() {
            {
                add(new InetSocketAddress("127.0.0.1", 21300));
            }
        };

        HyperbahnClient hyperbahn = new HyperbahnClient.Builder(tchannel.getServiceName(), tchannel)
                .setRouters(routers)
                .build();

        final ListenableFuture<Response<AdvertiseResponse>> responseFuture;

        try {
            responseFuture = hyperbahn.advertise();
        } catch (TChannelError ex) {
            System.out.println("Advertise failure: " + ex.toString());
            tchannel.shutdown();
            hyperbahn.shutdown();
            return;
        }

        responseFuture.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    Response<AdvertiseResponse> response = responseFuture.get();
                    System.out.println("Got response. All set: " + response.getBody().toString());
                } catch (Exception ex) {
                    System.out.println("Error happened: " + ex.getMessage());
                }
            }
        }, new Executor() {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        });

        Thread.sleep(TimeUnit.MILLISECONDS.convert(600, TimeUnit.SECONDS));

        tchannel.shutdown();
        hyperbahn.shutdown();
    }
}
