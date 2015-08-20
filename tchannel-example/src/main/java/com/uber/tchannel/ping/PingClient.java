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

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PingClient {

    private final String host;
    private final int port;
    private final int requests;

    public PingClient(String host, int port, int requests) {
        this.host = host;
        this.port = port;
        this.requests = requests;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "host", true, "Server Host to connect to");
        options.addOption("p", "port", true, "Server Port to connect to");
        options.addOption("n", "requests", true, "Number of requests to make");
        options.addOption("?", "help", false, "Usage");
        HelpFormatter formatter = new HelpFormatter();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("?")) {
            formatter.printHelp("PingClient", options, true);
            return;
        }

        String host = cmd.getOptionValue("h", "localhost");
        int port = Integer.parseInt(cmd.getOptionValue("p", "8888"));
        int requests = Integer.parseInt(cmd.getOptionValue("n", "10000"));

        System.out.println(String.format("Connecting from client to server on port: %d", port));
        new PingClient(host, port, requests).run();
        System.out.println("Stopping Client...");

    }

    public void run() throws Exception {
        TChannel tchannel = new TChannel.Builder("ping-client").build();

        Map<String, String> headers = new HashMap<String, String>() {
            {
                put("some", "header");
            }
        };

        Request<Ping> request = new Request.Builder<>(new Ping("{'key': 'ping?'}"))
                .setEndpoint("ping")
                .setHeaders(headers)
                .setService("some-service")
                .build();

        for (int i = 0; i < this.requests; i++) {
            Promise<Response<Pong>> f = tchannel.callJSON(
                    InetAddress.getByName(this.host),
                    this.port,
                    request,
                    Pong.class
            );

            final int iteration = i;
            f.addListener(new GenericFutureListener<Future<? super Response<Pong>>>() {
                @Override
                public void operationComplete(Future<? super Response<Pong>> future) throws Exception {
                    Response<?> response = (Response<?>) future.get(100, TimeUnit.MILLISECONDS);
                    if (iteration % 1000 == 0) {
                        System.out.println(response);
                    }
                }

            });

        }

        tchannel.shutdown();

    }

}
