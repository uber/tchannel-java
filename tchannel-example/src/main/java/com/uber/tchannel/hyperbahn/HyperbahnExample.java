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

import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.hyperbahn.api.HyperbahnClient;
import com.uber.tchannel.hyperbahn.messages.AdvertiseResponse;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class HyperbahnExample {
    public static void main(String[] args) throws UnknownHostException, InterruptedException, ExecutionException,
            TimeoutException {
        TChannel tchannel = new TChannel.Builder("hyperbahn-example").build();
        HyperbahnClient hyperbahn = new HyperbahnClient(tchannel);
        Response<AdvertiseResponse> response = hyperbahn.advertise(tchannel.getServiceName(), 0);
        System.err.println(response);

    }
}
