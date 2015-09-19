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
package com.uber.tchannel.handlers;

import com.uber.tchannel.errors.BadRequestError;
import com.uber.tchannel.errors.BusyError;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.schemes.RawRequest;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class TestRequestRouter {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testBadRequestErrorOnMissingArgScheme() {
        thrown.expect(BadRequestError.class);

        EmbeddedChannel channel = new EmbeddedChannel(
                new RequestRouter(null, Executors.newFixedThreadPool(1), 0)
        );

        RawRequest rawRequest= new RawRequest(1, 1, null, null, null, null, null);
        channel.writeInbound(rawRequest);

    }

    @Test
    public void testBadRequestErrorOnInvalidArgScheme() {
        thrown.expect(BadRequestError.class);

        EmbeddedChannel channel = new EmbeddedChannel(
                new RequestRouter(null, Executors.newFixedThreadPool(1), 0)
        );

        Map<String, String> transportHeaders = new HashMap<String, String>();
        transportHeaders.put(TransportHeaders.ARG_SCHEME_KEY, "foobar");

        RawRequest rawRequest= new RawRequest(1, 1, null, transportHeaders, null, null, null);
        channel.writeInbound(rawRequest);
    }

    @Test
    public void testRateLimiting() {
        thrown.expect(BusyError.class);

        EmbeddedChannel channel = new EmbeddedChannel(
                new RequestRouter(null, Executors.newFixedThreadPool(1), 0)
        );

        Map<String, String> transportHeaders = new HashMap<String, String>();
        transportHeaders.put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.HTTP.getScheme());

        RawRequest rawRequest= new RawRequest(1, 1, null, transportHeaders, null, null, null);
        channel.writeInbound(rawRequest);
    }
}
