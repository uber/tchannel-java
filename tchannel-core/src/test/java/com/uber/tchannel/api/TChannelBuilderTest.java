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

package com.uber.tchannel.api;

import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TChannelBuilderTest {

    @Test
    public void testSetServerHost() throws Exception {

        TChannel tchannel = new TChannel.Builder("some-service")
                .setServerHost(InetAddress.getLoopbackAddress())
                .build();
        tchannel.listen();
        assertEquals("localhost", tchannel.getHost().getCanonicalHostName());
        tchannel.shutdown();

    }

    @Test
    public void testServerListeningHostValidity() throws Exception {

        // InetAddress constructed using hostname will not return IP address
        // with getHostName call
        TChannel tchannel = new TChannel.Builder("some-service")
                .setServerHost(InetAddress.getByName("localhost"))
                .build();
        tchannel.listen();

        // The regular expression used here doesn't cover all invalid cases, but it's
        // consistent with the one in javascript code
        assertTrue((tchannel.getListeningHost().matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$")));
    }

}
