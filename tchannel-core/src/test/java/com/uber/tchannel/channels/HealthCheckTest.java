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

package com.uber.tchannel.channels;

import com.uber.tchannel.BaseTest;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.messages.generated.HealthStatus;
import com.uber.tchannel.messages.generated.Meta;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HealthCheckTest extends BaseTest {
    @Test
    public void testHealthCheck() throws Exception {

        InetAddress host = InetAddress.getByName(null);

        // create server
        final TChannel server = new TChannel.Builder("server")
                .setServerHost(host)
                .build();
        final SubChannel subServer = server.makeSubChannel("server");
        subServer.registerHealthHandler();
        server.listen();

        int port = server.getListeningPort();

        // create client
        final TChannel client = new TChannel.Builder("client")
                .setServerHost(host)
                .build();
        final SubChannel subClient = client.makeSubChannel("server");
        client.listen();

        ThriftRequest<Meta.health_args> req = new ThriftRequest.Builder<Meta.health_args>("server", "Meta::health")
                .setBody(new Meta.health_args())
                .build();

        TFuture<ThriftResponse<Meta.health_result>> future = subClient.send(
                req,
                host,
                port);

        try (ThriftResponse<Meta.health_result> res = future.get()) {
            assertFalse(res.isError());
            HealthStatus status = res.getBody(Meta.health_result.class).getSuccess();
            assertTrue(status.isOk());
        }

        server.shutdown();
    }
}
