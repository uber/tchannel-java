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

package com.uber.tchannel.thrift;

import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.thrift.generated.KeyValue;
import com.uber.tchannel.thrift.generated.NotFoundError;
import io.netty.util.concurrent.Promise;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KeyValueClient {

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to KeyValue Server…");
        TChannel tchannel = new TChannel.Builder("keyvalue-client")
                .build();

        setValue(tchannel, "foo", "bar");
        String value = getValue(tchannel, "foo");

        System.out.println(String.format("{'%s' => '%s'}", "foo", value));

        try {
            String otherValue = getValue(tchannel, "baz");
            System.out.println(String.format("{'%s' => '%s'}", "foo", otherValue));
        } catch (NotFoundError e) {
            System.out.println(String.format("Key '%s' not found.", e.getKey()));
        }

        tchannel.shutdown();

        System.out.println("Disconnected from KeyValue Server.");

    }

    public static void setValue(TChannel tchannel, String key, String value) throws Exception {
        KeyValue.setValue_args setValue = new KeyValue.setValue_args(key, value);

        Promise<Response<KeyValue.setValue_result>> setPromise = tchannel.callThrift(
                InetAddress.getLocalHost(),
                8888,
                new Request.Builder<>(setValue)
                        .setService("keyvalue-service")
                        .setEndpoint("KeyValue::setValue")
                        .build(),
                KeyValue.setValue_result.class
        );

        Response<KeyValue.setValue_result> _ = setPromise.get(100, TimeUnit.MILLISECONDS);
    }

    public static String getValue(
            TChannel tchannel,
            String key
    ) throws NotFoundError, UnknownHostException, TimeoutException, ExecutionException, InterruptedException {

        KeyValue.getValue_args getValue = new KeyValue.getValue_args(key);

        Promise<Response<KeyValue.getValue_result>> getPromise = tchannel.callThrift(
                InetAddress.getLocalHost(),
                8888,
                new Request.Builder<>(getValue)
                        .setService("keyvalue-service")
                        .setEndpoint("KeyValue::getValue")
                        .build(),
                KeyValue.getValue_result.class
        );

        Response<KeyValue.getValue_result> getResult = getPromise.get(100, TimeUnit.MILLISECONDS);

        String value = getResult.getBody().getSuccess();
        NotFoundError err = getResult.getBody().getNotFound();

        if (value == null) {
            throw err;
        }

        return value;
    }

}
