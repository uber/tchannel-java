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

import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.thrift.generated.KeyValue;
import com.uber.tchannel.thrift.generated.NotFoundError;

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

        ListenableFuture<ThriftResponse<KeyValue.setValue_result>> future = tchannel
            .makeSubChannel("keyvalue-service")
            .send(
                new ThriftRequest.Builder<KeyValue.setValue_args>("keyvalue-service", "KeyValue::setValue")
                    .setBody(setValue)
                    .build(),
                InetAddress.getLocalHost(),
                8888
            );

        ThriftResponse<KeyValue.setValue_result> response = future.get();
        if (response.isError()) {
            System.out.println("setValue failed due to: " + response.getError().getMessage());
        } else {
            System.out.println("setValue succeeded");
        }

        response.release();
    }

    public static String getValue(
            TChannel tchannel,
            String key
    ) throws NotFoundError, UnknownHostException, TimeoutException,
            ExecutionException, InterruptedException, TChannelError {

        KeyValue.getValue_args getValue = new KeyValue.getValue_args(key);

        ListenableFuture<ThriftResponse<KeyValue.getValue_result>> future = tchannel
            .makeSubChannel("keyvalue-service")
            .send(
                new ThriftRequest.Builder<KeyValue.getValue_args>("keyvalue-service", "KeyValue::getValue")
                    .setBody(getValue)
                    .build(),
                InetAddress.getLocalHost(),
                8888
            );

        ThriftResponse<KeyValue.getValue_result> getResult = future.get();
        if (getResult.isError()) {
            System.out.println("getValue failed due to: " + getResult.getError().getMessage());
        } else {
            System.out.println("getValue succeeded");
        }

        String value = getResult.getBody(KeyValue.getValue_result.class).getSuccess();
        NotFoundError err = getResult.getBody(KeyValue.getValue_result.class).getNotFound();

        if (value == null) {
            throw err;
        }

        getResult.release();

        return value;
    }

}
