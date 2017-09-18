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

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.thrift.generated.KeyValue;
import com.uber.tchannel.thrift.generated.NotFoundError;
import com.uber.tchannel.utils.TChannelUtilities;

import java.util.concurrent.ExecutionException;

public final class KeyValueClient {

    private KeyValueClient() {}

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to KeyValue Server…");
        TChannel tchannel = new TChannel.Builder("keyvalue-client")
                .build();
        SubChannel subChannel = tchannel.makeSubChannel("keyvalue-service");

        setValue(subChannel, "foo", "bar");
        String value = getValue(subChannel, "foo");

        System.out.println(String.format("{'%s' => '%s'}", "foo", value));

        try {
            String otherValue = getValue(subChannel, "baz");
            System.out.println(String.format("{'%s' => '%s'}", "foo", otherValue));
        } catch (NotFoundError e) {
            System.out.println(String.format("Key '%s' not found.", e.getKey()));
        }

        tchannel.shutdown(false);

        System.out.println("Disconnected from KeyValue Server.");
    }

    public static void setValue(SubChannel subChannel, String key, String value) throws Exception {
        KeyValue.setValue_args setValue = new KeyValue.setValue_args(key, value);

        TFuture<ThriftResponse<KeyValue.setValue_result>> future = subChannel
            .send(
                new ThriftRequest.Builder<KeyValue.setValue_args>("keyvalue-service", "KeyValue::setValue")
                    .setTimeout(1000)
                    .setBody(setValue)
                    .build(),
                TChannelUtilities.getCurrentIp(),
                8888
            );

        try (ThriftResponse<KeyValue.setValue_result> response = future.get()) {
            if (response.isError()) {
                System.out.println("setValue failed due to: " + response.getError().getMessage());
            } else {
                System.out.println("setValue succeeded");
            }
        }
    }

    public static String getValue(
            SubChannel subChannel,
            String key
    ) throws NotFoundError, ExecutionException, InterruptedException, TChannelError {

        KeyValue.getValue_args getValue = new KeyValue.getValue_args(key);

        TFuture<ThriftResponse<KeyValue.getValue_result>> future = subChannel
            .send(
                new ThriftRequest.Builder<KeyValue.getValue_args>("keyvalue-service", "KeyValue::getValue")
                    .setTimeout(1000)
                    .setBody(getValue)
                    .build(),
                TChannelUtilities.getCurrentIp(),
                8888
            );

        try (ThriftResponse<KeyValue.getValue_result> getResult = future.get()) {
            if (getResult.isError()) {
                System.out.println("getValue failed due to: " + getResult.getError().getMessage());
                return null;
            } else {
                System.out.println("getValue succeeded");
            }

            String value = getResult.getBody(KeyValue.getValue_result.class).getSuccess();
            NotFoundError err = getResult.getBody(KeyValue.getValue_result.class).getNotFound();

            if (value == null) {
                throw err;
            }

            return value;
        }
    }

}
