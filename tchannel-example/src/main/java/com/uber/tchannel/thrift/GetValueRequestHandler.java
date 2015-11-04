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

import java.util.Map;
import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.schemes.ThriftRequest;
import com.uber.tchannel.schemes.ThriftResponse;
import com.uber.tchannel.thrift.generated.KeyValue;
import com.uber.tchannel.thrift.generated.NotFoundError;

public class GetValueRequestHandler extends ThriftRequestHandler<KeyValue.getValue_args, KeyValue.getValue_result> {

    protected final Map<String, String> keyValueStore;

    public GetValueRequestHandler(Map<String, String> keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    @Override
    public ThriftResponse<KeyValue.getValue_result> handleImpl(ThriftRequest<KeyValue.getValue_args> request) {
        String key = request.getBody(KeyValue.getValue_args.class).getKey();

        String value = this.keyValueStore.get(key);

        NotFoundError err = null;
        if (value == null) {
            err = new NotFoundError(key);
        }

        return new ThriftResponse.Builder<KeyValue.getValue_result>(request)
            .setBody(new KeyValue.getValue_result(value, err))
            .build();
    }
}
