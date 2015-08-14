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

import com.uber.tchannel.api.Res;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Serializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;

import java.util.HashMap;
import java.util.Map;

public class ResponseRouter extends SimpleChannelInboundHandler<RawResponse> {

    private final Map<Long, Value> messageMap = new HashMap<>();
    private final Serializer serializer = new Serializer(new HashMap<ArgScheme, Serializer.SerializerInterface>() {
        {
            put(ArgScheme.JSON, new JSONSerializer());
        }
    });

    public <T> void expect(long messageId, Promise<Res<T>> promise, Class<T> klass) {
        this.messageMap.put(messageId, new Value<>(klass, promise));
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, RawResponse response) throws Exception {

        Value<?> value = this.messageMap.remove(response.getId());

        Res<?> res = new Res<>(
                this.serializer.decodeEndpoint(response),
                this.serializer.decodeHeaders(response),
                this.serializer.decodeBody(response, value.getKlass())
        );

        value.promise.setSuccess((Res) res);

    }

    private class Value<T> {
        private final Class<T> klass;
        private final Promise<Res<T>> promise;

        public Value(Class<T> klass, Promise<Res<T>> promise) {
            this.klass = klass;
            this.promise = promise;
        }

        public Class<T> getKlass() {
            return klass;
        }

        public Promise<Res<T>> getPromise() {
            return promise;
        }
    }

}
