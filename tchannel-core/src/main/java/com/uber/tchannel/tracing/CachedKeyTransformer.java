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

package com.uber.tchannel.tracing;

import com.google.common.base.Function;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class CachedKeyTransformer implements Function<String, String> {
    private static final int MAX_CACHE_SIZE = 100;

    private final int cacheSize;

    private final Map<String, String> cache = new ConcurrentHashMap<>();

    private final Function<String, String> transformer;

    CachedKeyTransformer(Function<String, String> transformer) {
        this(transformer, MAX_CACHE_SIZE);
    }

    CachedKeyTransformer(Function<String, String> transformer, int cacheSize) {
        this.transformer = transformer;
        this.cacheSize = cacheSize;
    }

    @Override
    public String apply(String key) {
        String out = cache.get(key);
        if (out != null) {
            return out;
        }
        out = transformer.apply(key);
        // not extremely accurate due to race conditions, but good enough as upper bound
        if (cache.size() < cacheSize) {
            cache.put(key, out);
        }
        return out;
    }
}
