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
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import io.opentracing.propagation.TextMap;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Map;

class PrefixedHeadersCarrier implements TextMap {

    private static final int MAX_CACHE_SIZE = 100;

    private static final Function<String, String> PREFIXED_KEYS = prefixedKeys(Tracing.HEADER_KEY_PREFIX);

    private static final Function<String, String> UNPREFIXED_KEYS = unprefixedKeys(Tracing.HEADER_KEY_PREFIX);

    private final Map<String, String> headers;
    private final String prefix;
    private final Function<String, String> encoder;
    private final Function<String, String> decoder;

    PrefixedHeadersCarrier(Map<String, String> headers) {
        this(headers, Tracing.HEADER_KEY_PREFIX, PREFIXED_KEYS, UNPREFIXED_KEYS);
    }

    PrefixedHeadersCarrier(Map<String, String> headers, String prefix) {
        this(headers, prefix, prefixedKeys(prefix), unprefixedKeys(prefix));
    }

    private PrefixedHeadersCarrier(
            Map<String, String> headers,
            String prefix,
            Function<String, String> encoder,
            Function<String, String> decoder
    ) {
        this.headers = headers;
        this.prefix = prefix;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public @NotNull Iterator<Map.Entry<String, String>> iterator() {
        final Iterator<Map.Entry<String, String>> iterator = headers.entrySet().iterator();
        return new AbstractIterator<Map.Entry<String, String>>() {
            @Override
            protected Map.Entry<String, String> computeNext() {
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    if (entry.getKey().startsWith(prefix)) {
                        return Maps.immutableEntry(
                                decoder.apply(entry.getKey()),
                                entry.getValue());
                    }
                }
                return this.endOfData();
            }
        };
    }

    @Override
    public void put(String key, String value) {
        headers.put(encoder.apply(key), value);
    }

    Map<String, String> getNonTracingHeaders() {
        return Maps.filterKeys(headers, new Predicate<String>() {
            @Override
            public boolean apply(String key) {
                return !key.startsWith(prefix);
            }
        });
    }

    private static Function<String, String> prefixedKeys(final String prefix) {
        return cachingTransformer(new Function<String, String>() {
            @Override
            public String apply(String key) {
                return prefix + key;
            }
        });
    }

    private static Function<String, String> unprefixedKeys(final String prefix) {
        return cachingTransformer(new Function<String, String>() {
            @Override
            public String apply(String key) {
                return key.substring(prefix.length());
            }
        });
    }

    private static Function<String, String> cachingTransformer(
            Function<String, String> transformer
    ) {
        final LoadingCache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .build(CacheLoader.from(transformer));
        return new Function<String, String>() {
            @Override
            public String apply(String key) {
                return cache.getUnchecked(key);
            }
        };
    }
}
