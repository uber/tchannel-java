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
