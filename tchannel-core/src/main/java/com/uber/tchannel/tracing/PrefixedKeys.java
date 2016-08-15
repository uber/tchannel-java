package com.uber.tchannel.tracing;

// TODO profile and cache prefixed and unprefixed keys if necessary
public final class PrefixedKeys {
    public static String prefixedKey(String key, String prefix) {
        return prefix + key;
    }

    public static String unprefixedKey(String key, String prefix) {
        return key.substring(prefix.length());
    }
}
