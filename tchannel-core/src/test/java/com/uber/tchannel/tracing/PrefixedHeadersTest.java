package com.uber.tchannel.tracing;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PrefixedHeadersTest {

    @Test
    public void testPrefixedHeadersReader() throws Exception {
        Map<String, String> headers = ImmutableMap.of("as", "thrift", "$abc$trace-id", "12345");
        PrefixedHeadersCarrier carrier = new PrefixedHeadersCarrier("$abc$", headers);
        Map<String, String> extracted = new HashMap<>();
        for (Map.Entry<String, String> kv: carrier) {
            extracted.put(kv.getKey(), kv.getValue());
        }
        assertEquals(ImmutableMap.of("trace-id", "12345"), extracted);
    }

    @Test
    public void testPrefixedHeadersWriter() throws Exception {
        Map<String, String> headers = new HashMap<>(ImmutableMap.of("as", "thrift"));
        PrefixedHeadersCarrier carrier = new PrefixedHeadersCarrier("$abc$", headers);
        carrier.put("trace-id", "12345");
        assertEquals(ImmutableMap.of("as", "thrift", "$abc$trace-id", "12345"), headers);
    }
}
