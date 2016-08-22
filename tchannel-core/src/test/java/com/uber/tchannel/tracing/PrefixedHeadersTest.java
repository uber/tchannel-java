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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PrefixedHeadersTest {

    @Test
    public void testPrefixedHeadersReader() throws Exception {
        Map<String, String> headers = ImmutableMap.of("as", "thrift", "$abc$trace-id", "12345");
        PrefixedHeadersCarrier carrier = new PrefixedHeadersCarrier(headers, "$abc$");
        Map<String, String> extracted = new HashMap<>();
        for (Map.Entry<String, String> kv: carrier) {
            extracted.put(kv.getKey(), kv.getValue());
        }
        assertEquals(ImmutableMap.of("trace-id", "12345"), extracted);
    }

    @Test
    public void testPrefixedHeadersWriter() throws Exception {
        Map<String, String> headers = new HashMap<>(ImmutableMap.of("as", "thrift"));
        PrefixedHeadersCarrier carrier = new PrefixedHeadersCarrier(headers, "$abc$");
        carrier.put("trace-id", "12345");
        assertEquals(ImmutableMap.of("as", "thrift", "$abc$trace-id", "12345"), headers);
    }
}
