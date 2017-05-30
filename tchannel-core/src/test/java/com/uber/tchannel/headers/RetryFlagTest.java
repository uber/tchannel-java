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

package com.uber.tchannel.headers;

import com.uber.tchannel.BaseTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class RetryFlagTest extends BaseTest {

    @Test
    public void testToRetryFlag() throws Exception {
        List<Character> unparsedFlags = new ArrayList<>();
        unparsedFlags.add('t');
        unparsedFlags.add('n');
        unparsedFlags.add('c');

        for (char c : unparsedFlags) {
            assertNotNull(RetryFlag.toRetryFlag(c));
        }

        assertNull(RetryFlag.toRetryFlag('f'));

    }

    @Test
    public void testParseFlags() throws Exception {
        Set<RetryFlag> realFlags = new HashSet<>();
        realFlags.add(RetryFlag.NoRetry);
        realFlags.add(RetryFlag.RetryOnConnectionError);
        realFlags.add(RetryFlag.RetryOnTimeout);

        Set<RetryFlag> parsedFlags = RetryFlag.parseFlags("tnc");
        assertEquals(realFlags, parsedFlags);
    }
}
