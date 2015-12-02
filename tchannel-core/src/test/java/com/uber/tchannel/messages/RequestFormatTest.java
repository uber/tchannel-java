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

package com.uber.tchannel.messages;

import com.uber.tchannel.BaseTest;
import com.uber.tchannel.messages.generated.Example;
import io.netty.buffer.ByteBuf;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class RequestFormatTest extends BaseTest {

    @Test
    public void testThriftHeader() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ByteBuf arg2 = request.getArg2();
        assertEquals(2, arg2.readableBytes());
        assertEquals(0, arg2.getByte(0));
        assertEquals(0, arg2.getByte(1));
        request.release();
    }

    @Test
    public void testJsonHeader() throws Exception {
        JsonRequest<Example> request = new JsonRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ByteBuf arg2 = request.getArg2();
        assertEquals(2, arg2.readableBytes());

        // "{}"
        assertEquals(0x7b, arg2.getByte(0));
        assertEquals(0x7d, arg2.getByte(1));
        request.release();
    }

    @Test
    public void testRawHeader() throws Exception {
        RawRequest request = new RawRequest.Builder("keyvalue-service", "setValue")
            .build();
        ByteBuf arg2 = request.getArg2();
        assertEquals(0, arg2.readableBytes());
        request.release();
    }
}
