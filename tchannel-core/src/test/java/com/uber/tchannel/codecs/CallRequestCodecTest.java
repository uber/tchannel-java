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

package com.uber.tchannel.codecs;

import com.uber.tchannel.Fixtures;
import com.uber.tchannel.fragmentation.DefragmentationState;
import com.uber.tchannel.framing.TFrame;
import com.uber.tchannel.messages.CallRequest;
import com.uber.tchannel.messages.CallRequestContinue;
import com.uber.tchannel.messages.MessageType;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;


public class CallRequestCodecTest {

    @Test
    public void testEncode() throws Exception {


    }

    @Test
    public void testDecode() throws Exception {

    }

    @Test
    public void testBytesRemaining() throws Exception {
    }

    @Test
    public void testAcceptInboundMessage() throws Exception {
        CallRequestCodec codec = new CallRequestCodec();
        TFrame callRequestFrame = new TFrame(0, MessageType.CallRequest, 42, Unpooled.EMPTY_BUFFER);
        assertTrue(codec.acceptInboundMessage(callRequestFrame));

        TFrame callRequestContinueFrame = new TFrame(0, MessageType.CallRequestContinue, 42, Unpooled.EMPTY_BUFFER);
        assertTrue(codec.acceptInboundMessage(callRequestContinueFrame));

        TFrame callResponseFrame = new TFrame(0, MessageType.CallResponse, 42, Unpooled.EMPTY_BUFFER);
        assertFalse(codec.acceptInboundMessage(callResponseFrame));

        TFrame callResponseContinueFrame = new TFrame(0, MessageType.CallResponseContinue, 42, Unpooled.EMPTY_BUFFER);
        assertFalse(codec.acceptInboundMessage(callResponseContinueFrame));

        TFrame errorFrame = new TFrame(0, MessageType.Error, 42, Unpooled.EMPTY_BUFFER);
        assertFalse(codec.acceptInboundMessage(errorFrame));


    }

    @Test
    public void testAcceptOutboundMessage() throws Exception {
        CallRequestCodec codec = new CallRequestCodec();
        CallRequest callRequest = Fixtures.callRequestWithId(0);
        assertTrue(codec.acceptOutboundMessage(callRequest));

        CallRequestContinue callRequestContinue = Fixtures.callRequestContinueWithId(0);
        assertTrue(codec.acceptOutboundMessage(callRequestContinue));

    }

    @Test
    public void testEncodeCallRequest() throws Exception {

    }

    @Test
    public void testWriteEmptyArg() throws Exception {
    }

    @Test
    public void testWriteArg() throws Exception {

    }

    @Test
    public void testReadArgWithAllArgs() throws Exception {
        CallRequestCodec codec = new CallRequestCodec();
        Map<Long, DefragmentationState> defragmentationState = codec.getDefragmentationState();

        long id = 42;
        assertEquals(defragmentationState.get(id), null);

        codec.readArg(id, 8, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x06,
                        // this arg has length '6'
                        0x00,
                        0x01,
                        0x02,
                        0x03,
                        0x04,
                        0x05
                }
        ));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_2);

        codec.readArg(id, 4, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x02,
                        // this arg has length '2'
                        0x00,
                        0x01,
                }
        ));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_2);

        codec.readArg(id, 2, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x00
                        // this arg has length '0'
                }
        ));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_3);

        codec.readArg(id, 4, true, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x02,
                        // this arg has length '2'
                        0x0e,
                        0x0f
                }
        ));

        assertEquals(defragmentationState.get(id), DefragmentationState.PROCESSING_ARG_3);

        codec.readArg(id, 2, false, Unpooled.wrappedBuffer(
                new byte[]{
                        0x00,
                        0x00
                        // this arg has length '0'
                }
        ));

        assertEquals(defragmentationState.get(id), null);


    }
}