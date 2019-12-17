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

import com.uber.tchannel.ResultCaptor;
import com.uber.tchannel.frames.InitFrame;
import com.uber.tchannel.frames.InitRequestFrame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class InitRequestFrameCodecTest {

    @Test
    public void shouldEncodeAndDecodeInitRequest() throws Exception {

        InitRequestFrame initReq = new InitRequestFrame(
                42,
                InitRequestFrame.DEFAULT_VERSION,
                new HashMap<String, String>() {{
                    put(InitFrame.HOST_PORT_KEY, "0.0.0.0:0");
                    put(InitFrame.PROCESS_NAME_KEY, "test-process");
                }}
        );

        ByteBufAllocator spy = spy(ByteBufAllocator.DEFAULT);
        ResultCaptor<ByteBuf> byteBufResultCaptor = new ResultCaptor<>();
        doAnswer(byteBufResultCaptor).when(spy).buffer(anyInt());

        TFrame encode = MessageCodec.encode(spy, initReq);
        assertEquals(1, byteBufResultCaptor.getResult().refCnt());

        TFrame tFrame = CodecTestUtil.encodeDecode(encode);
        assertEquals(0, byteBufResultCaptor.getResult().refCnt());

        InitRequestFrame newInitReq =
            (InitRequestFrame) MessageCodec.decode(
                tFrame
            );

        tFrame.release();
        assertEquals(initReq.getType(), newInitReq.getType());
        assertEquals(newInitReq.getId(), newInitReq.getId());
        assertEquals(newInitReq.getVersion(), newInitReq.getVersion());
        assertEquals(newInitReq.getHostPort(), newInitReq.getHostPort());
        assertEquals(newInitReq.getProcessName(), initReq.getProcessName());
        assertEquals(0, byteBufResultCaptor.getResult().refCnt());
    }

    @Test
    public void encodeWithError() throws Exception {

        InitRequestFrame initReq = new InitRequestFrame(
            42,
            InitRequestFrame.DEFAULT_VERSION,
            new HashMap<String, String>() {{
                put(InitFrame.HOST_PORT_KEY, null);
                put(InitFrame.PROCESS_NAME_KEY, null);
            }}
        );

        ByteBufAllocator spy = spy(ByteBufAllocator.DEFAULT);
        ResultCaptor<ByteBuf> byteBufResultCaptor = new ResultCaptor<>();
        doAnswer(byteBufResultCaptor).when(spy).buffer(anyInt());

        try {
            TFrame encode = MessageCodec.encode(spy, initReq);
            fail();
        } catch (Exception ex) {
            //expected
        }
        assertEquals(0, byteBufResultCaptor.getResult().refCnt());
    }

}
