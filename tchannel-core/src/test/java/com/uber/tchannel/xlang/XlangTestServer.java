package com.uber.tchannel.xlang;

import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import io.netty.channel.ChannelFuture;

import java.net.InetAddress;


// Instructions:
//      1. run this program
//      2. run node test/streaming_bisect.js --host "127.0.0.1:8888" --first
// References:
//      1. https://github.com/uber/tchannel/blob/master/docs/cross_language_testing/running.md
//      2. https://github.com/uber/tchannel-node/blob/master/scripts/xlang_test.js


public final class XlangTestServer {

    private XlangTestServer() {}

    public static void main(String[] args) throws Exception {
        TChannel tchannel = new TChannel.Builder("test_as_raw")
            .setServerHost(InetAddress.getByName(null))
            .setServerPort(8888)
            .build();
        tchannel.makeSubChannel("test_as_raw").register("streaming_echo", new EchoHandler());

        ChannelFuture f = tchannel.listen();

        f.channel().closeFuture().sync();

        tchannel.shutdown();
    }

    public static class EchoHandler implements RequestHandler {
        @Override
        public Response handle(Request request) {
            return new RawResponse.Builder(request)
                .setArg2(request.getArg2().retain())
                .setArg3(request.getArg3().retain())
                .build();
        }
    }

}
