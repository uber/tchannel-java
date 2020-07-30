package com.uber.tchannel.handlers;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.errors.TChannelConnectionReset;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.*;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

public class ResponseRouterTest {

    private static TChannel tchannel;
    private static SubChannel subChannel;

    @BeforeClass
    public static void setUp() throws Exception {
        tchannel = new TChannel.Builder("tchannel-name")
            .setServerHost(InetAddress.getByName(null))
            .setResetOnTimeoutLimit(2)
            .build();

        tchannel.listen();
        subChannel = new SubChannel("service", tchannel);
    }

    @Test
    public void resetConnectionOnContinuousTimeouts() throws Exception {
        OutRequest or1 = createOutputRequest(1, 100);
        OutRequest or2 = createOutputRequest(2, 100);
        PeerManager peerManager = mock(PeerManager.class);
        ResponseRouter responseRouter = getResponseRouter(peerManager);
        responseRouter.send(or1);
        responseRouter.send(or2);
        Thread.sleep(200);
        // Connection will be reset because of 2 consecutive timeouts
        verify(peerManager, times(1))
            .handleConnectionErrors(any(Channel.class), any(TChannelConnectionReset.class));
    }

    @Test
    public void doNotResetConnectionOnNonContinuousTimeouts() throws Exception {
        OutRequest or1 = createOutputRequest(1, 100);
        OutRequest or2 = createOutputRequest(2, 20000);
        OutRequest or3 = createOutputRequest(3, 100);
        PeerManager peerManager = mock(PeerManager.class);
        ResponseRouter responseRouter = getResponseRouter(peerManager);
        responseRouter.send(or1);
        responseRouter.send(or2);
        Thread.sleep(200);
        RawResponse response = mock(RawResponse.class);
        when(response.isError()).thenReturn(false);
        when(response.getId()).thenReturn((long) 2);
        responseRouter.handleResponse(response);
        responseRouter.send(or3);
        Thread.sleep(200);
        // Connection will not be reset because a success response occurred between 2 timeouts
        verify(peerManager, times(0))
            .handleConnectionErrors(any(Channel.class), any(TChannelConnectionReset.class));
    }

    private ResponseRouter getResponseRouter(PeerManager peerManager) throws Exception {
        ResponseRouter responseRouter = spy(new ResponseRouter(
                tchannel,
                peerManager,
                new HashedWheelTimer(10, TimeUnit.MILLISECONDS)));
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        responseRouter.channelActive(ctx);
        doNothing().when(responseRouter).sendRequest();
        return responseRouter;
    }

    private OutRequest createOutputRequest(int id, long timeout) {
        RawRequest request =
            new RawRequest.Builder("service", "endpoint").setTimeout(timeout).build();
        request.setId(id);
        return new OutRequest<>(subChannel, request, tchannel.getTracingContext());
    }
}
