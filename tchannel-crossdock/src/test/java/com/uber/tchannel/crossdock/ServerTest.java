package com.uber.tchannel.crossdock;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.uber.tchannel.crossdock.Server.SERVER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ServerTest {
    private Server server;
    private TChannel tchannel;

    @Before
    public void setUp() throws Exception {
        server = new Server("127.0.0.1", null);
        server.start();
        tchannel = server.tchannel;
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void TestEcho() throws Exception {
        SubChannel subChannel = tchannel.makeSubChannel(SERVER_NAME);

        JsonRequest<String> request = new JsonRequest
                .Builder<String>(SERVER_NAME, "echo")
                .setTimeout(2, TimeUnit.SECONDS)
                .setBody("Hello, miserable world")
                .build();

        TFuture<JsonResponse<String>> responsePromise = subChannel.send(
                request,
                tchannel.getHost(),
                tchannel.getListeningPort()
        );

        JsonResponse<String> response = responsePromise.get();
        assertNull(response.getError());
        String resp = response.getBody(String.class);
        response.release();
        assertEquals("Hello, miserable world", resp);
    }
}
