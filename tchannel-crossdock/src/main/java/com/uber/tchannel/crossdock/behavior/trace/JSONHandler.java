package com.uber.tchannel.crossdock.behavior.trace;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import com.uber.tchannel.crossdock.api.Downstream;
import com.uber.tchannel.crossdock.api.Request;
import com.uber.tchannel.crossdock.api.Response;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

class JSONHandler extends JSONRequestHandler<Request,Response> {
    static final String ENDPOINT = "trace";

    private static final Logger logger = LoggerFactory.getLogger(JSONHandler.class);

    private final TraceBehavior behavior;

    public JSONHandler(TraceBehavior behavior) {
        this.behavior = behavior;
    }

    @Override
    public JsonResponse<Response> handleImpl(JsonRequest<Request> request) {
        Response response = behavior.handleRequest(request.getBody(Request.class));
        return new JsonResponse.Builder<Response>(request)
                .setTransportHeaders(request.getTransportHeaders())
                .setBody(response)
                .build();
    }

    public Response callDownstream(Downstream downstream, InetAddress host, int port) throws Exception {
        Request request = new Request(downstream.getServerRole(), downstream.getDownstream());
        JsonRequest<Request> jsonRequest = new JsonRequest
                .Builder<Request>(downstream.getServiceName(), ENDPOINT)
                .setTimeout(5, TimeUnit.SECONDS)
                .setBody(request)
                .build();

        logger.info("Sending JSON request {}", jsonRequest);

        SubChannel subChannel = behavior.tchannel.makeSubChannel(downstream.getServiceName());
        TFuture<JsonResponse<Response>> responsePromise = subChannel.send(
                jsonRequest,
                host,
                port
        );

        JsonResponse<Response> jsonResponse = responsePromise.get();
        try {
            if (jsonResponse.isError()) {
                throw new Exception("Error response: " + jsonResponse.getError());
            }
            Response response = jsonResponse.getBody(Response.class);
            return response;
        } finally {
            jsonResponse.release();
        }
    }
}
