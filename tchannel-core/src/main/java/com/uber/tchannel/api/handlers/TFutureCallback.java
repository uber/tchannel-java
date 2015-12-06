package com.uber.tchannel.api.handlers;

import com.uber.tchannel.messages.Response;

public interface TFutureCallback<V extends Response> {
    void onResponse(V response);
}
