package com.uber.tchannel.tracing;

import java.util.Map;

/**
 * Current implementation of EncodedRequest performs eager serialization
 * of application headers into arg2 field, making it difficult to append
 * tracing headers at the lower-level functions. This interface provides
 * a back door to update the headers, which is not as efficient.
 *
 * TODO this won't be needed if EncodedRequest performed lazy serialization.
 */
public interface TraceableRequest {
    Map<String, String> getHeaders();
    void updateHeaders(Map<String, String> headers);
}
