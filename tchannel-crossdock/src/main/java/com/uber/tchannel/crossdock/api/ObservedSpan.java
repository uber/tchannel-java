package com.uber.tchannel.crossdock.api;

public class ObservedSpan {
    private String traceId;
    private boolean sampled;
    private String baggage;

    public ObservedSpan(String traceId, boolean sampled, String baggage) {
        this.traceId = traceId;
        this.sampled = sampled;
        this.baggage = baggage;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public boolean isSampled() {
        return sampled;
    }

    public void setSampled(boolean sampled) {
        this.sampled = sampled;
    }

    public String getBaggage() {
        return baggage;
    }

    public void setBaggage(String baggage) {
        this.baggage = baggage;
    }

    @Override
    public String toString() {
        return "ObservedSpan{" +
                "traceId='" + traceId + '\'' +
                ", sampled=" + sampled +
                ", baggage='" + baggage + '\'' +
                '}';
    }
}
