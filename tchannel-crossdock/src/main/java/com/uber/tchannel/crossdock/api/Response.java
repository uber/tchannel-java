package com.uber.tchannel.crossdock.api;

public class Response {
    private ObservedSpan span;
    private Response downstream;

    public Response(ObservedSpan span, Response downstream) {
        this.span = span;
        this.downstream = downstream;
    }

    public ObservedSpan getSpan() {
        return span;
    }

    public void setSpan(ObservedSpan span) {
        this.span = span;
    }

    public Response getDownstream() {
        return downstream;
    }

    public void setDownstream(Response downstream) {
        this.downstream = downstream;
    }

    @Override
    public String toString() {
        return "Response{" +
                "span=" + span +
                ", downstream=" + downstream +
                '}';
    }
}
