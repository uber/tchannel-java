package com.uber.tchannel.crossdock.api;

public class Request {
    private String serverRole;
    private Downstream downstream;

    public Request(String serverRole, Downstream downstream) {
        this.serverRole = serverRole;
        this.downstream = downstream;
    }

    public String getServerRole() {
        return serverRole;
    }

    public void setServerRole(String serverRole) {
        this.serverRole = serverRole;
    }

    public Downstream getDownstream() {
        return downstream;
    }

    public void setDownstream(Downstream downstream) {
        this.downstream = downstream;
    }
}
