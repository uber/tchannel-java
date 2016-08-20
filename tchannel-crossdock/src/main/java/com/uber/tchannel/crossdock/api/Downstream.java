package com.uber.tchannel.crossdock.api;

public class Downstream {
    private String serviceName;
    private String serverRole;
    private String encoding;
    private String hostPort;
    private Downstream downstream;

    public Downstream(String serviceName, String serverRole, String encoding, String hostPort, Downstream downstream) {
        this.serviceName = serviceName;
        this.serverRole = serverRole;
        this.encoding = encoding;
        this.hostPort = hostPort;
        this.downstream = downstream;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServerRole() {
        return serverRole;
    }

    public void setServerRole(String serverRole) {
        this.serverRole = serverRole;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public Downstream getDownstream() {
        return downstream;
    }

    public void setDownstream(Downstream downstream) {
        this.downstream = downstream;
    }

    @Override
    public String toString() {
        return "Downstream{" +
                "serviceName='" + serviceName + '\'' +
                ", serverRole='" + serverRole + '\'' +
                ", encoding='" + encoding + '\'' +
                ", hostPort='" + hostPort + '\'' +
                ", downstream=" + downstream +
                '}';
    }
}
