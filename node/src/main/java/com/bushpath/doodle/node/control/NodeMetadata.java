package com.bushpath.doodle.node.control;

public class NodeMetadata {
    protected int id;
    protected String ipAddress;
    protected short port;

    public NodeMetadata(int id, String ipAddress, short port) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public int getId() {
        return this.id;
    }

    public String getIpAddress() {
        return this.ipAddress;
    }

    public short getPort() {
        return this.port;
    }

    @Override
    public String toString() {
        return this.id + ":" + this.ipAddress + ":" + this.port;
    }
}
