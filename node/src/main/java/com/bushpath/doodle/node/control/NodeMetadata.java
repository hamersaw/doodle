package com.bushpath.doodle.node.control;

import com.bushpath.doodle.protobuf.DoodleProtos.Node;

public class NodeMetadata {
    protected int id;
    protected String ipAddress;
    protected short port;

    protected short namenodeIpcPort;
    protected short datanodeXferPort;
    protected short datanodeIpcPort;
    protected short datanodeInfoPort;


    public NodeMetadata(int id, String ipAddress, short port,
            short namenodeIpcPort, short datanodeXferPort,
            short datanodeIpcPort, short datanodeInfoPort) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.port = port;

        this.namenodeIpcPort = namenodeIpcPort;
        this.datanodeXferPort = datanodeXferPort;
        this.datanodeIpcPort = datanodeIpcPort;
        this.datanodeInfoPort = datanodeInfoPort;
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

    public short getNamenodeIpcPort() {
        return this.namenodeIpcPort;
    }

    public short getDatanodeXferPort() {
        return this.datanodeXferPort;
    }

    public short getDatanodeIpcPort() {
        return this.datanodeIpcPort;
    }

    public short getDatanodeInfoPort() {
        return this.datanodeInfoPort;
    }

    public Node toProtobuf() {
        return Node.newBuilder()
            .setId(this.id)
            .setIpAddress(this.ipAddress)
            .setPort(this.port)
            .setNamenodeIpcPort(this.namenodeIpcPort)
            .setDatanodeXferPort(this.datanodeXferPort)
            .setDatanodeIpcPort(this.datanodeIpcPort)
            .setDatanodeInfoPort(this.datanodeInfoPort)
            .build();
    }

    @Override
    public String toString() {
        return this.id + ":" + this.ipAddress + ":" + this.port;
    }
}
