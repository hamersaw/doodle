package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.ControlPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class ControlPlugin extends Plugin {
    public ControlPlugin(String id) {
        super(id);
    }

    public ControlPlugin(DataInputStream in) throws Exception {
        super(in);
    }

    public ControlPluginGossip toGossip() {
        ControlPluginGossip.Builder builder = ControlPluginGossip.newBuilder()
            .setId(this.id)
            .setClasspath(this.getClass().getName());

        for (VariableOperation operation : this.operations.values()) {
            builder.addOperations(operation);
        }

        return builder.build();
    }

    public void serializeControlPlugin(DataOutputStream out)
            throws IOException {
        this.serializePlugin(out);
    }
}
