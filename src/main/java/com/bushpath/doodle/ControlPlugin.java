package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.ControlPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

public abstract class ControlPlugin extends Plugin {
    public ControlPlugin(String id) {
        super(id);
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
}
