package com.bushpath.doodle;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public abstract class SketchPlugin extends Plugin {
    List<String> controlPluginIds;

    public SketchPlugin(String id, ControlPlugin[] controlPlugins) {
        super(id);

        this.controlPluginIds = new ArrayList();
        for (ControlPlugin controlPlugin : controlPlugins) {
            this.controlPluginIds.add(controlPlugin.getId());
        }
    }

    public int[] indexFeatures(List<String> list) throws Exception {
        // retrieve this plugins features
        Collection<String> features = this.getFeatures();

        // compute indices of listed features
        int[] indices = new int[features.size()];
        int i=0;
        for (String feature : features) {
            indices[i] = -1;
            for (int j=0; j<list.size(); j++) {
                if (feature.equals(list.get(j))) {
                    indices[i] = j;
                    break;
                }
            }

            if (indices[i] == -1) {
                throw new RuntimeException("Feature '" + feature
                    + "' not found for sketch '" + this.id + "'");
            }

            i++;
        }

        return indices;
    }

    public abstract Collection<String> getFeatures();
    public abstract Transform getTransform(BlockingQueue<ByteString> in,
        BlockingQueue<SketchWriteRequest> out, int bufferSize);
    public abstract void write(ByteString byteString) throws Exception;

    public SketchPluginGossip toGossip() {
        SketchPluginGossip.Builder builder = SketchPluginGossip.newBuilder()
            .setId(this.id)
            .setClasspath(this.getClass().getName())
            .addAllControlPlugins(this.controlPluginIds);

        for (VariableOperation operation : this.operations.values()) {
            builder.addOperations(operation);
        }

        return builder.build();
    }
}
