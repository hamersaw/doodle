package com.bushpath.doodle;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;

public abstract class SketchPlugin extends Plugin {
    protected String inflatorClass;
    protected Set<String> controlPluginIds;

    public SketchPlugin(String id, String inflatorClass) {
        super(id);

        this.inflatorClass = inflatorClass;
        this.controlPluginIds = new TreeSet();
    }

    public SketchPlugin(DataInputStream in) throws IOException {
        super(in);
 
        // read inflatorClass
        int inflatorClassLength = in.readInt();
        byte[] inflatorClassBytes = new byte[inflatorClassLength];
        in.readFully(inflatorClassBytes);
        this.inflatorClass = new String(inflatorClassBytes);

        // read controlPluginIds
        int controlPluginIdsCount = in.readInt();
        this.controlPluginIds = new TreeSet();
        for (int i=0; i<controlPluginIdsCount; i++) {
            int controlPluginIdLength = in.readInt();
            byte[] controlPluginIdBytes =
                new byte[controlPluginIdLength];
            in.readFully(controlPluginIdBytes);
            this.controlPluginIds.add(new String(controlPluginIdBytes));
        }
    }

    public Set<String> getControlPluginIds() {
        return this.controlPluginIds;
    }

    public String getInflatorClass() {
        return this.inflatorClass;
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

    public void initControlPlugins(ControlPlugin[] controlPlugins)
            throws Exception {
        for (ControlPlugin controlPlugin : controlPlugins) {
            this.addControlPlugin(controlPlugin);
            this.controlPluginIds.add(controlPlugin.getId());
        }
    }

    public abstract Collection<String> getFeatures();
    public abstract Transform getTransform(BlockingQueue<ByteString> in,
        BlockingQueue<SketchWriteRequest> out, int bufferSize);
    public abstract long getObservationCount();
    protected abstract void 
        addControlPlugin(ControlPlugin controlPlugin) throws Exception;
    public abstract void loadData(DataInputStream in) throws IOException;
    public abstract void write(ByteString byteString) throws Exception;
    public abstract void query(Query query,
        BlockingQueue<Serializable> queue) throws Exception;
    public abstract void serialize(DataOutputStream out)
        throws IOException;

    public void serializeSketchPlugin(DataOutputStream out)
            throws IOException {
        this.serializePlugin(out);

        // write this.inflatorClass
        out.writeInt(this.inflatorClass.length());
        out.write(this.inflatorClass.getBytes());

        // write this.ControlPlugins
        out.writeInt(this.controlPluginIds.size());
        for (String controlPluginId : this.controlPluginIds) {
            out.writeInt(controlPluginId.length());
            out.write(controlPluginId.getBytes());
        }
    }

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
