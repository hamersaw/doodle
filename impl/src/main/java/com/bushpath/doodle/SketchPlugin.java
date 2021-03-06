package com.bushpath.doodle;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteRequest;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class SketchPlugin extends Plugin {
    protected String inflatorClass;
    protected int replicationFactor;
    protected Map<Integer, Set<Integer>> replicas;
    protected Map<Integer, Long> flushTimestamps;
    protected Map<Integer, Long> writeTimestamps;

    public SketchPlugin(String id, int replicationFactor,
            ControlPlugin controlPlugin, String inflatorClass) {
        super(id);

        this.inflatorClass = inflatorClass;
        this.replicationFactor = replicationFactor;
        this.replicas = controlPlugin.getReplicas(replicationFactor);
        this.flushTimestamps = new HashMap();
        this.writeTimestamps = new HashMap();
    }

    public SketchPlugin(DataInputStream in,
            ControlPlugin controlPlugin) throws IOException {
        super(in);

        this.inflatorClass = in.readUTF();
        this.replicationFactor = in.readInt();
        this.replicas = controlPlugin.getReplicas(replicationFactor);
        this.flushTimestamps = new HashMap();
        this.writeTimestamps = new HashMap();

        int length = in.readInt();
        for (int i=0; i<length; i++) {
            int nodeId = in.readInt();
            long timestamp = in.readLong();

            this.flushTimestamps.put(nodeId, timestamp);
            this.writeTimestamps.put(nodeId, timestamp);
        }
    }

    public void flush(int nodeId, ObjectInputStream in,
            ObjectOutputStream out) throws Exception {
        this.flushMemoryTables(nodeId, in, out);
        this.flushTimestamps.put(nodeId,
            this.writeTimestamps.get(nodeId));
    }

    public String getInflatorClass() {
        return this.inflatorClass;
    }

    public long getFlushTimestamp(int nodeId) {
        if (!this.flushTimestamps.containsKey(nodeId)) {
            return 0;
        }

        return this.flushTimestamps.get(nodeId);
    }

    public Collection<Integer> getPrimaryReplicas(int nodeId) {
        Set<Integer> primaryNodeIds = new TreeSet();
        for (Map.Entry<Integer, Set<Integer>> replica :
                this.replicas.entrySet()) {
            if (replica.getValue().contains(nodeId)) {
                primaryNodeIds.add(replica.getKey());
            }
        }

        return primaryNodeIds;
    }

    public Map<Integer, Set<Integer>> getReplicas() {
        return this.replicas;
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

    public long getWriteTimestamp(int nodeId) {
        if (!this.writeTimestamps.containsKey(nodeId)) {
            return 0;
        }

        return this.writeTimestamps.get(nodeId);
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

    public void write(int nodeId, long timestamp, ByteString data)
            throws Exception{
        this.write(nodeId, data);

        // update writeTimestamps
        if (!this.writeTimestamps.containsKey(nodeId)
                || timestamp > this.writeTimestamps.get(nodeId)) {
            this.writeTimestamps.put(nodeId, timestamp);
        }
    }

    public void serializeSketchPlugin(DataOutputStream out)
            throws IOException {
        this.serializePlugin(out);
        out.writeUTF(this.inflatorClass);
        out.writeInt(this.replicationFactor);
        out.writeInt(this.flushTimestamps.size());
        for (Map.Entry<Integer, Long> entry :
                this.flushTimestamps.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    public abstract Collection<String> getFeatures();
    protected abstract void flushMemoryTables(int nodeId,
        ObjectInputStream in, ObjectOutputStream out) throws Exception;
    public abstract long getObservationCount(Serializable e);
    public abstract Transform getTransform(BlockingQueue<ByteString> in,
        BlockingQueue<JournalWriteRequest> out, int bufferSize);
    public abstract void serialize(DataOutputStream out)
        throws IOException;
    protected abstract void write(int nodeId, ByteString data)
        throws Exception;
    public abstract void query(int nodeId, Query query,
        ObjectInputStream in, BlockingQueue<Serializable> queue)
        throws Exception;
}
