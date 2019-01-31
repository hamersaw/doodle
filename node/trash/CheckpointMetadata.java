package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.protobuf.DoodleProtos.Checkpoint;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CheckpointMetadata {
    protected long timestamp;
    protected String sketchId;
    protected String checkpointId;
    protected Map<Integer, Set<Integer>> replicas;

    public CheckpointMetadata(long timestamp, String sketchId, 
            String checkpointId) {
        this.timestamp = timestamp;
        this.sketchId = sketchId;
        this.checkpointId = checkpointId;
        this.replicas = new HashMap();
    }

    public void addReplica(int primaryNodeId,
            int ... secondaryNodeIds) {
        // check if primaryNodeId already registered
        Set<Integer> set = null;
        if (this.replicas.containsKey(primaryNodeId)) {
            set = this.replicas.get(primaryNodeId);
        } else {
            set = new HashSet();
            this.replicas.put(primaryNodeId, set);
        }

        // add secondaryNodeIds
        for (int secondaryNodeId : secondaryNodeIds) {
            set.add(secondaryNodeId);
        }
    }
    
    public long getTimestamp() {
        return this.timestamp;
    }

    public String getSketchId() {
        return this.sketchId;
    }

    public String getCheckpointId() {
        return this.checkpointId;
    }

    public Set<Map.Entry<Integer, Set<Integer>>> getReplicaEntrySet() {
        return this.replicas.entrySet();
    }

    public Checkpoint toProtobuf() {
        // initialize builder
        Checkpoint.Builder builder = Checkpoint.newBuilder()
            .setTimestamp(this.timestamp)
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId);

        // add replicas
        for (Map.Entry<Integer, Set<Integer>> entry :
                this.replicas.entrySet()) {
            Replica.Builder replicaBuilder = 
                Replica.newBuilder()
                    .setPrimaryNodeId(entry.getKey());

            for (Integer secondaryNodeId : entry.getValue()) {
                replicaBuilder.addSecondaryNodeIds(secondaryNodeId);
            }

            builder.addReplicas(replicaBuilder.build());
        }

        return builder.build();
    }
}
