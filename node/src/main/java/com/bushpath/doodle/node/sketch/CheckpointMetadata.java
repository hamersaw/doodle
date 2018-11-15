package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.protobuf.DoodleProtos.Checkpoint;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckpointMetadata {
    protected long timestamp;
    protected String sketchId;
    protected String checkpointId;
    protected Map<Integer, ReplicaMetadata> replicas;

    public CheckpointMetadata(long timestamp, String sketchId, 
            String checkpointId) {
        this.timestamp = timestamp;
        this.sketchId = sketchId;
        this.checkpointId = checkpointId;
        this.replicas = new HashMap();
    }

    public void addReplica(NodeMetadata primaryReplica,
            NodeMetadata[] secondaryReplicas) {
        this.replicas.put(primaryReplica.getId(),
            new ReplicaMetadata(primaryReplica, secondaryReplicas));
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
    
    public Checkpoint toProtobuf() {
        // initialize builder
        Checkpoint.Builder builder = Checkpoint.newBuilder()
            .setTimestamp(this.timestamp)
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId);

        // add replicas
        for (ReplicaMetadata replica : this.replicas.values()) {
            Replica.Builder replicaBuilder =
                Replica.newBuilder()
                    .setPrimaryReplica(replica.primaryReplica.toProtobuf());

            for (NodeMetadata nodeMetadata : replica.secondaryReplicas) {
                replicaBuilder.addSecondaryReplicas(nodeMetadata.toProtobuf());
            }

            builder.addReplicas(replicaBuilder.build());
        }

        return builder.build();
    }

    protected class ReplicaMetadata {
        public NodeMetadata primaryReplica;
        public NodeMetadata[] secondaryReplicas;

        public ReplicaMetadata(NodeMetadata primaryReplica,
                NodeMetadata[] secondaryReplicas) {
            this.primaryReplica = primaryReplica;
            this.secondaryReplicas = secondaryReplicas;
        }
    }
}
