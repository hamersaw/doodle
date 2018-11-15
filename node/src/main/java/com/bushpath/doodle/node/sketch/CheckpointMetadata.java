package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.protobuf.DoodleProtos.Checkpoint;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckpointMetadata {
    protected long timestamp;
    protected String sketchId;
    protected String checkpointId;
    protected Map<Integer, Replica> replicas;

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
            new Replica(primaryReplica, secondaryReplicas));
    }
    
    public Checkpoint toProtobuf() {
        // initialize builder
        Checkpoint.Builder builder = Checkpoint.newBuilder()
            .setTimestamp(this.timestamp)
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId);

        // add replicas
        for (Replica replica : this.replicas.values()) {
            CheckpointEntry.Builder entryBuilder =
                CheckpointEntry.newBuilder()
                    .setPrimaryReplica(replica.primaryReplica.toProtobuf());

            for (NodeMetadata nodeMetadata : replica.secondaryReplicas) {
                entryBuilder.addSecondaryReplicas(nodeMetadata.toProtobuf());
            }

            builder.addCheckpointEntries(entryBuilder.build());
        }

        return builder.build();
    }

    protected class Replica {
        public NodeMetadata primaryReplica;
        public NodeMetadata[] secondaryReplicas;

        public Replica(NodeMetadata primaryReplica,
                NodeMetadata[] secondaryReplicas) {
            this.primaryReplica = primaryReplica;
            this.secondaryReplicas = secondaryReplicas;
        }
    }
}
