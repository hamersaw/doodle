package com.bushpath.doodle.node.sketch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class CheckpointManager {
    protected static final Logger log =
        LoggerFactory.getLogger(PipeManager.class);

    protected NodeManager nodeManager;
    protected Map<String, CheckpointMetadata> checkpoints;
    protected ReadWriteLock lock;

    public CheckpointManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.checkpoints = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void addCheckpoint(CheckpointMetadata checkpoint) {
        this.lock.writeLock().lock();
        try {
            if (this.checkpoints.containsKey(checkpoint.getCheckpointId())) {
                throw new RuntimeException("checkpoint '"
                    + checkpoint.getCheckpointId() + "' already exists");
            }

            this.checkpoints.put(checkpoint.getCheckpointId(), checkpoint);
            log.info("Added checkpoint '" + checkpoint.getCheckpointId()
                + "' for sketch '" + checkpoint.getSketchId() + "'");
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public boolean containsCheckpoint(String checkpointId) {
        this.lock.readLock().lock();
        try {
            return this.checkpoints.containsKey(checkpointId);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void create(String sketchId, String checkpointId)
            throws Exception {
        this.lock.readLock().lock();
        try {
            if (this.checkpoints.containsKey(checkpointId)) {
                throw new RuntimeException("checkpoint '"
                    + checkpointId + "' already exists");
            }
        } finally {
            this.lock.readLock().unlock();
        } 
 
        this.lock.writeLock().lock();
        try {
            // initialize checkpoint
            long timestamp = System.currentTimeMillis();
            CheckpointMetadata checkpoint =
                new CheckpointMetadata(timestamp, sketchId, checkpointId);

            // iterate over nodes initializing checkpoint replicas
            for (NodeMetadata nodeMetadata :
                    this.nodeManager.getNodeValues()) {
                NodeMetadata[] secondaryReplicas = new NodeMetadata[2];
                secondaryReplicas[0] =
                    this.nodeManager.getRandomNode(nodeMetadata.getId());
                secondaryReplicas[1] =  this.nodeManager.getRandomNode(
                    nodeMetadata.getId(), secondaryReplicas[0].getId());

                if (secondaryReplicas[0] == null || secondaryReplicas[1] == null) {
                    throw new RuntimeException("not enough nodes to satisfy checkpoint replication.");
                }

                checkpoint.addReplica(nodeMetadata, secondaryReplicas);
            }

            // add checkpoint to checkpointIds
            this.checkpoints.put(checkpointId, checkpoint);
            log.info("Created checkpoint '" + checkpointId
                + "' for sketch '" + sketchId + "'");
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Set<Map.Entry<String, CheckpointMetadata>> getCheckpointEntrySet() {
        this.lock.readLock().lock();
        try {
            return checkpoints.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public int hashCode() {
        CRC32 crc32 = new CRC32();

        this.lock.readLock().lock();
        try {
            for (String checkpointId : this.checkpoints.keySet()) {
                crc32.update(checkpointId.getBytes());
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }
}
