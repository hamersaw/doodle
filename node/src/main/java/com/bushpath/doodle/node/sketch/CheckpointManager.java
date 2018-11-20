package com.bushpath.doodle.node.sketch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;

import java.util.HashSet;
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
    protected String directory;
    protected CheckpointTransferTimerTask checkpointTransferTimerTask;
    protected Map<String, CheckpointMetadata> checkpoints;
    protected ReadWriteLock lock;

    public CheckpointManager(NodeManager nodeManager, String directory,
            CheckpointTransferTimerTask checkpointTransferTimerTask) {
        this.nodeManager = nodeManager;
        this.directory = directory;
        this.checkpointTransferTimerTask = checkpointTransferTimerTask;
        this.checkpointTransferTimerTask.setCheckpointManager(this);
        this.checkpoints = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void addCheckpoint(CheckpointMetadata checkpoint) throws Exception {
        this.lock.writeLock().lock();
        try {
            // check if checkpoint already exists
            if (this.checkpoints.containsKey(checkpoint.getCheckpointId())) {
                throw new RuntimeException("checkpoint '"
                    + checkpoint.getCheckpointId() + "' already exists");
            }

            // add checkpoint
            this.checkpoints.put(checkpoint.getCheckpointId(), checkpoint);
            log.info("Added checkpoint '" + checkpoint.getCheckpointId()
                + "' for sketch '" + checkpoint.getSketchId() + "'");

            // add checkpoint transfers to CheckpointTransferTimerTask
            int nodeId = this.nodeManager.getThisNodeId();
            for (Map.Entry<Integer, Set<Integer>> entry :
                    checkpoint.getReplicaEntrySet()) {
                if (entry.getValue().contains(nodeId)) {
                    NodeMetadata nodeMetadata =
                        this.nodeManager.getNode(entry.getKey());
                    this.checkpointTransferTimerTask.addTransfer(
                        checkpoint.getCheckpointId(), nodeMetadata);
                }
            }
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

    public CheckpointMetadata createCheckpoint(String sketchId,
            String checkpointId) throws Exception {
        // initialize checkpoint
        long timestamp = System.currentTimeMillis();
        CheckpointMetadata checkpoint =
            new CheckpointMetadata(timestamp, sketchId, checkpointId);

        // iterate over nodes initializing checkpoint replicas
        for (NodeMetadata nodeMetadata :
                this.nodeManager.getNodeValues()) {
            int[] secondaryNodeIds = new int[2];

            // get first secondary node
            NodeMetadata secondaryNodeMetadata =
                this.nodeManager.getRandomNode(nodeMetadata.getId());
            if (secondaryNodeMetadata == null) {
                throw new RuntimeException("not enough nodes to"
                    + " satisfy checkpoint replication.");
            }
            secondaryNodeIds[0] = secondaryNodeMetadata.getId();

            // get second secondary node
            secondaryNodeMetadata = this.nodeManager
                .getRandomNode(nodeMetadata.getId(), secondaryNodeIds[0]);
            if (secondaryNodeMetadata == null) {
                throw new RuntimeException("not enough nodes to"
                    + " satisfy checkpoint replication.");
            }
            secondaryNodeIds[1] = secondaryNodeMetadata.getId();

            checkpoint.addReplica(nodeMetadata.getId(),
                secondaryNodeIds);
        }

        // return checkpoint
        return checkpoint;
    }

    public Set<Map.Entry<String, CheckpointMetadata>> getCheckpointEntrySet() {
        this.lock.readLock().lock();
        try {
            return checkpoints.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public String getCheckpointFile(String checkpointId) {
        return this.getCheckpointFile(checkpointId,
            this.nodeManager.getThisNodeId());
    }

    public String getCheckpointFile(String checkpointId, int nodeId) {
        return this.directory + "/" + nodeId + "/" + checkpointId + ".bin";
    }

    public Set<CheckpointMetadata> getSketchCheckpoints(String sketchId) {
        Set<CheckpointMetadata> checkpoints = new HashSet();

        this.lock.readLock().lock();
        try {
            // iterate over checkpoints and grab all for sketchId
            for (CheckpointMetadata checkpoint :
                    this.checkpoints.values()) {
                if (checkpoint.getSketchId().equals(sketchId)) {
                    checkpoints.add(checkpoint);
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return checkpoints;
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
