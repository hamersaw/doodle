package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.node.control.NodeMetadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CheckpointTransferTimerTask extends TimerTask {
    protected Map<String, Set<NodeMetadata>> transfers;
    protected ReadWriteLock lock;

    public CheckpointTransferTimerTask() {
        this.transfers = new HashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void addTransfer(String checkpointId,
            NodeMetadata ... nodeMetadatas) {
        this.lock.writeLock().lock();
        try {
            Set<NodeMetadata> set = null;
            if (this.transfers.containsKey(checkpointId)) {
                set = this.transfers.get(checkpointId);
            } else {
                set = new HashSet();
                this.transfers.put(checkpointId, set);
            }

            for (NodeMetadata nodeMetadata : nodeMetadatas) {
                set.add(nodeMetadata);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public void run() {
        this.lock.writeLock().lock();
        try {
            // iterate over transfers
            for (String checkpointId : this.transfers.keySet()) {
                Set<NodeMetadata> primaryNodes = 
                    this.transfers.get(checkpointId);;

                Set<NodeMetadata> successfulTransfers = new HashSet();
                for (NodeMetadata nodeMetadata : primaryNodes) {
                    // TODO - transfer checkpoint from node
                    System.out.println("TODO - transfer checkpoint '"
                        + checkpointId + " from node '"
                        + nodeMetadata.getId() + "'");

                    successfulTransfers.add(nodeMetadata);
                }

                // remove successful transfers
                primaryNodes.removeAll(successfulTransfers);
                if (primaryNodes.isEmpty()) {
                    this.transfers.remove(checkpointId);
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}
