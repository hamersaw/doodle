package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointTransferRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointTransferResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeMetadata;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CheckpointTransferTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(CheckpointTransferTimerTask.class);

    protected Map<String, Set<NodeMetadata>> transfers;
    protected CheckpointManager checkpointManager;
    protected ReadWriteLock lock;

    public CheckpointTransferTimerTask() {
        this.transfers = new HashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void setCheckpointManager(CheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    public void addTransfer(String checkpointId,
            NodeMetadata ... nodeMetadatas) {
        this.lock.writeLock().lock();
        try {
            // see if checkpointId already exists
            Set<NodeMetadata> set = null;
            if (this.transfers.containsKey(checkpointId)) {
                set = this.transfers.get(checkpointId);
            } else {
                set = new HashSet();
                this.transfers.put(checkpointId, set);
            }

            // add transfer
            for (NodeMetadata nodeMetadata : nodeMetadatas) {
                set.add(nodeMetadata);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public void run() {
        boolean locked = this.lock.writeLock().tryLock();
        if (!locked) {
            return;
        }

        try {
            // iterate over transfers
            for (String checkpointId : this.transfers.keySet()) {
                Set<NodeMetadata> primaryNodes = 
                    this.transfers.get(checkpointId);;

                Set<NodeMetadata> successfulTransfers = new HashSet();
                for (NodeMetadata nodeMetadata : primaryNodes) {
                    try {
                        this.transferCheckpoint(checkpointId, nodeMetadata);
                        successfulTransfers.add(nodeMetadata);
                        log.debug("transfered checkpoint '{}' from node '{}'",
                            checkpointId, nodeMetadata.getId());
                    } catch (NoSuchElementException e) {
                    } catch (Exception e) {
                        log.error("Failed to transfer checkpoint '{}'"
                            + " from node '{}'", checkpointId, 
                            nodeMetadata.getId(), e);
                    }
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

    public void transferCheckpoint(String checkpointId,
            NodeMetadata nodeMetadata) throws Exception {
        // open checkpoint FileOutputStream and DataOutputStream
        String checkpointFile = this.checkpointManager
            .getCheckpointFile(checkpointId, nodeMetadata.getId());
        File file = new File(checkpointFile);
        file.getParentFile().mkdirs();
        FileOutputStream fileOut = new FileOutputStream(file);
        DataOutputStream dataOut =
            new DataOutputStream(fileOut);

        // transfer checkpoint data
        long offset = 0;
        while (true) {
            // create CheckpointTransferRequest
            CheckpointTransferRequest checkpointTransferRequest =
                CheckpointTransferRequest.newBuilder()
                    .setCheckpointId(checkpointId)
                    .setOffset(offset)
                    .build();

            // send request
            CheckpointTransferResponse checkpointTransferResponse = 
                (CheckpointTransferResponse) CommUtility.send(
                    MessageType.CHECKPOINT_TRANSFER.getNumber(),
                    checkpointTransferRequest,
                    nodeMetadata.getIpAddress(),
                    (short) nodeMetadata.getPort());
 
            // write checkpoint transfer buffer to out
            byte[] data = checkpointTransferResponse
                .getData().toByteArray();

            if (data.length == 0
                    && !checkpointTransferResponse.getLastMessage()) {
                throw new NoSuchElementException("");
            }

            dataOut.write(data);
            offset += data.length;

            // if last message -> break
            if (checkpointTransferResponse.getLastMessage()) {
                break;
            }
        }

        // close DataOutputStream and FileOutputStream
        dataOut.close();
        fileOut.close();
    }
}
