package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Distributor extends Thread {
    protected static final Logger log =
        LoggerFactory.getLogger(Distributor.class);

    protected BlockingQueue<JournalWriteRequest> queue;
    protected NodeManager nodeManager;
    protected boolean shutdown;

    public Distributor(BlockingQueue<JournalWriteRequest> queue,
            NodeManager nodeManager) {
        this.queue = queue;
        this.nodeManager = nodeManager;
        this.shutdown = true;
    }

    @Override
    public void run() {
        this.shutdown = false;

        JournalWriteRequest jwRequest = null;
        while (!this.shutdown) {
            try {
                jwRequest = this.queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }

            if (jwRequest == null) {
                continue;
            }

            // handle sketchWriteRequest
            try {
                NodeMetadata nodeMetadata =
                    this.nodeManager.get(jwRequest.getNodeId());

                JournalWriteResponse jwResponse = (JournalWriteResponse)
                    CommUtility.send(
                        MessageType.JOURNAL_WRITE.getNumber(),
                        jwRequest, nodeMetadata.getIpAddress(),
                        nodeMetadata.getPort());
            } catch (Exception e) {
                log.error("Failed to send JournalWriteRequest", e);
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
