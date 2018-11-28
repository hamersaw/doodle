package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Distributor extends Thread {
    protected static final Logger log =
        LoggerFactory.getLogger(Distributor.class);

    protected BlockingQueue<SketchWriteRequest> queue;
    protected NodeManager nodeManager;
    protected boolean shutdown;

    public Distributor(BlockingQueue<SketchWriteRequest> queue,
            NodeManager nodeManager) {
        this.queue = queue;
        this.nodeManager = nodeManager;
        this.shutdown = true;
    }

    @Override
    public void run() {
        this.shutdown = false;
        while (!this.shutdown) {
            SketchWriteRequest swRequest = null;
            try {
                swRequest = this.queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }

            if (swRequest == null) {
                continue;
            }

            // handle sketchWriteRequest
            try {
                NodeMetadata nodeMetadata =
                    this.nodeManager.get(swRequest.getNodeId());

                SketchWriteResponse swResponse = (SketchWriteResponse)
                    CommUtility.send(
                        MessageType.SKETCH_WRITE.getNumber(),
                        swRequest, nodeMetadata.getIpAddress(),
                        nodeMetadata.getPort());
            } catch (Exception e) {
                log.error("Failed to send SketchWriteRequest", e);
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
