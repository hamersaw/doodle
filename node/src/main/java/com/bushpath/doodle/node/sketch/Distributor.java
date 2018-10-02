package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteResponse;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Distributor extends Thread {
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
            SketchWriteRequest sketchWriteRequest = null;
            try {
                sketchWriteRequest = this.queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }

            if (sketchWriteRequest == null) {
                continue;
            }

            // handle sketchWriteRequest
            try {
                NodeMetadata nodeMetadata =
                    this.nodeManager.getNode(sketchWriteRequest.getNodeId());

                SketchWriteResponse response = (SketchWriteResponse)
                    CommUtility.send(
                        MessageType.SKETCH_WRITE.getNumber(),
                        sketchWriteRequest, nodeMetadata.getIpAddress(),
                        nodeMetadata.getPort());
            } catch (Exception e) {
                // TODO - handle exception
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
