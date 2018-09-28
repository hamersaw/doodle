package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Distributor extends Thread {
    protected BlockingQueue<SketchWriteRequest> queue;
    protected boolean shutdown;

    public Distributor(BlockingQueue<SketchWriteRequest> queue) {
        this.queue = queue;
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

            // TODO - handle sketchWriteRequest
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
