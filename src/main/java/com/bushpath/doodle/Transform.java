package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class Transform extends Thread {
    protected BlockingQueue<List<Float>> in;
    protected BlockingQueue<SketchWriteRequest> out;
    protected boolean shutdown;

    public Transform(BlockingQueue<List<Float>> in,
            BlockingQueue<SketchWriteRequest> out) {
        this.in = in;
        this.out = out;
        this.shutdown = true;
    }

    @Override
    public void run() {
        this.shutdown = false;
        while (!this.shutdown) {
            List<Float> list = null;
            try {
                list = this.in.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }

            if (list == null) {
                continue;
            }

            // TODO - handle sketchWriteRequest
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}
