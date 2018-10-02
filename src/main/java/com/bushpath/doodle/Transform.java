package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import com.google.protobuf.ByteString;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class Transform extends Thread {
    protected String sketchId;
    protected BlockingQueue<ByteString> in;
    protected BlockingQueue<SketchWriteRequest> out;
    protected boolean shutdown;

    public Transform(String sketchId, BlockingQueue<ByteString> in,
            BlockingQueue<SketchWriteRequest> out) {
        this.sketchId = sketchId;
        this.in = in;
        this.out = out;
        this.shutdown = true;
    }

    @Override
    public void run() {
        this.shutdown = false;
        while (!this.shutdown) {
            ByteString byteString = null;
            try {
                byteString = this.in.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }

            if (byteString == null) {
                continue;
            }

            // process ByteString
            this.process(byteString);
        }

        // close (all record have been processed)
        this.close();
    }

    public abstract void process(ByteString byteString);
    public abstract void close();

    public void shutdown() {
        this.shutdown = true;
    }
}
