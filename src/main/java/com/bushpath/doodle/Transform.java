package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class Transform extends Thread {
    protected static final Logger log =
        LoggerFactory.getLogger(Transform.class);

    protected String sketchId;
    protected BlockingQueue<ByteString> in;
    protected BlockingQueue<SketchWriteRequest> out;
    protected int featureCount;
    protected int bufferSize;
    protected boolean shutdown;

    public Transform(String sketchId, BlockingQueue<ByteString> in,
            BlockingQueue<SketchWriteRequest> out, 
            int featureCount, int bufferSize) {
        this.sketchId = sketchId;
        this.in = in;
        this.out = out;
        this.featureCount = featureCount;
        this.bufferSize = bufferSize;
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

            // compute record count in ByteString
            int recordCount = (byteString.size() / 4) / this.featureCount;

            DataInputStream byteIn = new DataInputStream(byteString.newInput());
            for (int i=0; i<recordCount; i++) {
                // read observation
                float[] observation = new float[this.featureCount];
                try {
                    for (int j=0; j<observation.length; j++) {
                        observation[j] = byteIn.readFloat();
                    }
                } catch (IOException e) {
                    log.error("Failed to read observation for sketch '{}'",
                        this.sketchId, e);
                    return;
                }

                // process observation
                try {
                    this.process(observation);
                } catch (Exception e) {
                    log.error("Failed to process observation for sketch '{}'",
                        this.sketchId, e);
                }
            }

            try {
                this.onPipeWriteEnd();
            } catch (Exception e) {
                log.error("Failed to 'onPipeWriteEnd' for sketch '{}'",
                    this.sketchId, e);
                e.printStackTrace();
            }
        }

        // close (all record have been processed)
        try {
            this.close();
        } catch (Exception e) {
            log.error("Failed to close transform for sketch '{}'",
                this.sketchId, e);
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public abstract void close() throws Exception;
    public abstract void onPipeWriteEnd() throws Exception;
    public abstract void process(float[] observation) throws Exception;
}
