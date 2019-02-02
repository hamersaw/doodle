package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteRequest;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class Transform extends Thread {
    protected static final Logger log =
        LoggerFactory.getLogger(Transform.class);

    protected String sketchId;
    protected BlockingQueue<ByteString> in;
    protected BlockingQueue<JournalWriteRequest> out;
    protected int featureCount;
    protected int bufferSize;
    protected Map<Integer, ByteString.Output> buffers;
    protected Map<Integer, ObjectOutputStream> objectOutputStreams;
    protected boolean shutdown;

    public Transform(String sketchId, BlockingQueue<ByteString> in,
            BlockingQueue<JournalWriteRequest> out, 
            int featureCount, int bufferSize) {
        this.sketchId = sketchId;
        this.in = in;
        this.out = out;
        this.featureCount = featureCount;
        this.bufferSize = bufferSize;
        this.buffers = new HashMap();
        this.objectOutputStreams = new HashMap();
        this.shutdown = true;
    }

    protected void checkBufferSize(int nodeId) throws Exception {
        this.objectOutputStreams.get(nodeId).flush();

        if (this.buffers.get(nodeId).size() >= this.bufferSize) {
			this.flushBuffer(nodeId, true);
        }
    }

	private void flushBuffer(int nodeId,
            boolean delete) throws Exception {
		ByteString.Output byteStringOutput = this.buffers.get(nodeId);
        this.objectOutputStreams.get(nodeId).close();

		// create JournalWriteRequest   
		JournalWriteRequest journalWriteRequest =
			JournalWriteRequest.newBuilder()
				.setNodeId(nodeId)
				.setSketchId(this.sketchId)
				.setData(byteStringOutput.toByteString())
				.build();

		while (!this.out.offer(journalWriteRequest)) {}

        if (delete) {
            this.buffers.remove(nodeId);
            this.objectOutputStreams.remove(nodeId);
        }
	}

    protected ObjectOutputStream getObjectOutputStream(int nodeId)
            throws Exception {
        if (!this.buffers.containsKey(nodeId)) {
            ByteString.Output byteStringOutput = ByteString.newOutput();
            this.buffers.put(nodeId, byteStringOutput);

            ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(byteStringOutput);
            this.objectOutputStreams.put(nodeId, objectOutputStream);
        }

        return this.objectOutputStreams.get(nodeId);
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

        // close (all records have been processed)
        try {
            for (Integer nodeId : this.buffers.keySet()) {
                this.flushBuffer(nodeId, false);
            }
        } catch (Exception e) {
            log.error("Failed to close transform for sketch '{}'",
                this.sketchId, e);
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public abstract void onPipeWriteEnd() throws Exception;
    public abstract void process(float[] observation) throws Exception;
}
