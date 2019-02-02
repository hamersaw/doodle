package com.bushpath.doodle;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteRequest;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class SketchPlugin extends Plugin {
    protected String inflatorClass;
    protected Map<Integer, Long> writeTimestamps;

    public SketchPlugin(String id, String inflatorClass) {
        super(id);

        this.inflatorClass = inflatorClass;
        this.writeTimestamps = new HashMap();
    }

    public String getInflatorClass() {
        return this.inflatorClass;
    }

    public long getObservationCount(int nodeId, Query query)
            throws Exception {
        // perform query
        long observationCount = 0;
        BlockingQueue<Serializable> queue = this.query(nodeId, query);

        // iterate over results
        Serializable s = null;
        while (true) {
            // retrieve next Serializable from queue
            try {
                s = queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("failed to poll queue", e);
            }

            // check if s is valid
            if (s instanceof Exception) {
                throw (Exception) s;
            } else if (s instanceof Poison) {
                break;
            } else if (s == null) {
                continue;
            }

            // increment record count
            observationCount += this.getObservationCount(s);
        }

        return observationCount;
    }

    public long getWriteTimestamp(int nodeId) {
        if (!this.writeTimestamps.containsKey(nodeId)) {
            return 0;
        }

        return this.writeTimestamps.get(nodeId);
    }

    public int[] indexFeatures(List<String> list) throws Exception {
        // retrieve this plugins features
        Collection<String> features = this.getFeatures();

        // compute indices of listed features
        int[] indices = new int[features.size()];
        int i=0;
        for (String feature : features) {
            indices[i] = -1;
            for (int j=0; j<list.size(); j++) {
                if (feature.equals(list.get(j))) {
                    indices[i] = j;
                    break;
                }
            }

            if (indices[i] == -1) {
                throw new RuntimeException("Feature '" + feature
                    + "' not found for sketch '" + this.id + "'");
            }

            i++;
        }

        return indices;
    }

    public BlockingQueue<Serializable> query(int nodeId, Query query) {
        BlockingQueue<Serializable> queue =
            new ArrayBlockingQueue(2048);

        // start QueryHandler
        QueryHandler queryHandler =
            new QueryHandler(nodeId, this, query, queue);
        queryHandler.start();

        return queue;
    }

    public void write(int nodeId, long timestamp, ByteString data)
            throws Exception{
        this.write(nodeId, data);

        // update writeTimestamps
        if (!this.writeTimestamps.containsKey(nodeId)
                || timestamp > this.writeTimestamps.get(nodeId)) {
            this.writeTimestamps.put(nodeId, timestamp);
        }
    }

    public abstract Collection<String> getFeatures();
    public abstract long getObservationCount(Serializable e);
    public abstract Collection<Integer> getPrimaryReplicas(int nodeId);
    public abstract Transform getTransform(BlockingQueue<ByteString> in,
        BlockingQueue<JournalWriteRequest> out, int bufferSize);
    protected abstract void write(int nodeId, ByteString data)
        throws Exception;
    protected abstract void query(int nodeId,
        Query query, BlockingQueue<Serializable> queue);
}
