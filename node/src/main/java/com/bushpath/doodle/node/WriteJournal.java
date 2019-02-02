package com.bushpath.doodle.node;

import com.bushpath.doodle.SketchPlugin;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.sketch.SketchManager;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteJournal {
    protected static final Logger log =
        LoggerFactory.getLogger(WriteJournal.class);

    protected SketchManager sketchManager;
    protected Map<String, TreeMap<Long, ByteString>> journal;
    protected ReadWriteLock lock;

    public WriteJournal(SketchManager sketchManager) {
        this.sketchManager = sketchManager;
        this.journal = new HashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void add(int nodeId, String sketchId,
            ByteString data) throws Exception  {
        this.lock.writeLock().lock();
        try {
            long timestamp = System.nanoTime();

            // write to local sketch
            SketchPlugin sketch = this.sketchManager.get(sketchId);
            sketch.write(nodeId, timestamp, data);

            // get memoryTable
            TreeMap<Long, ByteString> memoryTable = null;
            if (this.journal.containsKey(sketchId)) {
                memoryTable = this.journal.get(sketchId);
            } else {
                memoryTable = new TreeMap();
                this.journal.put(sketchId, memoryTable);
            }

            // add buffer
            memoryTable.put(timestamp, data);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Map<Long, ByteString> search(String sketchId,
            long timestamp) throws Exception {
        this.lock.readLock().lock();
        try {
            if (!this.journal.containsKey(sketchId)) {
                return new HashMap(); // wait until gossip inits sketch
            }

            return this.journal.get(sketchId).tailMap(timestamp, false);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
