package com.bushpath.doodle.node;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteJournal {
    protected static final Logger log =
        LoggerFactory.getLogger(WriteJournal.class);

    protected Map<String, TreeMap<Long, ByteString>> journal;
    protected ReadWriteLock lock;

    public WriteJournal() {
        this.journal = new HashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void add(String sketchId,
            ByteString data) throws Exception  {
        this.lock.writeLock().lock();
        try {
            // get memoryTable
            TreeMap<Long, ByteString> memoryTable = null;
            if (this.journal.containsKey(sketchId)) {
                memoryTable = this.journal.get(sketchId);
            } else {
                memoryTable = new TreeMap();
                this.journal.put(sketchId, memoryTable);
            }

            // add buffer
            memoryTable.put(System.currentTimeMillis(), data);
            // TODO - store replica count
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void search(String sketchId, long timestamp) {
        this.lock.readLock().lock();
        try {
            // TODO
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
