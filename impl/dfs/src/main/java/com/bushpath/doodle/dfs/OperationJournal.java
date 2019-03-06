package com.bushpath.doodle.dfs;

import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;

import com.bushpath.doodle.dfs.file.FileManager;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OperationJournal {
    protected FileManager fileManager;
    protected TreeMap<Long, FileOperation> operations;
    protected long timestamp;
    protected ReadWriteLock lock;

    public OperationJournal(FileManager fileManager) {
        this.fileManager = fileManager;
        this.operations = new TreeMap();
        this.timestamp = 0;
        this.lock = new ReentrantReadWriteLock();
    }

    public void add(FileOperation operation) throws Exception {
        this.lock.writeLock().lock();
        try {
            // execute operation
            this.fileManager.handleOperation(operation);

            // add to operations
            this.operations.put(operation.getTimestamp(), operation);
            this.timestamp = operation.getTimestamp();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public Map<Long, FileOperation> search(long timestamp) {
        this.lock.readLock().lock();
        try {
            return this.operations.tailMap(timestamp, false);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
