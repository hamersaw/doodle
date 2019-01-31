package com.bushpath.doodle.node;

import com.bushpath.doodle.protobuf.DoodleProtos.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlManager;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OperationJournal {
    protected static final Logger log =
        LoggerFactory.getLogger(OperationJournal.class);

    protected ControlManager controlManager;
    protected SketchManager sketchManager;
    protected TreeMap<Long, Operation> journal;
    protected ReadWriteLock lock;

    public OperationJournal(ControlManager controlManager,
            SketchManager sketchManager) {
        this.controlManager = controlManager;
        this.sketchManager = sketchManager;
        this.journal = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void add(Operation operation) throws Exception {
        this.lock.writeLock().lock();
        try {
            log.debug("adding operation {}:{}:{}",
                operation.getTimestamp(), operation.getPluginId(),
                operation.getOperationType());

            // execute operation
            switch (operation.getPluginType()) {
                case CONTROL:
                    this.controlManager.handleOperation(operation);
                    break;
                case SKETCH:
                    this.sketchManager.handleOperation(operation);
                    break;
            }

            // add operation to journal
            this.journal.put(operation.getTimestamp(), operation);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Map<Long, Operation> search(long timestamp) {
        this.lock.readLock().lock();
        try {
            return this.journal.tailMap(timestamp, false);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public long getTimestamp() {
        this.lock.readLock().lock();
        try {
            if (this.journal.isEmpty()) {
                return 0l;
            }

            return this.journal.lastKey();
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
