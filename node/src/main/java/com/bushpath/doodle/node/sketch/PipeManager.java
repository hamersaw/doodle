package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.Transform;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PipeManager {
    protected static final Logger log =
        LoggerFactory.getLogger(PipeManager.class);

    protected Map<Integer, BlockingQueue<List<Float>>> queues;
    protected Map<Integer, List<Transform>> transforms;
    protected Map<Integer, List<Distributor>> distributors;
    protected ReadWriteLock lock;
    protected Random random;

    public PipeManager() {
        this.queues = new HashMap();
        this.transforms = new HashMap();
        this.distributors = new HashMap();
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random(System.currentTimeMillis());
    }

    public void closePipe(int id) throws Exception {
        this.lock.writeLock().lock();
        try {
            // TODO
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public int openPipe(SketchPlugin sketch, short transformThreadCount,
            short distributorThreadCount) throws Exception {
        this.lock.writeLock().lock();
        try {
            // TODO
            return -1;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public BlockingQueue<float[]> writePipe(int id, List<Float> values) {
        this.lock.readLock().lock();
        try {
            // TODO
            return null;
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
