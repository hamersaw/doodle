package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.Transform;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;

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

    protected NodeManager nodeManager;
    protected Map<Integer, BlockingQueue<ByteString>> queues;
    protected Map<Integer, Transform[]> transforms;
    protected Map<Integer, Distributor[]> distributors;
    protected ReadWriteLock lock;
    protected Random random;

    public PipeManager(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.queues = new HashMap();
        this.transforms = new HashMap();
        this.distributors = new HashMap();
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random(System.currentTimeMillis());
    }

    public void close(int id) throws Exception {
        this.lock.writeLock().lock();
        try {
            // check if pipe exists
            if (!this.queues.containsKey(id)) {
                throw new RuntimeException("Pipe '" + id + "' does not exist");
            }

            // remove queue
            this.queues.remove(id);
            
            // shutdown transforms 
            Transform[] transformArray = this.transforms.get(id);
            for (int i=0; i<transformArray.length; i++) {
                transformArray[i].shutdown();
                transformArray[i].join();
            }

            this.transforms.remove(id);

            // shutdown distributors
            Distributor[] distributorArray = this.distributors.get(id);
            for (int i=0; i<distributorArray.length; i++) {
                distributorArray[i].shutdown();
                distributorArray[i].join();
            }

            this.distributors.remove(id);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public int open(SketchPlugin sketch, int transformThreadCount,
            int distributorThreadCount, int bufferSize) throws Exception {
        this.lock.writeLock().lock();
        try {
            // generate pipe id
            int id;
            do {
                id = random.nextInt();
            } while (this.queues.containsKey(id));
 
            // create queues
            BlockingQueue<ByteString> in = new ArrayBlockingQueue(1024);
            BlockingQueue<SketchWriteRequest> out = new ArrayBlockingQueue(1024);

            // create transforms
            Transform[] transformArray = new Transform[transformThreadCount];
            for (int i=0; i<transformArray.length; i++) {
                transformArray[i] = sketch.getTransform(in, out, bufferSize);
                transformArray[i].start();
            }

            // create distributors
            Distributor[] distributorArray =
                new Distributor[distributorThreadCount];
            for (int i=0; i<distributorArray.length; i++) {
                distributorArray[i] = new Distributor(out, this.nodeManager);
                distributorArray[i].start();
            }

            // put pipe objects into maps
            this.queues.put(id, in);
            this.transforms.put(id, transformArray);
            this.distributors.put(id, distributorArray);

            return id;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void write(int id, ByteString byteString) {
        this.lock.readLock().lock();
        try {
            // check if pipe exists
            if (!this.queues.containsKey(id)) {
                throw new RuntimeException("Pipe '" + id + "' does not exist");
            }

            // add ByteString input queue
            BlockingQueue<ByteString> queue = this.queues.get(id);
            while (!queue.offer(byteString)) {}
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
