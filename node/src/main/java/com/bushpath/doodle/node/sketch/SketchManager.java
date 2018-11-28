package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class SketchManager {
    protected static final Logger log =
        LoggerFactory.getLogger(SketchManager.class);

    protected TreeMap<String, SketchPlugin> sketches;
    protected ReadWriteLock lock;

    public SketchManager() {
        this.sketches = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void add(String id,
            SketchPlugin sketch) throws Exception {
        this.lock.writeLock().lock();
        try {
            this.sketches.put(id, sketch);
            log.info("Added sketch {}", id);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void checkExists(String id) {
        this.lock.readLock().lock();
        try {
            if (!this.sketches.containsKey(id)) {
                throw new RuntimeException("Sketch '"
                    + id + "' does not exist");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void checkNotExists(String id) {
        this.lock.readLock().lock();
        try {
            if (this.sketches.containsKey(id)) {
                throw new RuntimeException("Sketch '"
                    + id + "' already exists");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public boolean contains(String id) {
        this.lock.readLock().lock();
        try {
            return this.sketches.containsKey(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public SketchPlugin get(String id) {
        this.lock.readLock().lock();
        try {
            return this.sketches.get(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Set<Map.Entry<String, SketchPlugin>> getEntrySet() {
        this.lock.readLock().lock();
        try {
            return this.sketches.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void remove(String id) {
        this.lock.writeLock().lock();
        try {
            this.sketches.remove(id);
        } finally {
            this.lock.writeLock().unlock();            
        }
    }

    @Override
    public int hashCode() {
        CRC32 crc32 = new CRC32();

        this.lock.readLock().lock();
        try {
            for (Map.Entry<String, SketchPlugin> entry :
                    this.sketches.entrySet()) {
                crc32.update(entry.getKey().getBytes());
                crc32.update(entry.getValue().hashCode());
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }
}
