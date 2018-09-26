package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class ControlPluginManager {
    protected static final Logger log =
        LoggerFactory.getLogger(ControlPluginManager.class);

    protected TreeMap<String, ControlPlugin> plugins;
    protected ReadWriteLock lock;

    public ControlPluginManager() {
        this.plugins = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void addPlugin(String id,
            ControlPlugin plugin) throws Exception {
        // check if plugin already exists
        this.lock.readLock().lock();
        try {
            if (this.plugins.containsKey(id)) {
                throw new RuntimeException("plugin '" + id + "' already exists");
            }
        } finally {
            this.lock.readLock().unlock();
        }

        // add plugin
        this.lock.writeLock().lock();
        try {
            this.plugins.put(id, plugin);
            log.info("Added plugin {}", id);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public boolean containsPlugin(String id) {
        this.lock.readLock().lock();
        try {
            return this.plugins.containsKey(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public ControlPlugin getPlugin(String id) {
        this.lock.readLock().lock();
        try {
            if (!this.plugins.containsKey(id)) {
                throw new RuntimeException("plugin '" + id + "' does not exist");
            }

            return this.plugins.get(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Set<Map.Entry<String, ControlPlugin>> getPluginEntrySet() {
        this.lock.readLock().lock();
        try {
            return this.plugins.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public int getPluginsHash() {
        CRC32 crc32 = new CRC32();

        this.lock.readLock().lock();
        try {
            // update crc32 with toString() of each node
            for (String plugin : this.plugins.keySet()) {
                crc32.update(plugin.getBytes());
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }
}
