package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.ReplicationTimerTask;
import com.bushpath.doodle.node.control.ControlManager;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.lang.reflect.Constructor;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class SketchManager {
    protected static final Logger log =
        LoggerFactory.getLogger(SketchManager.class);

    protected ControlManager controlManager;
    protected NodeManager nodeManager;
    protected PluginManager pluginManager;
    protected ReplicationTimerTask replicationTimerTask;
    protected TreeMap<String, SketchPlugin> sketches;
    protected ReadWriteLock lock;

    public SketchManager(ControlManager controlManager,
            NodeManager nodeManager, PluginManager pluginManager,
            ReplicationTimerTask replicationTimerTask) {
        this.controlManager = controlManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
        this.replicationTimerTask = replicationTimerTask;
        this.sketches = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
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

    public void handleOperation(Operation operation) throws Exception {
        String pluginId = operation.getPluginId();

        switch (operation.getOperationType()) {
            case ADD:
            case DELETE:
                // get plugin
                this.checkExists(operation.getPluginId());
                SketchPlugin adPlugin = this.sketches.get(pluginId);

                adPlugin.processVariable(operation.getVariable(),
                    operation.getOperationType());
                break;
            case INIT:
                // check if plugin already exists
                this.checkNotExists(operation.getPluginId());

                // get constructor
                Class<? extends SketchPlugin> clazz =
                    this.pluginManager
                        .getSketchPlugin(operation.getPluginClass());
                Constructor constructor =
                    clazz.getConstructor(String.class, ControlPlugin.class);

                // retreive ControlPlugin
                String controlPluginId = operation.getControlPluginId();
                this.controlManager.checkExists(controlPluginId);
                ControlPlugin controlPlugin =
                    this.controlManager.get(controlPluginId);
                controlPlugin.freeze();

                // create SketchPlugin
                SketchPlugin sketch = (SketchPlugin) constructor
                    .newInstance(pluginId, controlPlugin);

                // add sketch
                this.lock.writeLock().lock();
                try {
                    this.sketches.put(pluginId, sketch);
                    log.info("Added plugin {}", pluginId);
                } finally {
                    this.lock.writeLock().unlock();
                }

                // add replicas to ReplicationTimerTask
                for (int nodeId :
                        sketch.getPrimaryReplicas(this.nodeManager.getThisNodeId())) {
                    this.replicationTimerTask
                        .addReplica(nodeId, sketch);
                }

                break;
        }
    }
}
