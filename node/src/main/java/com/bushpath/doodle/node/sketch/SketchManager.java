package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlManager;
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
    protected PluginManager pluginManager;
    protected TreeMap<String, SketchPlugin> sketches;
    protected ReadWriteLock lock;

    public SketchManager(ControlManager controlManager,
            PluginManager pluginManager) {
        this.controlManager = controlManager;
        this.pluginManager = pluginManager;
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

    public void handleOperation(Operation operation) throws Exception {
        String pluginId = operation.getPluginId();

        switch (operation.getOperationType()) {
            case ADD:
                // get plugin
                this.checkExists(operation.getPluginId());
                SketchPlugin aPlugin = this.sketches.get(pluginId);

                // add variables
                Variable aVariable = operation.getVariable();
                for (String value : aVariable.getValuesList()) {
                    aPlugin.addVariable(aVariable.getType(),
                        aVariable.getName(), value);
                }

                log.info("'{}': added {} value(s) to variable '{}:{}'",
                    pluginId, aVariable.getValuesCount(),
                    aVariable.getType(), aVariable.getName());

                break;
            case DELETE:
                // get plugin
                this.checkExists(operation.getPluginId());
                SketchPlugin dPlugin = this.sketches.get(pluginId);

                // delete variables
                Variable dVariable = operation.getVariable();
                for (String value : dVariable.getValuesList()) {
                    dPlugin.deleteVariable(dVariable.getType(),
                        dVariable.getName(), value);
                }

                log.info("'{}': deleted {} value(s) from variable '{}:{}'",
                    pluginId, dVariable.getValuesCount(),
                    dVariable.getType(), dVariable.getName());

                break;
            case INIT:
                // check if plugin already exists
                this.checkNotExists(operation.getPluginId());

                // create SketchPlugin
                Class<? extends SketchPlugin> clazz =
                    this.pluginManager
                        .getSketchPlugin(operation.getPluginClass());
                Constructor constructor =
                    clazz.getConstructor(String.class);
                SketchPlugin sketch = (SketchPlugin) constructor
                    .newInstance(pluginId);

                // initialize ControlPlugin's
                List<String> list =
                    operation.getLinkPluginIdsList();
                ControlPlugin[] controlPlugins = 
                    new ControlPlugin[list.size()];
                for (int i=0; i<controlPlugins.length; i++) {
                    controlPlugins[i] =
                        this.controlManager.get(list.get(i));
                    controlPlugins[i].freeze();
                }

                sketch.initControlPlugins(controlPlugins);

                // add sketch
                this.lock.writeLock().lock();
                try {
                    this.sketches.put(pluginId, sketch);
                    log.info("Added plugin {}", pluginId);
                } finally {
                    this.lock.writeLock().unlock();
                }

                break;
        }
    }
}
