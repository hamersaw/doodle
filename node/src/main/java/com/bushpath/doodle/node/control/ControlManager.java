package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.plugin.PluginManager;

import java.lang.reflect.Constructor;
import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class ControlManager {
    protected static final Logger log =
        LoggerFactory.getLogger(ControlManager.class);

    protected PluginManager pluginManager;
    protected TreeMap<String, ControlPlugin> plugins;
    protected ReadWriteLock lock;

    public ControlManager(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
        this.plugins = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void checkExists(String id) {
        this.lock.readLock().lock();
        try {
            if (!this.plugins.containsKey(id)) {
                throw new RuntimeException("ControlPlugin '"
                    + id + "' does not exist");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void checkNotExists(String id) {
        this.lock.readLock().lock();
        try {
            if (this.plugins.containsKey(id)) {
                throw new RuntimeException("ControlPlugin '"
                    + id + "' already exists");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public boolean contains(String id) {
        this.lock.readLock().lock();
        try {
            return this.plugins.containsKey(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public ControlPlugin get(String id) {
        this.lock.readLock().lock();
        try {
            return this.plugins.get(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Set<Map.Entry<String, ControlPlugin>> getEntrySet() {
        this.lock.readLock().lock();
        try {
            return this.plugins.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void handleOperation(Operation operation) throws Exception {
        String pluginId = operation.getPluginId();

        switch (operation.getOperationType()) {
            case ADD:
                // get plugin
                this.checkExists(operation.getPluginId());
                ControlPlugin aPlugin = this.plugins.get(pluginId);

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
                ControlPlugin dPlugin = this.plugins.get(pluginId);

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

                // init control plugin
                Class<? extends ControlPlugin> clazz = 
                    this.pluginManager
                        .getControlPlugin(operation.getPluginClass());
                Constructor constructor = 
                    clazz.getConstructor(String.class);
                ControlPlugin iPlugin = (ControlPlugin) 
                    constructor.newInstance(pluginId);

                // add control plugin
                this.lock.writeLock().lock();
                try {
                    this.plugins.put(pluginId, iPlugin);
                    log.info("Added plugin {}", pluginId);
                } finally {
                    this.lock.writeLock().unlock();
                }

                break;
        }
    }
}
