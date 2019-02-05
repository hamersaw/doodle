package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.plugin.PluginManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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

    protected String directory;
    protected PluginManager pluginManager;
    protected TreeMap<String, ControlPlugin> plugins;
    protected ReadWriteLock lock;

    public ControlManager(String directory,
            PluginManager pluginManager) {
        this.directory = directory;
        this.pluginManager = pluginManager;
        this.plugins = new TreeMap();
        this.lock = new ReentrantReadWriteLock();

        // create persist directory if doesn't exist
        File file = new File(directory);
        if (!file.exists()) {
            file.mkdirs();
        }

        // parse persisted control plugins 
        for (String filename : file.list()) {
            try {
                DataInputStream in = new DataInputStream(
                    new FileInputStream(this.directory + "/" + filename));

                // init control plugin
                String className = in.readUTF();
                Class<? extends ControlPlugin> clazz = 
                    this.pluginManager.getControlPlugin(className);
                Constructor constructor = 
                    clazz.getConstructor(DataInputStream.class);
                ControlPlugin controlPlugin = (ControlPlugin) 
                    constructor.newInstance(in);

                in.close();

                // add to plugins
                this.plugins.put(controlPlugin.getId(), controlPlugin);
            } catch (Exception e) {
                log.warn("failed to read persisted plugin file {}",
                    filename, e);
            }
        }
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

    public void freeze(String id) throws IOException {
        this.lock.writeLock().lock();
        try {
            ControlPlugin controlPlugin = this.plugins.get(id);
            if (!controlPlugin.frozen()) {
                // freeze and initialize controlPlugin
                controlPlugin.freeze();
                controlPlugin.init();

                // serialize controlPlugin
                this.serialize(controlPlugin);
            }
        } finally {
            this.lock.writeLock().unlock();
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

        ControlPlugin controlPlugin = null;
        switch (operation.getOperationType()) {
            case ADD:
            case DELETE:
                // get plugin
                this.checkExists(operation.getPluginId());
                controlPlugin = this.plugins.get(pluginId);

                controlPlugin.processVariable(operation.getVariable(),
                    operation.getOperationType());
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
                controlPlugin = (ControlPlugin) 
                    constructor.newInstance(pluginId);

                // add control plugin
                this.lock.writeLock().lock();
                try {
                    this.plugins.put(pluginId, controlPlugin);
                    log.info("Added plugin {}", pluginId);
                } finally {
                    this.lock.writeLock().unlock();
                }

                break;
        }

        // serialize plugin
        this.lock.readLock().lock();
        try {
            controlPlugin.setLastUpdated(operation.getTimestamp());
            this.serialize(controlPlugin);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    protected void serialize(ControlPlugin controlPlugin)
            throws IOException {
        String filename = this.directory + "/"
            + controlPlugin.getId();
        DataOutputStream out = new DataOutputStream(
            new FileOutputStream(filename));

        controlPlugin.serialize(out);
        out.close();
    }
}
