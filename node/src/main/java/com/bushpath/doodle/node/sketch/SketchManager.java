package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import com.bushpath.rutils.query.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.ReplicationTimerTask;
import com.bushpath.doodle.node.control.ControlManager;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class SketchManager {
    protected static final Logger log =
        LoggerFactory.getLogger(SketchManager.class);

    protected String directory;
    protected ControlManager controlManager;
    protected NodeManager nodeManager;
    protected PluginManager pluginManager;
    protected ReplicationTimerTask replicationTimerTask;
    protected TreeMap<String, SketchPlugin> sketches;
    protected ReadWriteLock lock;

    public SketchManager(String directory,
            ControlManager controlManager, NodeManager nodeManager,
            PluginManager pluginManager,
            ReplicationTimerTask replicationTimerTask) {
        this.directory = directory;
        this.controlManager = controlManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
        this.replicationTimerTask = replicationTimerTask;
        this.sketches = new TreeMap();
        this.lock = new ReentrantReadWriteLock();

        // create persist directory if doesn't exist
        File file = new File(directory);
        if (!file.exists()) {
            file.mkdirs();
        }

        // parse persisted control plugins 
        FilenameFilter filenameFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".bin");
            }
        };

        for (String filename : file.list(filenameFilter)) {
            try {
                DataInputStream in = new DataInputStream(
                    new FileInputStream(this.directory + "/" + filename));

                // get constructor
                String className = in.readUTF();
                Class<? extends SketchPlugin> clazz =
                    this.pluginManager.getSketchPlugin(className);
                Constructor constructor = clazz.getConstructor(
                    DataInputStream.class, ControlPlugin.class);

                // retrieve ControlPlugin
                String controlPluginId = in.readUTF();
                ControlPlugin controlPlugin =
                    this.controlManager.get(controlPluginId);

                // create SketchPlugin
                SketchPlugin sketch = (SketchPlugin) constructor
                    .newInstance(in, controlPlugin);

                // add sketch
                this.sketches.put(sketch.getId(), sketch);
 
                // add replicas to ReplicationTimerTask
                for (int nodeId : sketch.getPrimaryReplicas(
                        this.nodeManager.getThisNodeId())) {
                    this.replicationTimerTask
                        .addReplica(nodeId, sketch);
                }
            } catch (Exception e) {
                log.warn("failed to read persisted plugin file {}",
                    filename, e);
            }
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

    public void flush(String id, int nodeId) throws Exception {
        this.lock.writeLock().lock();
        try {
            // open streams
            String datFilename = this.getDatFile(id, nodeId);
            File file = new File(datFilename);
            File tmpFile = new File(datFilename + ".tmp");

            FileInputStream fileIn = null;
            ObjectInputStream in = null;
            if (file.exists()) {
                fileIn = new FileInputStream(file);
                in = new ObjectInputStream(fileIn);
            }

            FileOutputStream fileOut = new FileOutputStream(tmpFile);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            // flush data
            SketchPlugin sketchPlugin = this.sketches.get(id);
            sketchPlugin.flush(nodeId, in, out);
            this.serialize(sketchPlugin);

            // close streams
            if (in != null) {
                in.close();
                fileIn.close();
            }

            out.close();
            fileOut.close();

            // clean up files
            file.delete();
            tmpFile.renameTo(file);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void freeze(String id) throws IOException {
        this.lock.writeLock().lock();
        try {
            SketchPlugin sketchPlugin = this.sketches.get(id);
            if (!sketchPlugin.frozen()) {
                // freeze and initialize sketchPlugin
                sketchPlugin.freeze();
                sketchPlugin.init();

                // serialize sketchPlugin
                this.serialize(sketchPlugin);
            }
        } finally {
            this.lock.writeLock().unlock();
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
    
    protected String getDatFile(String id, int nodeId) {
        return this.directory + "/" + id + "-" + nodeId + ".dat";
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

        SketchPlugin sketchPlugin = null;
        switch (operation.getOperationType()) {
            case ADD:
            case DELETE:
                // get plugin
                this.checkExists(operation.getPluginId());
                sketchPlugin = this.sketches.get(pluginId);

                sketchPlugin.processVariable(operation.getVariable(),
                    operation.getOperationType());
                break;
            case INIT:
                // check if plugin already exists
                this.checkNotExists(operation.getPluginId());

                // get constructor
                Class<? extends SketchPlugin> clazz =
                    this.pluginManager
                        .getSketchPlugin(operation.getPluginClass());
                Constructor constructor = clazz.getConstructor(
                    String.class, Integer.class, ControlPlugin.class);

                // retreive ControlPlugin
                String controlPluginId = operation.getControlPluginId();
                this.controlManager.checkExists(controlPluginId);
                this.controlManager.freeze(controlPluginId);
                ControlPlugin controlPlugin =
                    this.controlManager.get(controlPluginId);

                // create SketchPlugin
                sketchPlugin = (SketchPlugin) constructor.newInstance(
                    pluginId, operation.getReplicationFactor(),
                    controlPlugin);

                // add sketch
                this.lock.writeLock().lock();
                try {
                    this.sketches.put(pluginId, sketchPlugin);
                    log.info("Added plugin {}", pluginId);
                } finally {
                    this.lock.writeLock().unlock();
                }

                // add replicas to ReplicationTimerTask
                for (int nodeId : sketchPlugin.getPrimaryReplicas(
                        this.nodeManager.getThisNodeId())) {
                    this.replicationTimerTask
                        .addReplica(nodeId, sketchPlugin);
                }

                break;
        }

        // serialize plugin
        this.lock.readLock().lock();
        try {
            sketchPlugin.setLastUpdated(operation.getTimestamp());
            this.serialize(sketchPlugin);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public BlockingQueue<Serializable> query(String id,
            int nodeId, Query query) {
        this.lock.readLock().lock();
        try {
            // get SketchPlugin
            SketchPlugin sketchPlugin = this.sketches.get(id);
            File file = new File(this.getDatFile(id, nodeId));

            // start QueryHandler
            BlockingQueue<Serializable> queue =
                new ArrayBlockingQueue(2048);

            QueryHandler queryHandler = new QueryHandler(nodeId,
                sketchPlugin, query, file, queue);
            queryHandler.start();

            return queue;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    protected void serialize(SketchPlugin sketchPlugin)
            throws IOException {
        String filename = this.directory + "/"
            + sketchPlugin.getId() + ".bin";
        DataOutputStream out = new DataOutputStream(
            new FileOutputStream(filename));

        sketchPlugin.serialize(out);
        out.close();
    }
}
