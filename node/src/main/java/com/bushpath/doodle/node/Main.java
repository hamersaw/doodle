package com.bushpath.doodle.node;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;

import com.moandjiezana.toml.Toml;

import org.reflections.Reflections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlPluginManager;
import com.bushpath.doodle.node.control.ControlService;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.control.NodeService;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.plugin.PluginService;
import com.bushpath.doodle.node.sketch.CheckpointManager;
import com.bushpath.doodle.node.sketch.CheckpointMetadata;
import com.bushpath.doodle.node.sketch.CheckpointService;
import com.bushpath.doodle.node.sketch.CheckpointTransferTimerTask;
import com.bushpath.doodle.node.sketch.PipeManager;
import com.bushpath.doodle.node.sketch.PipeService;
import com.bushpath.doodle.node.sketch.SketchManager;
import com.bushpath.doodle.node.sketch.SketchService;
import com.bushpath.doodle.node.sketch.QueryService;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
    protected static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // check arguments
        if (args.length != 1) {
            System.err.println("Usage: <config-file>");
            System.exit(1);
        }

        // parse configuration file
        Toml toml = new Toml();
        try {
            toml.read(new File(args[0]));
        } catch (Exception e) {
            log.error("Failed to parse configuration file", e);
            System.exit(2);
        }

        // initialize PluginManager
        PluginManager pluginManager = new PluginManager();
        try {
            // find plugin jars
            Object[] paths =
                Files.walk(Paths.get(toml.getString("plugins.directory")))
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.endsWith("jar"))
                    .toArray();

            URL[] urls = new URL[paths.length];
            for (int i=0; i<urls.length; i++) {
                String absoluteName = ((Path) paths[i]).toUri().toString();

                log.debug("Loading JAR file '{}'", absoluteName);
                urls[i] = new URL(absoluteName);
            }

            // initialize new class loader as child
            URLClassLoader urlClassLoader =
                new URLClassLoader(urls, Main.class.getClassLoader());

            // must set ContextClassLoader of current thread
            // it is adopted by all threads created by this (ex. services)
            Thread.currentThread().setContextClassLoader(urlClassLoader);

            Reflections reflections = new Reflections();

            // register gossip plugins
            for (Class<? extends ControlPlugin> clazz :
                    reflections.getSubTypesOf(ControlPlugin.class)) {
                pluginManager.registerControlPlugin(clazz);
            }

            // reguster sketch plugins
            for (Class<? extends SketchPlugin> clazz :
                    reflections.getSubTypesOf(SketchPlugin.class)) {
                pluginManager.registerSketchPlugin(clazz);
            }
        } catch (Exception e) {
            log.error("Unknown plugin loading failure", e);
            System.exit(3);
        }

        // initialize ControlPluginManager
        ControlPluginManager controlPluginManager =
            new ControlPluginManager();

        // initialize NodeManager
        List<NodeMetadata> seedNodes = new ArrayList();
        if (toml.getTables("control.gossip.seed") != null) {
            for (Toml seedToml : toml.getTables("control.gossip.seed")) {
                seedNodes.add(
                    new NodeMetadata(
                        (short) -1,
                        seedToml.getString("ipAddress"),
                        seedToml.getLong("port").shortValue()
                    ));
            }
        }

        NodeManager nodeManager = new NodeManager(
                toml.getLong("control.nodeId").intValue(),
                seedNodes
            );

        try {
            NodeMetadata nodeMetadata = new NodeMetadata(
                    toml.getLong("control.nodeId").intValue(),
                    toml.getString("control.ipAddress"),
                    toml.getLong("control.port").shortValue()
                );

            nodeManager.add(nodeMetadata);
        } catch (Exception e) {
            log.error("Failed to initialize NodeManager", e);
            System.exit(2);
        }

        // intialize CheckpointManager
        CheckpointTransferTimerTask checkpointTransferTimerTask =
            new CheckpointTransferTimerTask();
        CheckpointManager checkpointManager = new CheckpointManager(
                nodeManager, 
                toml.getString("sketch.checkpoint.directory"),
                checkpointTransferTimerTask
            );

        // add previous checkpoints to CheckpointManager
        try {
            File checkpointConfigurationFile =
                new File(checkpointManager.getConfigurationFile());

            if (checkpointConfigurationFile.exists()) {
                // parse checkpoint toml
                Toml configurationToml = new Toml();
                configurationToml.read(checkpointConfigurationFile);

                for (Toml checkpointToml : 
                        configurationToml.getTables("checkpoint")) {
                    // create CheckpointMetadata
                    CheckpointMetadata checkpointMetadata =
                        new CheckpointMetadata(
                                checkpointToml.getLong("timestamp"),
                                checkpointToml.getString("sketchId"),
                                checkpointToml.getString("checkpointId")
                            );

                    // add replicas
                    for (Toml replicaToml :
                            checkpointToml.getTables("replica")) {


                        List<Long> secondaryNodeIds = replicaToml.getList("secondaryNodeIds");
                        checkpointMetadata.addReplica(
                            replicaToml.getLong("primaryNodeId").intValue(),
                            secondaryNodeIds.get(0).intValue(),
                            secondaryNodeIds.get(1).intValue());
                    }

                    // add to CheckpointManager
                    checkpointManager.add(checkpointMetadata, false);
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse checkpoint configuration file", e);
            System.exit(3);
        }

        // initailize PipeManager
        PipeManager pipeManager = new PipeManager(nodeManager);

        // initialize SketchManager
        SketchManager sketchManager =
            new SketchManager();

        // load most recent version of each sketch checkpoint
        try {
            Map<String, CheckpointMetadata> sketchCheckpoints
                = new HashMap();;

            // find latest checkpoint for each sketch
            for (Map.Entry<String, CheckpointMetadata> entry :
                    checkpointManager.getEntrySet()) {
                CheckpointMetadata checkpointMetadata =
                    entry.getValue();

                String sketchId = checkpointMetadata.getSketchId();
                if (!sketchCheckpoints.containsKey(sketchId) ||
                        sketchCheckpoints.get(sketchId).getTimestamp() 
                        < checkpointMetadata.getTimestamp()) {
                    sketchCheckpoints.put(sketchId, checkpointMetadata);
                }
            }

            // load sketch checkpoints
            for (CheckpointMetadata checkpoint :
                    sketchCheckpoints.values()) {
                String sketchId = checkpoint.getSketchId();
                String checkpointId = checkpoint.getCheckpointId();

                // open DataInputStream on checkpoint
                String checkpointFile = checkpointManager
                    .getCheckpointFile(checkpointId);
                FileInputStream fileIn =
                    new FileInputStream(checkpointFile);
                DataInputStream dataIn =
                    new DataInputStream(fileIn);

                // read classpath
                int classpathLength = dataIn.readInt(); 
                byte[] classpathBytes = new byte[classpathLength];
                dataIn.readFully(classpathBytes);
                String classpath = new String(classpathBytes);

                // initialize sketch
                Class<? extends SketchPlugin> clazz =
                    pluginManager.getSketchPlugin(classpath);
                Constructor constructor =
                    clazz.getConstructor(DataInputStream.class);
                SketchPlugin sketch = 
                    (SketchPlugin) constructor.newInstance(dataIn);

                sketch.replayVariableOperations();
                sketch.loadData(dataIn);

                dataIn.close();
                fileIn.close();

                // read plugins
                Set<String> controlPluginIds =
                    sketch.getControlPluginIds();
                ControlPlugin[] controlPlugins = 
                    new ControlPlugin[controlPluginIds.size()];
                int index=0;
                for (String cpId : controlPluginIds) {
                    if (controlPluginManager.contains(cpId)) {
                        controlPlugins[index++] = 
                            controlPluginManager.get(cpId);
                    } else {
                        // open DataInputStream on control plugin
                        String cpFile = checkpointManager
                            .getControlPluginFile(cpId);
                        FileInputStream cpFileIn =
                            new FileInputStream(cpFile);
                        DataInputStream cpDataIn =
                            new DataInputStream(cpFileIn);

                        // read classpath
                        int cpClasspathLength = cpDataIn.readInt(); 
                        byte[] cpClasspathBytes =
                            new byte[cpClasspathLength];
                        cpDataIn.readFully(cpClasspathBytes);
                        String cpClasspath =
                            new String(cpClasspathBytes);

                        // initialize sketch
                        Class<? extends ControlPlugin> cpClazz =
                            pluginManager.getControlPlugin(cpClasspath);
                        Constructor cpConstructor =
                            cpClazz.getConstructor(DataInputStream.class);
                        ControlPlugin controlPlugin = (ControlPlugin) 
                            cpConstructor.newInstance(cpDataIn);

                        controlPlugin.replayVariableOperations();

                        cpDataIn.close();
                        cpFileIn.close();

                        // add ControlPlugin to ControlPluginManager
                        controlPluginManager.add(cpId, controlPlugin);

                        controlPlugins[index++] = controlPlugin;
                    }
                }

                sketch.initControlPlugins(controlPlugins);

                // add new sketch
                sketchManager.add(sketchId, sketch);
            }
        } catch (Exception e) {
            log.error("Failed to load sketch checkpoints", e);
            System.exit(4);
        }
 
        // initialize Server
        Server server = new Server(
                toml.getLong("control.port").shortValue(),
                toml.getLong("control.threadCount").shortValue()
            );

        // register Services
        try {
            ControlService controlService = new ControlService(
                controlPluginManager, pluginManager);
            server.registerService(controlService);

            NodeService nodeService = new NodeService(nodeManager);
            server.registerService(nodeService);

            PluginService pluginService = new PluginService(pluginManager);
            server.registerService(pluginService);

            CheckpointService checkpointService =
                new CheckpointService(checkpointManager, 
                    controlPluginManager, pluginManager, sketchManager,
                    toml.getLong("sketch.checkpoint.transfer.bufferSizeBytes")
                        .intValue());
            server.registerService(checkpointService);

            PipeService pipeService =
                new PipeService(sketchManager, pipeManager);
            server.registerService(pipeService);

            QueryService queryService =
                new QueryService(sketchManager);
            server.registerService(queryService);

            SketchService sketchService = new SketchService(
                checkpointManager, controlPluginManager, 
                pluginManager, sketchManager);
            server.registerService(sketchService);

            GossipService gossipService = new GossipService(
                checkpointManager, controlPluginManager, pluginManager,
                nodeManager, sketchManager);
            server.registerService(gossipService);
        } catch (Exception e) {
            log.error("Unknwon Service registration failure", e);
            System.exit(5);
        }

        try {
            // start Server
            server.start();

            // start GossipTimerTask
            Timer timer = new Timer();
            GossipTimerTask gossipTimerTask =
                new GossipTimerTask(controlPluginManager, nodeManager,
                    sketchManager, checkpointManager);
            timer.scheduleAtFixedRate(gossipTimerTask, 0,
                toml.getLong("control.gossip.intervalMilliSeconds"));

            // start CheckpointTransferTimerTask
            timer.scheduleAtFixedRate(checkpointTransferTimerTask, 0,
                toml.getLong("sketch.checkpoint.transfer.intervalMilliSeconds"));

            server.join();
        } catch (InterruptedException e) {
            log.error("Unknown failure", e);
        }
    }
}
