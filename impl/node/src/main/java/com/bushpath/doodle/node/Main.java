package com.bushpath.doodle.node;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.Server;
import com.bushpath.doodle.SketchPlugin;

import com.moandjiezana.toml.Toml;

import org.reflections.Reflections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlManager;
import com.bushpath.doodle.node.control.ControlService;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.control.NodeService;
import com.bushpath.doodle.node.control.OperationJournal;
import com.bushpath.doodle.node.control.PipeManager;
import com.bushpath.doodle.node.control.PipeService;
import com.bushpath.doodle.node.data.ReplicationTimerTask;
import com.bushpath.doodle.node.data.SketchManager;
import com.bushpath.doodle.node.data.SketchService;
import com.bushpath.doodle.node.data.QueryService;
import com.bushpath.doodle.node.data.WriteJournal;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.plugin.PluginService;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
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
import java.util.TreeMap;
import java.util.Timer;
import java.util.TimerTask;

public class Main {
    protected static final Logger log =
        LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // check arguments
        if (args.length != 6) {
            System.err.println("Usage: <ipAddress> <port> <persistDirectory> <nodeId> <hostsFile> <configFile>");
            System.exit(1);
        }

        String ipAddress = args[0];
        short port = Short.parseShort(args[1]);
        String persistDirectory = args[2]; 
        int nodeId = Integer.parseInt(args[3]);
        String hostsPath = args[4];

        // parse configuration file
        Toml toml = new Toml();
        try {
            toml.read(new File(args[5]));
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

        // initialize ControlManager
        ControlManager controlManager = new ControlManager(
            persistDirectory + "/" 
                + toml.getString("control.directory"), pluginManager);

        // initialize NodeManager
        TreeMap<Integer, NodeMetadata> nodes = new TreeMap();
        try {
            // parse nodes from hosts file
            FileReader fileIn = new FileReader(hostsPath);
            BufferedReader in = new BufferedReader(fileIn);

            String line = null;
            while ((line = in.readLine()) != null) {
                String[] array = line.split(" ");

                int id = Integer.parseInt(array[3]);
                NodeMetadata nodeMetadata = new NodeMetadata(
                    id, array[0], Short.parseShort(array[1]));

                nodes.put(id, nodeMetadata);
                log.debug("added node '{}' - {}:{}",
                    id, array[0], array[1]);
            }

            in.close();
            fileIn.close();
        } catch (Exception e) {
            log.error("Failed to initialize NodeManager", e);
            System.exit(4);
        }

        NodeManager nodeManager = new NodeManager(nodeId, nodes);

        // initialize PipeManager
        PipeManager pipeManager = new PipeManager(nodeManager);

        // initialize ReplicationTimerTask
        ReplicationTimerTask replicationTimerTask =
            new ReplicationTimerTask(nodeManager);

        // initialize SketchManager
        SketchManager sketchManager = new SketchManager(
            persistDirectory + "/" + toml.getString("data.directory"),
            controlManager, nodeManager,
            pluginManager, replicationTimerTask);

        // initialize Journals
        OperationJournal operationJournal = null;
        WriteJournal writeJournal = null;
        try {
            operationJournal = new OperationJournal(
                persistDirectory + "/" +
                    toml.getString("control.journal.directory"),
                toml.getLong("control.journal.maximumFileSizeBytes")
                    .intValue(),
                controlManager, sketchManager);

            writeJournal = new WriteJournal(
                persistDirectory + "/" +
                    toml.getString("data.journal.directory"),
                toml.getLong("data.journal.maximumFileSizeBytes")
                    .intValue(),
                sketchManager);
        } catch (Exception e) {
            log.error("Failed to initialize journals", e);
            System.exit(3);
        }

        // initialize Server
        Server server = new Server(port,
            toml.getLong("serverThreadCount").shortValue());

        // register Services
        try {
            ControlService controlService = new ControlService(
                controlManager, pluginManager);
            server.registerService(controlService);

            GossipService gossipService = new GossipService(
                nodeManager, operationJournal, writeJournal);
            server.registerService(gossipService);

            NodeService nodeService = new NodeService(nodeManager);
            server.registerService(nodeService);

            PluginService pluginService = new PluginService(pluginManager);
            server.registerService(pluginService);

            PipeService pipeService =
                new PipeService(sketchManager, pipeManager);
            server.registerService(pipeService);

            QueryService queryService = new QueryService(nodeManager,
                pluginManager, sketchManager);
            server.registerService(queryService);

            SketchService sketchService = new SketchService(
                controlManager, nodeManager,
                pluginManager, sketchManager);
            server.registerService(sketchService);
        } catch (Exception e) {
            log.error("Unknwon Service registration failure", e);
            System.exit(4);
        }

        try {
            // start Server
            server.start();

            // start TimerTasks
            Timer timer = new Timer();

            GossipTimerTask gossipTimerTask =
                new GossipTimerTask(nodeManager, operationJournal);
            timer.scheduleAtFixedRate(gossipTimerTask, 0,
                toml.getLong("gossip.intervalMilliSeconds"));

            timer.scheduleAtFixedRate(replicationTimerTask, 0,
                toml.getLong("data.replication.intervalMilliSeconds"));

            MemoryManagementTimerTask memoryManagementTimerTask =
                new MemoryManagementTimerTask(
                    nodeManager, sketchManager,
                    toml.getLong("memoryManagement.writeDiffMilliSeconds"));
            timer.scheduleAtFixedRate(memoryManagementTimerTask, 0,
                toml.getLong("memoryManagement.intervalMilliSeconds"));

            // wait indefinitely
            server.join();
        } catch (Exception e) {
            log.error("Unknown failure", e);
        }
    }
}
