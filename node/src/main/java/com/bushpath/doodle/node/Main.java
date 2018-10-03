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
import com.bushpath.doodle.node.sketch.SketchPluginManager;
import com.bushpath.doodle.node.sketch.SketchService;
import com.bushpath.doodle.node.sketch.PipeManager;
import com.bushpath.doodle.node.sketch.PipeService;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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

                log.info("Loading JAR file '{}'", absoluteName);
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

            nodeManager.addNode(nodeMetadata);
        } catch (Exception e) {
            log.error("Failed to initialize NodeManager", e);
            System.exit(2);
        }

        // initailize PipeManager
        PipeManager pipeManager = new PipeManager(nodeManager);

        // initialize SketchPluginManager
        SketchPluginManager sketchPluginManager =
            new SketchPluginManager();
 
        // initialize Server
        Server server = new Server(
                toml.getLong("control.port").shortValue(),
                toml.getLong("control.threadCount").shortValue()
            );

        // register Services
        try {
            ControlService controlService = new ControlService(controlPluginManager,
                nodeManager, pluginManager, sketchPluginManager);
            server.registerService(controlService);

            NodeService nodeService = new NodeService(nodeManager);
            server.registerService(nodeService);

            PluginService pluginService = new PluginService(pluginManager);
            server.registerService(pluginService);

            PipeService pipeService =
                new PipeService(sketchPluginManager, pipeManager);
            server.registerService(pipeService);

            SketchService sketchService = new SketchService(
                controlPluginManager, pluginManager, sketchPluginManager);
            server.registerService(sketchService);
        } catch (Exception e) {
            log.error("Unknwon Service registration failure", e);
            System.exit(4);
        }

        try {
            // start Server
            server.start();

            // start GossipTimerTask
            Timer timer = new Timer();
            GossipTimerTask gossipTimerTask =
                new GossipTimerTask(controlPluginManager, nodeManager,
                    pluginManager, sketchPluginManager);
            timer.scheduleAtFixedRate(gossipTimerTask, 0,
                toml.getLong("control.gossip.intervalMilliSeconds"));

            server.join();
        } catch (InterruptedException e) {
            log.error("Unknown failure", e);
        }
    }
}
