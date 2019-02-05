package com.bushpath.doodle.node;

import com.bushpath.anamnesis.ipc.rpc.RpcServer;
import com.bushpath.anamnesis.ipc.rpc.packet_handler.IpcConnectionContextPacketHandler;

import com.bushpath.doodle.ControlPlugin;
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
import com.bushpath.doodle.node.filesystem.ClientNamenodeService;
import com.bushpath.doodle.node.filesystem.DataTransferService;
import com.bushpath.doodle.node.filesystem.FileSystemService;
import com.bushpath.doodle.node.filesystem.FileManager;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.plugin.PluginService;
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
import java.net.ServerSocket;
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
import java.util.concurrent.Executors; 
import java.util.concurrent.ExecutorService;

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

        // initialize FileManager
        FileManager fileManager = new FileManager();

        // initialize ControlManager
        ControlManager controlManager = new ControlManager(
            toml.getString("control.directory"), pluginManager);

        // initialize NodeManager
        List<NodeMetadata> seedNodes = new ArrayList();
        if (toml.getTables("gossip.seed") != null) {
            for (Toml seedToml : toml.getTables("gossip.seed")) {
                seedNodes.add(
                    new NodeMetadata(
                        (short) -1,
                        seedToml.getString("ipAddress"),
                        seedToml.getLong("port").shortValue(),
                        (short) -1, (short) -1, (short) -1, (short) -1
                    ));
            }
        }

        NodeManager nodeManager = new NodeManager(
                toml.getLong("nodeId").intValue(),
                seedNodes
            );

        try {
            NodeMetadata nodeMetadata = new NodeMetadata(
                    toml.getLong("nodeId").intValue(),
                    toml.getString("ipAddress"),
                    toml.getLong("port").shortValue(),
                    toml.getLong("filesystem.namenode.ipcPort").shortValue(),
                    toml.getLong("filesystem.datanode.xferPort").shortValue(),
                    toml.getLong("filesystem.datanode.ipcPort").shortValue(),
                    toml.getLong("filesystem.datanode.infoPort").shortValue()
                );

            nodeManager.add(nodeMetadata);
        } catch (Exception e) {
            log.error("Failed to initialize NodeManager", e);
            System.exit(2);
        }

        // initialize PipeManager
        PipeManager pipeManager = new PipeManager(nodeManager);

        // initialize ReplicationTimerTask
        ReplicationTimerTask replicationTimerTask =
            new ReplicationTimerTask(nodeManager);

        // initialize SketchManager
        SketchManager sketchManager = new SketchManager(
            toml.getString("data.directory"), controlManager,
            nodeManager, pluginManager, replicationTimerTask);

        // initialize Journals
        OperationJournal operationJournal = null;
        WriteJournal writeJournal = null;
        try {
            operationJournal = new OperationJournal(
                toml.getString("control.journal.directory"),
                toml.getLong("control.journal.maximumFileSizeBytes")
                    .intValue(),
                controlManager, sketchManager);

            writeJournal = new WriteJournal(sketchManager);
        } catch (Exception e) {
            log.error("Failed to initialize journals", e);
            System.exit(3);
        }

        // initialize Server
        Server server = new Server(
                toml.getLong("port").shortValue(),
                toml.getLong("serverThreadCount").shortValue()
            );

        // register Services
        try {
            FileSystemService fileSystemService =
                new FileSystemService(fileManager, nodeManager, sketchManager);
            server.registerService(fileSystemService);

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
                controlManager, pluginManager, sketchManager);
            server.registerService(sketchService);
        } catch (Exception e) {
            log.error("Unknwon Service registration failure", e);
            System.exit(4);
        }

        // start HDFS emulation
        try {
            // initialize threadpool
            int threadCount =
                toml.getLong("filesystem.workerThreadCount").intValue();
            ExecutorService executorService =
                Executors.newFixedThreadPool(threadCount);
 
            // initialize RpcServer
            int namenodePort =
                toml.getLong("filesystem.namenode.ipcPort").intValue();
            ServerSocket serverSocket = new ServerSocket(namenodePort);
            RpcServer rpcServer =
                new RpcServer(serverSocket, executorService);

            // register ClientNamenodeService
            ClientNamenodeService clientNamenodeService =
                new ClientNamenodeService(fileManager, nodeManager);
            rpcServer.addRpcProtocol(
                "org.apache.hadoop.hdfs.protocol.ClientProtocol",
                clientNamenodeService);

			rpcServer.addPacketHandler(
				new IpcConnectionContextPacketHandler());

            // start RpcServer
            rpcServer.start();

            // initialize DataTransferService
            int datanodeXferPort =
                toml.getLong("filesystem.datanode.xferPort").intValue();
            ServerSocket xferServerSocket =
                new ServerSocket(datanodeXferPort);
            DataTransferService dataTransferService =
                new DataTransferService(xferServerSocket,
                    executorService, fileManager, sketchManager);

            // start DataTransferService
            dataTransferService.start();
        } catch (Exception e) {
            log.error("Unknown HDFS emulation startup failure", e);
            System.exit(5);
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

            // wait indefinitely
            server.join();
        } catch (Exception e) {
            log.error("Unknown failure", e);
        }
    }
}
