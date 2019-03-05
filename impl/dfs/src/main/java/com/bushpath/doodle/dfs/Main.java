package com.bushpath.doodle.dfs;

import com.bushpath.doodle.Server;

import com.moandjiezana.toml.Toml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.anamnesis.ipc.rpc.RpcServer;
import com.bushpath.anamnesis.ipc.rpc.packet_handler.IpcConnectionContextPacketHandler;

import java.io.File;

public class Main {
    protected static final Logger log =
        LoggerFactory.getLogger(Main.class);

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

        // initialize FileManager
        FileManager fileManager = new FileManager();

        // initialize Server
        Server server = new Server(
                toml.getLong("port").shortValue(),
                toml.getLong("serverThreadCount").shortValue()
            );

        // register Services
        try {
            FileSystemService fileSystemService =
                new FileSystemService(fileManager);
            server.registerService(fileSystemService);
        } catch (Exception e) {
            log.error("Unknwon Service registration failure", e);
            System.exit(1);
        }

        /*// start HDFS emulation
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
        }*/

        try {
            // start Server
            server.start();

            /*// start TimerTasks
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
                toml.getLong("memoryManagement.intervalMilliSeconds"));*/

            // wait indefinitely
            server.join();
        } catch (Exception e) {
            log.error("Unknown failure", e);
        }
    }
}
