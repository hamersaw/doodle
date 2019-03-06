package com.bushpath.doodle.dfs;

import com.moandjiezana.toml.Toml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.anamnesis.ipc.rpc.RpcServer;
import com.bushpath.anamnesis.ipc.rpc.packet_handler.IpcConnectionContextPacketHandler;

import com.bushpath.doodle.dfs.file.FileManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.ServerSocket;
import java.util.TreeMap;
import java.util.concurrent.Executors; 
import java.util.concurrent.ExecutorService;

public class Main {
    protected static final Logger log =
        LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // check arguments
        if (args.length != 6) {
            System.err.println("Usage: <ipAddress> <namenodeIpcPort> <datanodeXferPort> <nodeId> <hostsFile> <configFile>");
            System.exit(1);
        }

        String ipAddress = args[0];
        short namenodeIpcPort = Short.parseShort(args[1]);
        short datanodeXferPort = Short.parseShort(args[2]);
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

        // initialize FileManager
        FileManager fileManager = new FileManager();

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
                    id, array[0], Short.parseShort(array[1]),
                    Short.parseShort(array[4]),
                    Short.parseShort(array[5]),
                    (short) -1, (short) -1);

                nodes.put(id, nodeMetadata);
                log.debug("added node '{}' - {}:{}",
                    id, array[0], array[1], array[4], array[5]);
            }

            in.close();
            fileIn.close();
        } catch (Exception e) {
            log.error("Failed to initialize NodeManager", e);
            System.exit(3);
        }

        NodeManager nodeManager = new NodeManager(nodeId, nodes);

        // start HDFS emulation
        try {
            // initialize threadpool
            int threadCount =
                toml.getLong("filesystem.workerThreadCount").intValue();
            ExecutorService executorService =
                Executors.newFixedThreadPool(threadCount);
 
            // initialize RpcServer
            ServerSocket serverSocket =
                new ServerSocket(namenodeIpcPort);
            RpcServer rpcServer =
                new RpcServer(serverSocket, executorService);

            // register ClientNamenodeService
            ClientNamenodeService clientNamenodeService =
                new ClientNamenodeService(fileManager, nodeManager);
            rpcServer.addRpcProtocol(
                "org.apache.hadoop.hdfs.protocol.ClientProtocol",
                clientNamenodeService);

            // register DoodleDfsService
            DoodleDfsService doodleDfsService =
                new DoodleDfsService(fileManager, nodeManager);
            rpcServer.addRpcProtocol(
                "com.bushpath.doodle.protocol.DfsProtocol",
                doodleDfsService);

            // start RpcServer
			rpcServer.addPacketHandler(
				new IpcConnectionContextPacketHandler());

            rpcServer.start();

            // initialize DataTransferService
            ServerSocket xferServerSocket =
                new ServerSocket(datanodeXferPort);
            DataTransferService dataTransferService =
                new DataTransferService(xferServerSocket,
                    executorService, fileManager);

            // start DataTransferService
            dataTransferService.start();

            // wait indefinitely
            rpcServer.join();
            dataTransferService.join();
        } catch (Exception e) {
            log.error("Unknown HDFS emulation startup failure", e);
            System.exit(4);
        }
    }
}
