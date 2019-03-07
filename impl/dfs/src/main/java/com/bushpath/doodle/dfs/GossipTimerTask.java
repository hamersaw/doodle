package com.bushpath.doodle.dfs;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.FileGossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileGossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;

import java.io.DataInputStream;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

public class GossipTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipTimerTask.class);

    protected FileManager fileManager;
    protected NodeManager nodeManager;
    protected OperationJournal operationJournal;

    public GossipTimerTask(FileManager fileManager,
            NodeManager nodeManager,
            OperationJournal operationJournal) {
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
        this.operationJournal = operationJournal;
    }

    @Override
    public void run() {
        try {
            // retrieve random node
            NodeMetadata nodeMetadata = this.nodeManager
                .getRandomNode(this.nodeManager.getThisNodeId());

            // return if no other nodes
            if (nodeMetadata == null) {
                return;
            }

            // create GossipRequest
            FileGossipRequest.Builder builder =
                FileGossipRequest.newBuilder().setOperationTimestamp(
                    this.operationJournal.getTimestamp());

            for (Map.Entry<Integer, DoodleInode> entry :
                    this.fileManager.getEntrySet()) {
                DoodleInode inode = entry.getValue();
                if (inode.getFileType() == FileType.REGULAR &&
                        !((DoodleFile) inode.getEntry()).isComplete()) {
                    builder.addIncompleteInodes(inode.getInodeValue());
                }
            }

            // send GossipRequest
            FileGossipRequest request = builder.build();
            FileGossipResponse response = null;
            try {
                RpcClient rpcClient = new RpcClient(
                    nodeMetadata.getIpAddress(),
                    nodeMetadata.getNamenodeIpcPort(), "dfsNode",
                    "com.bushpath.doodle.protocol.DfsProtocol");

                // read response
                DataInputStream in = rpcClient.send("gossip", request);
                response = FileGossipResponse.parseDelimitedFrom(in);

                // close DataInputStream and RpcClient
                in.close();
                rpcClient.close();
            } catch (ConnectException e) {
                log.warn("Connection to {} unsuccessful",
                    nodeMetadata.toString());
                return;
            } catch (Exception e) {
                log.error("Unknown communication error", e);
                return;
            }

            // handle response
            for (FileOperation operation :
                    response.getOperationsList()) {
                this.operationJournal.add(operation);
            }

            Map<Integer, Map<Long, Integer>> inodes = new HashMap();
            for (Map.Entry<Long, Integer> entry :
                    response.getBlocksMap().entrySet()) {
                int inode = BlockManager.getInode(entry.getKey());
                if (!inodes.containsKey(inode)) {
                    inodes.put(inode, new HashMap());
                }

                inodes.get(inode).put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<Integer, Map<Long, Integer>> entry :
                    inodes.entrySet()) {
                DoodleInode inode =
                    this.fileManager.getInode(entry.getKey());
                DoodleFile file = (DoodleFile) inode.getEntry();

                file.addBlocks(entry.getValue());
            }
        } catch (Exception e) {
            log.error("Unknown failure ", e);
        }
    }
}
