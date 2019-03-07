package com.bushpath.doodle.dfs;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.FileGossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileGossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.net.ConnectException;
import java.util.TimerTask;

public class GossipTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipTimerTask.class);

    protected NodeManager nodeManager;
    protected OperationJournal operationJournal;

    public GossipTimerTask(NodeManager nodeManager,
            OperationJournal operationJournal) {
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
            FileGossipRequest request = FileGossipRequest.newBuilder()
                .setOperationTimestamp(
                    this.operationJournal.getTimestamp())
                .build();

            // send GossipRequest
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
        } catch (Exception e) {
            log.error("Unknown failure ", e);
        }
    }
}
