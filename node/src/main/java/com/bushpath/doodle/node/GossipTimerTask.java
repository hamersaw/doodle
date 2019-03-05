package com.bushpath.doodle.node;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.OperationJournal;

import java.net.ConnectException;
import java.util.TimerTask;

public class GossipTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipTimerTask.class);

    protected NodeManager nodeManager;
    protected OperationJournal operationJournal;
    protected Node thisNode;

    public GossipTimerTask(NodeManager nodeManager,
            OperationJournal operationJournal) throws Exception {
        this.nodeManager = nodeManager;
        this.operationJournal = operationJournal;

        this.thisNode = 
            nodeManager.get(nodeManager.getThisNodeId()).toProtobuf();
    }

    @Override
    public void run() {
        try {
            // retrieve random node
            NodeMetadata nodeMetadata = this.nodeManager
                .getRandomNode(this.nodeManager.getThisNodeId());

            // check if there are other registered nodes
            // TODO - remove
            /*if (nodeMetadata == null) {
                nodeMetadata = this.nodeManager.getRandomSeed();
            }*/

            // fallback to contacting a seed node
            if (nodeMetadata == null) {
                return;
            }

            // create GossipRequest
            GossipRequest request = GossipRequest.newBuilder()
                //.setNode(this.thisNode) TODO - remove
                //.setNodesHash(this.nodeManager.hashCode())
                .setOperationTimestamp(
                    this.operationJournal.getTimestamp())
                .build();

            // send GossipRequest
            GossipResponse response = null;
            try {
                response = (GossipResponse) CommUtility.send(
                    MessageType.GOSSIP.getNumber(), request,
                    nodeMetadata.getIpAddress(),
                    (short) nodeMetadata.getPort());
            } catch (ConnectException e) {
                log.warn("Connection to {} unsuccessful",
                    nodeMetadata.toString());
                return;
            } catch (Exception e) {
                log.error("Unknown communication error", e);
                return;
            }

            // handle response
            // TODO - remove
            /*for (Node node : response.getNodesList()) {
                // check if node exists
                if (this.nodeManager.contains(node.getId())) {
                    continue;
                }

                // add node
                this.nodeManager.add(
                    new NodeMetadata(
                        node.getId(),
                        node.getIpAddress(),
                        (short) node.getPort(),
                        (short) node.getNamenodeIpcPort(),
                        (short) node.getDatanodeXferPort(),
                        (short) node.getDatanodeIpcPort(),
                        (short) node.getDatanodeInfoPort()
                    ));
            }*/

            // handle operations
            for (Operation operation : response.getOperationsList()) {
                this.operationJournal.add(operation);
            }
        } catch (Exception e) {
            log.error("Unknown failure ", e);
        }
    }
}
