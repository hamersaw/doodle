package com.bushpath.doodle.node.control;

import com.bushpath.doodle.protobuf.DoodleProtos.NodeListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class NodeService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(NodeService.class);

    protected NodeManager nodeManager;

    public NodeService(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.NODE_LIST.getNumber(),
                MessageType.NODE_SHOW.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case NODE_LIST:
                    // parse request
                    NodeListRequest nodeListRequest =
                        NodeListRequest.parseDelimitedFrom(in);

                    log.info("handling NodeListRequest");

                    // init response
                    NodeListResponse.Builder nodeListBuilder =
                        NodeListResponse.newBuilder();

                    // add nodes
                    for (NodeMetadata nodeMetadata :
                            this.nodeManager.getNodeValues()) {
                        Node node = Node.newBuilder()
                            .setId(nodeMetadata.getId())
                            .setIpAddress(nodeMetadata.getIpAddress())
                            .setPort(nodeMetadata.getPort())
                            .build();

                        nodeListBuilder.addNodes(node);
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    nodeListBuilder.build().writeDelimitedTo(out);
                    break;
                case NODE_SHOW:
                    // parse request
                    NodeShowRequest nodeShowRequest =
                        NodeShowRequest.parseDelimitedFrom(in);

                    log.info("handling NodeShowRequest '{}'",
                        nodeShowRequest.getId());

                    // init response
                    NodeShowResponse.Builder nodeShowBuilder =
                        NodeShowResponse.newBuilder();

                    NodeMetadata nodeMetadata =
                        this.nodeManager.getNode(nodeShowRequest.getId());

                    nodeShowBuilder.setNode(nodeMetadata.toProtobuf());

                    // write to out
                    out.writeInt(messageType);
                    nodeShowBuilder.build().writeDelimitedTo(out);
                    break;
                default:
                    log.warn("Unreachable");
            }
        } catch (Exception e) {
            log.warn("Handling exception", e);

            // create Failure
            Failure.Builder builder = Failure.newBuilder()
                .setType(e.getClass().getName());

            if (e.getMessage() != null) {
                builder.setText(e.getMessage());
            }

            // write to out
            out.writeInt(MessageType.FAILURE.getNumber());
            builder.build().writeDelimitedTo(out);
        }
    }
}
