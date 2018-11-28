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
                    NodeListRequest nlRequest =
                        NodeListRequest.parseDelimitedFrom(in);

                    log.trace("handling NodeListRequest");

                    // init response
                    NodeListResponse.Builder nlBuilder =
                        NodeListResponse.newBuilder();

                    // add nodes
                    for (NodeMetadata nodeMetadata :
                            this.nodeManager.getValues()) {
                        nlBuilder
                            .addNodes(nodeMetadata.toProtobuf());
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    nlBuilder.build().writeDelimitedTo(out);
                    break;
                case NODE_SHOW:
                    // parse request
                    NodeShowRequest nsRequest =
                        NodeShowRequest.parseDelimitedFrom(in);

                    int nsId = nsRequest.getId();
                    log.trace("handling NodeShowRequest '{}'", nsId);
                    
                    // check if node exists
                    this.nodeManager.checkExists(nsId);

                    // init response
                    NodeShowResponse.Builder nsBuilder =
                        NodeShowResponse.newBuilder();

                    NodeMetadata nodeMetadata =
                        this.nodeManager.get(nsId);

                    nsBuilder.setNode(nodeMetadata.toProtobuf());

                    // write to out
                    out.writeInt(messageType);
                    nsBuilder.build().writeDelimitedTo(out);
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
