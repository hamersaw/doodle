package com.bushpath.doodle.node;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

public class GossipService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipService.class);

    protected NodeManager nodeManager;
    protected OperationJournal operationJournal;
    protected WriteJournal writeJournal;

    public GossipService(NodeManager nodeManager,
            OperationJournal operationJournal,
            WriteJournal writeJournal) {
        this.nodeManager = nodeManager;
        this.operationJournal = operationJournal;
        this.writeJournal = writeJournal;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.GOSSIP.getNumber(),
                MessageType.JOURNAL_OPERATION.getNumber(),
                MessageType.JOURNAL_WRITE.getNumber(),
                MessageType.JOURNAL_WRITE_SEARCH.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case GOSSIP:
                    // parse request
                    GossipRequest gRequest =
                        GossipRequest.parseDelimitedFrom(in);

                    log.trace("handling GossipRequest");

                    // init response
                    GossipResponse.Builder gBuilder =
                        GossipResponse.newBuilder();

                    // handle node
                    Node node = gRequest.getNode();
                    if (!this.nodeManager.contains(node.getId())) {
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
                    }

                    // handle node gossip
                    if (gRequest.getNodesHash() !=
                            this.nodeManager.hashCode()) {
                        // if node hash != -> add all nodes
                        for (NodeMetadata nodeMetadata : 
                                this.nodeManager.getValues()) {
                            gBuilder.addNodes(nodeMetadata.toProtobuf());
                        }
                    }

                    // handle operation gossip
                    long gTimestamp = gRequest.getOperationTimestamp();
                    for (Map.Entry<Long, Operation> entry :
                            this.operationJournal
                                .search(gTimestamp).entrySet()) {
                        gBuilder.addOperations(entry.getValue());
                    }

                    // write to out
                    out.writeInt(messageType);
                    gBuilder.build().writeDelimitedTo(out);
                    break;
                case JOURNAL_OPERATION:
                    // parse request
                    JournalOperationRequest joRequest =
                        JournalOperationRequest.parseDelimitedFrom(in);

                    log.trace("handling JournalOperationRequest {}");

                    // init response
                    JournalOperationResponse.Builder joBuilder =
                        JournalOperationResponse.newBuilder();

                    // handle
                    for (Operation operation :
                            joRequest.getOperationsList()) {
                        this.operationJournal.add(operation);
                    }

                    // write to out
                    out.writeInt(messageType);
                    joBuilder.build().writeDelimitedTo(out);
                    break;
                case JOURNAL_WRITE:
                    // parse request
                    JournalWriteRequest jwRequest =
                        JournalWriteRequest.parseDelimitedFrom(in);

                    String jwSketchId = jwRequest.getSketchId();
                    log.trace("handling JournalWriteRequest {}",
                        jwSketchId);

                    // init response
                    JournalWriteResponse.Builder jwBuilder =
                        JournalWriteResponse.newBuilder();

                    // handle
                    this.writeJournal.add(jwSketchId,
                        jwRequest.getData());

                    // TODO - write to sketch

                    // write to out
                    out.writeInt(messageType);
                    jwBuilder.build().writeDelimitedTo(out);
                    break;
                case JOURNAL_WRITE_SEARCH:
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
