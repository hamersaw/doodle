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
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteSearchRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteSearchResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.WriteUpdate;

import com.google.protobuf.ByteString;

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

                    int jwNodeId = jwRequest.getNodeId();
                    String jwSketchId = jwRequest.getSketchId();
                    log.trace("handling journalWriteRequest {}:{}",
                        jwNodeId, jwSketchId);

                    // init response
                    JournalWriteResponse.Builder jwBuilder =
                        JournalWriteResponse.newBuilder();

                    // handle
                    this.writeJournal.add(jwNodeId, jwSketchId,
                        jwRequest.getData());

                    // write to out
                    out.writeInt(messageType);
                    jwBuilder.build().writeDelimitedTo(out);
                    break;
                case JOURNAL_WRITE_SEARCH:
                    // parse request
                    JournalWriteSearchRequest jwsRequest =
                        JournalWriteSearchRequest.parseDelimitedFrom(in);

                    log.trace("handling JournalWriteSearchRequest");

                    // init response
                    JournalWriteSearchResponse.Builder jwsBuilder =
                        JournalWriteSearchResponse.newBuilder();

                    // handle WriteJournal searches
                    for (Map.Entry<String, Long> entry :
                            jwsRequest.getSketchesMap().entrySet()) {
                        WriteUpdate.Builder wuBuilder =
                            WriteUpdate.newBuilder()
                                .setSketchId(entry.getKey());

                        for (Map.Entry<Long, ByteString> e :
                                this.writeJournal.search(entry.getKey(),
                                    entry.getValue()).entrySet()) {
                            wuBuilder.putData(e.getKey(), e.getValue());
                        }

                        jwsBuilder.addWriteUpdates(wuBuilder.build());
                    }

                    // write to out
                    out.writeInt(messageType);
                    jwsBuilder.build().writeDelimitedTo(out);
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
