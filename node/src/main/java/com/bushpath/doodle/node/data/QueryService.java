package com.bushpath.doodle.node.data;

import com.bushpath.doodle.Poison;
import com.bushpath.doodle.Service;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryProfileRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryProfileResponse;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueryService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(QueryService.class);

    protected NodeManager nodeManager;
    protected PluginManager pluginManager;
    protected SketchManager sketchManager;

    public QueryService(NodeManager nodeManager,
            PluginManager pluginManager, SketchManager sketchManager) {
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
        this.sketchManager = sketchManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.QUERY.getNumber(),
                MessageType.QUERY_PROFILE.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case QUERY:
                    // parse request
                    QueryRequest qRequest =
                        QueryRequest.parseDelimitedFrom(in);

                    ByteString data = qRequest.getQuery();
                    Query query =
                        Query.fromInputStream(data.newInput());

                    int nodeId = qRequest.getNodeId();
                    String qEntity = query.getEntity();
                    log.trace("handling QueryRequest {}:{}",
                        nodeId, qEntity);

                    // check if sketch exists
                    this.sketchManager.checkExists(qEntity);

                    // query
                    BlockingQueue<Serializable> queue =
                        this.sketchManager
                            .query(qEntity, nodeId, query);
                    this.handleResponse(queue, out,
                        qRequest.getBufferSize());

                    break;
                case QUERY_PROFILE:
                    // parse request
                    QueryProfileRequest qpRequest =
                        QueryProfileRequest.parseDelimitedFrom(in);

                    ByteString profileData = qpRequest.getQuery();
                    Query profileQuery =
                        Query.fromInputStream(profileData.newInput());

                    int qpNodeId = qpRequest.getNodeId();
                    String qpEntity = profileQuery.getEntity();
                    log.trace("handling QueryProfileRequest {}:{}",
                        qpNodeId, qpEntity);

                    // check if sketch exists
                    this.sketchManager.checkExists(qpEntity);

                    // execute test query
                    long qpStartTime = System.currentTimeMillis();
                    long observationCount = this.sketchManager
                        .getObservationCount(qpEntity,
                            qpNodeId, profileQuery);
                    long qpExecutionTime = System.currentTimeMillis()
                        - qpStartTime;

                    // init response
                    QueryProfileResponse.Builder qpBuilder =
                        QueryProfileResponse.newBuilder();

                    qpBuilder.setObservationCount(observationCount);
                    qpBuilder
                        .setExecutionTimeMilliSeconds(qpExecutionTime);

                    // write to out
                    out.writeInt(messageType);
                    qpBuilder.build().writeDelimitedTo(out);
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

    protected void handleResponse(BlockingQueue<Serializable> in,
            DataOutputStream out, int bufferSize) throws Exception {
        ByteString.Output byteString = ByteString.newOutput();
        ObjectOutputStream byteOut = new ObjectOutputStream(byteString);

        Serializable s = null;
        while (true) {
            // retrieve next "bytes" from queue
            try {
                s = in.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("failed to poll queue", e);
            }

            if (s instanceof Exception) {
                throw (Exception) s;
            } else if (s instanceof Poison) {
                break;
            } else if (s == null) {
                continue;
            }

            // write bytes to ByteString
            try {
                byteOut.writeObject(s);
                byteOut.flush();
            } catch (IOException e) {
                log.warn("failed to write object", e);
            }

            if (byteString.size() >= bufferSize) {
                try {
                    byteOut.close();
                } catch (IOException e) {
                    log.warn("failed to close ObjectOutputStream", e);
                }

                // write QueryResponse
                QueryResponse queryResponse = 
                    QueryResponse.newBuilder()
                        .setData(byteString.toByteString())
                        .setLastMessage(false)
                        .build();

                try {
                    out.writeInt(MessageType.QUERY.getNumber());
                    queryResponse.writeDelimitedTo(out);
                } catch (IOException e) {
                    log.error("failed to write QueryResponse", e);
                }

                byteString = ByteString.newOutput();

                try {
                    byteOut = new ObjectOutputStream(byteString);
                } catch (IOException e) {
                    log.error("failed to create ObjectOutputStream", e);
                    return;
                }
            }
        }

        try {
            byteOut.close();
        } catch (IOException e) {
            log.warn("failed to close ObjectOutputStream", e);
        }

        // write QueryResponse
        QueryResponse queryResponse = 
            QueryResponse.newBuilder()
                .setData(byteString.toByteString())
                .setLastMessage(true)
                .build();

        try {
            out.writeInt(MessageType.QUERY.getNumber());
            queryResponse.writeDelimitedTo(out);
        } catch (IOException e) {
            log.error("failed to write QueryResponse", e);
        }
    }
}
