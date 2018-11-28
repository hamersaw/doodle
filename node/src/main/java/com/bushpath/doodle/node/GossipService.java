package com.bushpath.doodle.node;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Checkpoint;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipHashRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipHashResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipUpdateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipUpdateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.control.ControlPluginManager;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.sketch.CheckpointManager;
import com.bushpath.doodle.node.sketch.CheckpointMetadata;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public class GossipService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipService.class);

    protected CheckpointManager checkpointManager;
    protected ControlPluginManager controlPluginManager;
    protected PluginManager pluginManager;
    protected NodeManager nodeManager;
    protected SketchManager sketchManager;

    public GossipService(CheckpointManager checkpointManager,
            ControlPluginManager controlPluginManager,
            PluginManager pluginManager, NodeManager nodeManager,
            SketchManager sketchManager) {
        this.checkpointManager = checkpointManager;
        this.controlPluginManager = controlPluginManager;
        this.pluginManager = pluginManager;
        this.nodeManager = nodeManager;
        this.sketchManager = sketchManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.GOSSIP_HASH.getNumber(),
                MessageType.GOSSIP_UPDATE.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case GOSSIP_HASH:
                    // parse request
                    GossipHashRequest gossipHashRequest =
                        GossipHashRequest.parseDelimitedFrom(in);

                    log.trace("handling GossipHashRequest");

                    // init response
                    GossipHashResponse.Builder gossipHashBuilder =
                        GossipHashResponse.newBuilder()
                        .setNodesHash(this.nodeManager.hashCode())
                        .setControlHash(this.controlPluginManager.hashCode())
                        .setSketchHash(this.sketchManager.hashCode())
                        .setCheckpointHash(this.checkpointManager.hashCode());


                    // write to out
                    out.writeInt(messageType);
                    gossipHashBuilder.build().writeDelimitedTo(out);
                    break;
                case GOSSIP_UPDATE:
                    // parse request
                    GossipUpdateRequest gossipUpdateRequest =
                        GossipUpdateRequest.parseDelimitedFrom(in);

                    log.trace("handling GossipUpdateRequest");

                    // init response
                    GossipUpdateResponse.Builder gossipUpdateBuilder =
                        GossipUpdateResponse.newBuilder();

                    // handle response
                    for (Node nodeProto : gossipUpdateRequest.getNodesList()) {
                        // check if node exists
                        if (this.nodeManager
                                .contains(nodeProto.getId())) {
                            continue;
                        }

                        // add node
                        NodeMetadata nodeMetadata = new NodeMetadata(
                                nodeProto.getId(),
                                nodeProto.getIpAddress(),
                                (short) nodeProto.getPort()
                            );

                        try {
                            this.nodeManager.add(nodeMetadata);
                        } catch (Exception e) {
                            log.error("Failed to add node {}",
                                nodeMetadata, e);
                        }
                    }

                    // handle control plugins
                    for (ControlPluginGossip pluginGossip :
                            gossipUpdateRequest.getControlPluginsList()) {
                        ControlPlugin plugin;
                        // handle plugin
                        if (this.controlPluginManager
                                .contains(pluginGossip.getId())) {
                            // retrieve plugin
                            plugin = this.controlPluginManager
                                .get(pluginGossip.getId());
                        } else {
                            // create plugin if it doesn't exit
                            try {
                                Class<? extends ControlPlugin> clazz =
                                    this.pluginManager.getControlPlugin(
                                        pluginGossip.getClasspath());
                                Constructor constructor = 
                                    clazz.getConstructor(String.class);
                                plugin = (ControlPlugin) constructor
                                    .newInstance(pluginGossip.getId());

                                this.controlPluginManager.add(
                                    pluginGossip.getId(), plugin);
                            } catch (Exception e) {
                                log.error("Failed to add ControlPlugin: {}",
                                    pluginGossip.getId(), e);
                                continue;
                            }
                        }

                        // handle operations
                        for (VariableOperation operation :
                                pluginGossip.getOperationsList()) {
                            plugin.handleVariableOperation(operation);
                        }
                    }
             
                    // handle sketch plugins
                    for (SketchPluginGossip pluginGossip :
                            gossipUpdateRequest.getSketchPluginsList()) {
                        SketchPlugin sketch;
                        // handle sketch
                        if (this.sketchManager
                                .contains(pluginGossip.getId())) {
                            // retrieve sketch
                            sketch = this.sketchManager
                                .get(pluginGossip.getId());
                        } else {
                            // create sketch if it doesn't exit
                            try {
                                // create SketchPlugin
                                Class<? extends SketchPlugin> clazz =
                                    this.pluginManager.getSketchPlugin(
                                        pluginGossip.getClasspath());
                                Constructor constructor = 
                                    clazz.getConstructor(String.class);
                                sketch = (SketchPlugin) constructor
                                    .newInstance(pluginGossip.getId());
                                
                                // initControlPlugins
                                List<String> list = pluginGossip
                                    .getControlPluginsList();
                                ControlPlugin[] controlPlugins = 
                                    new ControlPlugin[list.size()];

                                for (int i=0; i<controlPlugins.length; i++) {
                                    controlPlugins[i] = 
                                        this.controlPluginManager
                                            .get(list.get(i));
                                }

                                sketch.initControlPlugins(controlPlugins);

                                // add sketch
                                this.sketchManager.add(
                                    pluginGossip.getId(), sketch);
                            } catch (Exception e) {
                                log.error("Failed to add Sketch: {}",
                                    pluginGossip.getId(), e);
                                continue;
                            }
                        }

                        // handle operations
                        for (VariableOperation operation :
                                pluginGossip.getOperationsList()) {
                            sketch.handleVariableOperation(operation);
                        }
                    }

                    // handle checkpoints
                    for (Checkpoint checkpoint :
                            gossipUpdateRequest.getCheckpointsList()) {
                        if (this.checkpointManager.contains(
                                checkpoint.getCheckpointId())) {
                            continue;
                        }

                        CheckpointMetadata checkpointMetadata =
                            new CheckpointMetadata(
                                    checkpoint.getTimestamp(),
                                    checkpoint.getSketchId(),
                                    checkpoint.getCheckpointId()
                                );

                        // add replicas
                        for (Replica replica :
                                checkpoint.getReplicasList()) {
                            // get primary node
                            int primaryNodeId = replica.getPrimaryNodeId();

                            // get secondary nodes
                            int[] secondaryNodeIds =
                                new int[replica.getSecondaryNodeIdsCount()];
                            int index = 0;
                            for (Integer secondaryNodeId :
                                    replica.getSecondaryNodeIdsList()) {
                                secondaryNodeIds[index++] = secondaryNodeId;
                            }

                            // add replica
                            checkpointMetadata.addReplica(
                                    primaryNodeId,
                                    secondaryNodeIds
                                );
                        }

                        // add Checkpoint
                        SketchPlugin checkpointSketch = this.sketchManager
                            .get(checkpointMetadata.getSketchId());
 
                        // serialize sketch
                        String checkpointFile = this.checkpointManager
                            .getCheckpointFile(
                                checkpointMetadata.getCheckpointId());
                        File file = new File(checkpointFile);
                        file.getParentFile().mkdirs();
                        FileOutputStream fileOut = new FileOutputStream(file);
                        DataOutputStream dataOut =
                            new DataOutputStream(fileOut);
                        checkpointSketch.serialize(dataOut);
                        dataOut.close();
                        fileOut.close();

                        this.checkpointManager.add(checkpointMetadata);
                    }

                    // write to out
                    out.writeInt(messageType);
                    gossipUpdateBuilder.build().writeDelimitedTo(out);
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
