package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlInitRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlInitResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlModifyRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlModifyResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.util.Map;

public class ControlService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(ControlService.class);

    protected ControlPluginManager controlPluginManager;
    protected NodeManager nodeManager;
    protected PluginManager pluginManager;
    protected SketchManager sketchManager;

    public ControlService(ControlPluginManager controlPluginManager,
            NodeManager nodeManager, PluginManager pluginManager,
            SketchManager sketchManager) {
        this.controlPluginManager = controlPluginManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
        this.sketchManager = sketchManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.GOSSIP.getNumber(),
                MessageType.CONTROL_INIT.getNumber(),
                MessageType.CONTROL_LIST.getNumber(),
                MessageType.CONTROL_MODIFY.getNumber(),
                MessageType.CONTROL_SHOW.getNumber()
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
                    GossipRequest gossipRequest =
                        GossipRequest.parseDelimitedFrom(in);

                    log.trace("handling GossipRequest");

                    // init response
                    GossipResponse.Builder gossipBuilder =
                        GossipResponse.newBuilder();

                    // populate builder
                    if (gossipRequest.getNodesHash() !=
                            this.nodeManager.getNodesHash()) {
                        // if node hash != -> add all nodes
                        for (NodeMetadata node : this.nodeManager.getNodeValues()) {
                            Node nodeProto = Node.newBuilder()
                                .setId(node.getId())
                                .setIpAddress(node.getIpAddress())
                                .setPort(node.getPort())
                                .build();

                            gossipBuilder.addNodes(nodeProto);
                        }
                    }

                    // handle control plugins
                    if (gossipRequest.getControlHash() !=
                            this.controlPluginManager.hashCode()) {
                        for (Map.Entry<String, ControlPlugin> entry :
                                this.controlPluginManager.getPluginEntrySet()) {
                            gossipBuilder
                                .addControlPlugins(entry.getValue().toGossip());
                        }
                    }

                    // handle sketch plugins
                    if (gossipRequest.getSketchHash() !=
                            this.sketchManager.hashCode()) {
                        for (Map.Entry<String, SketchPlugin> entry :
                                this.sketchManager.getSketchesEntrySet()) {
                            gossipBuilder
                                .addSketchPlugins(entry.getValue().toGossip());
                        }
                    }

                    // write to out
                    out.writeInt(messageType);
                    gossipBuilder.build().writeDelimitedTo(out);
                    break;
                case CONTROL_INIT:
                    // parse request
                    ControlInitRequest controlInitRequest =
                        ControlInitRequest.parseDelimitedFrom(in);

                    log.info("handling ControlInitRequest {}:{}",
                        controlInitRequest.getId(), controlInitRequest.getPlugin());

                    // init response
                    ControlInitResponse.Builder controlInitBuilder =
                        ControlInitResponse.newBuilder();

                    // add control plugin
                    Class<? extends ControlPlugin> clazz = this.pluginManager
                        .getControlPlugin(controlInitRequest.getPlugin());
                    Constructor constructor = clazz.getConstructor(String.class);
                    ControlPlugin controlPlugin = (ControlPlugin) 
                        constructor.newInstance(controlInitRequest.getId());

                    this.controlPluginManager.addPlugin(controlInitRequest.getId(),
                        controlPlugin);

                    // write to out
                    out.writeInt(messageType);
                    controlInitBuilder.build().writeDelimitedTo(out);
                    break;
                case CONTROL_LIST:
                    // parse request
                    ControlListRequest controlListRequest =
                        ControlListRequest.parseDelimitedFrom(in);

                    log.info("handling ControlListRequest");

                    // init response
                    ControlListResponse.Builder controlListBuilder =
                        ControlListResponse.newBuilder();

                    // add plugins
                    for (Map.Entry<String, ControlPlugin> entry :
                            this.controlPluginManager.getPluginEntrySet()) {
                        controlListBuilder.putPlugins(entry.getKey(),
                            entry.getValue().getClass().getName());
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    controlListBuilder.build().writeDelimitedTo(out);
                    break;
                case CONTROL_MODIFY:
                    // parse request
                    ControlModifyRequest controlModifyRequest =
                        ControlModifyRequest.parseDelimitedFrom(in);

                    log.info("handling ControlModifyRequest '{}'",
                        controlModifyRequest.getId());

                    // init response
                    ControlModifyResponse.Builder controlModifyBuilder =
                        ControlModifyResponse.newBuilder();

                    // handle operations
                    ControlPlugin modifyPlugin = this.controlPluginManager
                        .getPlugin(controlModifyRequest.getId());

                    for (VariableOperation operation :
                            controlModifyRequest.getOperationsList()) {
                        modifyPlugin.handleVariableOperation(operation);
                    }

                    // write to out
                    out.writeInt(messageType);
                    controlModifyBuilder.build().writeDelimitedTo(out);
                    break;
                case CONTROL_SHOW:
                    // parse request
                    ControlShowRequest controlShowRequest =
                        ControlShowRequest.parseDelimitedFrom(in);

                    log.info("handling ControlShowRequest '{}'",
                        controlShowRequest.getId());

                    // init response
                    ControlShowResponse.Builder controlShowBuilder =
                        ControlShowResponse.newBuilder();

                    // handle operations
                    ControlPlugin showPlugin = this.controlPluginManager
                        .getPlugin(controlShowRequest.getId());

                    controlShowBuilder.setPlugin(showPlugin.getClass().getName());
                    controlShowBuilder.addAllVariables(showPlugin.getVariables());

                    // write to out
                    out.writeInt(messageType);
                    controlShowBuilder.build().writeDelimitedTo(out);
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
