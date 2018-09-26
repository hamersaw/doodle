package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlInitRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlInitResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.plugin.PluginManager;

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

    public ControlService(ControlPluginManager controlPluginManager,
            NodeManager nodeManager, PluginManager pluginManager) {
        this.controlPluginManager = controlPluginManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
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
        switch (MessageType.forNumber(messageType)) {
            case GOSSIP:
                // parse request
                GossipRequest gossipRequest =
                    GossipRequest.parseDelimitedFrom(in);

                log.trace("handling GossipRequest");

                // init response
                GossipResponse.Builder gossipBuilder = GossipResponse.newBuilder();

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

                if (gossipRequest.getControlPluginsHash() !=
                        this.controlPluginManager.getPluginsHash()) {
                    // if control plugins hash != -> add all control plugins
                    for (Map.Entry<String, ControlPlugin> entry :
                            this.controlPluginManager.getPluginEntrySet()) {
                        gossipBuilder.putControlPlugins(entry.getKey(),
                            entry.getValue().getClass().getName());
                    }
                }

                // TODO - populate pluginBytes

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
                try {
                    Class<? extends ControlPlugin> clazz = this.pluginManager
                        .getControlPlugin(controlInitRequest.getPlugin());
                    Constructor constructor = clazz.getConstructor();
                    ControlPlugin controlPlugin =
                        (ControlPlugin) constructor.newInstance();

                    this.controlPluginManager.addPlugin(controlInitRequest.getId(),
                        controlPlugin);
                } catch (Exception e) {
                    log.error("Failed to add ControlPlugin: {}",
                        controlInitRequest.getId(), e);
                }

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
                // TODO
                break;
            case CONTROL_SHOW:
                // TODO
                break;
            default:
                // unreachable code
        }
    }
}
