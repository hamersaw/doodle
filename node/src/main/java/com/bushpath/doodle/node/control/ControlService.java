package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;

public class ControlService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(ControlService.class);

    protected ControlPluginManager controlPluginManager;
    protected NodeManager nodeManager;

    public ControlService(ControlPluginManager controlPluginManager,
            NodeManager nodeManager) {
        this.controlPluginManager = controlPluginManager;
        this.nodeManager = nodeManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.GOSSIP.getNumber(),
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
            default:
                // unreachable code
        }
    }
}
