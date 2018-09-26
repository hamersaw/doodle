package com.bushpath.doodle.node;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlPluginManager;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.util.TimerTask;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Map;

public class GossipTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipTimerTask.class);

    protected ControlPluginManager controlPluginManager;
    protected NodeManager nodeManager;
    protected PluginManager pluginManager;

    public GossipTimerTask(ControlPluginManager controlPluginManager,
            NodeManager nodeManager, PluginManager pluginManager) {
        this.controlPluginManager = controlPluginManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
    }

    @Override
    public void run() {
        log.trace("Executing");

        // retrieve random node
        NodeMetadata node = this.nodeManager.getRandomNode();
        if (node == null) {
            return;
        }

        // create GossipRequest
        GossipRequest.Builder builder = GossipRequest.newBuilder()
            .setNodesHash(this.nodeManager.getNodesHash())
            .setControlPluginsHash(this.controlPluginManager.getPluginsHash());

        for (Map.Entry<String, ControlPlugin> entry :
                this.controlPluginManager.getPluginEntrySet()) {
            builder.putPluginHashes(entry.getKey(), entry.getValue().hashCode());
        }

        GossipRequest request = builder.build();

        // send GossipRequest
        GossipResponse response = null;
        try {
            Socket socket = new Socket(node.getIpAddress(), node.getPort());

            DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeInt(MessageType.GOSSIP.getNumber());
            request.writeDelimitedTo(out);

            // recv response
            // TODO - validate we have the correct message type
            in.readInt();
            response = GossipResponse.parseDelimitedFrom(in);
        } catch (ConnectException e) {
            log.warn("Connection to {} unsuccessful", node.toString());
            return;
        } catch (IOException e) {
            log.error("Unknown communication error", e);
            return;
        }

        // handle response
        for (Node nodeProto : response.getNodesList()) {
            // check if node exists
            if (this.nodeManager.containsNode(nodeProto.getId())) {
                continue;
            }

            // add node
            NodeMetadata nodeMetadata = new NodeMetadata(nodeProto.getId(),
                nodeProto.getIpAddress(), (short) nodeProto.getPort());

            try {
                this.nodeManager.addNode(nodeMetadata);
            } catch (Exception e) {
                log.error("Failed to add node {}", nodeMetadata, e);
            }
        }

        for (Map.Entry<String, String> pluginEntry :
                response.getControlPluginsMap().entrySet()) {
            // check if control plugin exists
            if (this.controlPluginManager.containsPlugin(pluginEntry.getKey())) {
                continue;
            }

            // add control plugin
            try {
                Class<? extends ControlPlugin> clazz =
                    this.pluginManager.getControlPlugin(pluginEntry.getValue());
                Constructor constructor = clazz.getConstructor();
                ControlPlugin controlPlugin =
                    (ControlPlugin) constructor.newInstance();

                this.controlPluginManager.addPlugin(pluginEntry.getKey(),
                    controlPlugin);
            } catch (Exception e) {
                log.error("Failed to add ControlPlugin: {}",
                    pluginEntry.getKey(), e);
            }
        }

        // handle pluginOperations
        for (Map.Entry<String, VariableOperations> entry :
                response.getPluginOperationsMap().entrySet()) {
            try {
                ControlPlugin controlPlugin =
                    this.controlPluginManager.getPlugin(entry.getKey());

                for (VariableOperation operation :
                        entry.getValue().getOperationsList()) {
                    controlPlugin.handleVariableOperation(operation);
                }
            } catch (Exception e) {
                log.error("Failed to process VariableOperations "
                    + "for ControlPlugin '{}'", entry.getKey(), e);
            }
        }
    }
}
