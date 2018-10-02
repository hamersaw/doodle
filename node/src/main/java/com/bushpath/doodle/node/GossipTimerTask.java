package com.bushpath.doodle.node;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
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
import com.bushpath.doodle.node.sketch.SketchPluginManager;

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
    protected SketchPluginManager sketchPluginManager;

    public GossipTimerTask(ControlPluginManager controlPluginManager,
            NodeManager nodeManager, PluginManager pluginManager, 
            SketchPluginManager sketchPluginManager) {
        this.controlPluginManager = controlPluginManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
        this.sketchPluginManager = sketchPluginManager;
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
            .setControlPluginsHash(this.controlPluginManager.getPluginsHash())
            .setSketchPluginsHash(this.sketchPluginManager.getPluginsHash());

        for (Map.Entry<String, ControlPlugin> entry :
                this.controlPluginManager.getPluginEntrySet()) {
            builder.putControlOperationsHashes(entry.getKey(),
                entry.getValue().hashCode());
        }

        for (Map.Entry<String, SketchPlugin> entry :
                this.sketchPluginManager.getPluginEntrySet()) {
            builder.putSketchOperationsHashes(entry.getKey(),
                entry.getValue().hashCode());
        }

        GossipRequest request = builder.build();

        // send GossipRequest
        GossipResponse response = null;
        try {
            response = (GossipResponse) CommUtility.send(
                MessageType.GOSSIP.getNumber(), request,
                node.getIpAddress(), (short) node.getPort());
        } catch (ConnectException e) {
            log.warn("Connection to {} unsuccessful", node.toString());
            return;
        } catch (Exception e) {
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

        // handle control plugin hashes and operations
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

        for (Map.Entry<String, VariableOperations> entry :
                response.getControlOperationsMap().entrySet()) {
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
 
        // handle sketch plugin hashes and operations
        for (Map.Entry<String, String> pluginEntry :
                response.getSketchPluginsMap().entrySet()) {
            // check if sketch plugin exists
            if (this.sketchPluginManager.containsPlugin(pluginEntry.getKey())) {
                continue;
            }

            // add sketch plugin
            try {
                Class<? extends SketchPlugin> clazz =
                    this.pluginManager.getSketchPlugin(pluginEntry.getValue());
                Constructor constructor = clazz.getConstructor(String.class);
                SketchPlugin sketchPlugin =
                    (SketchPlugin) constructor.newInstance(pluginEntry.getKey());

                this.sketchPluginManager.addPlugin(pluginEntry.getKey(),
                    sketchPlugin);
            } catch (Exception e) {
                log.error("Failed to add SketchPlugin: {}",
                    pluginEntry.getKey(), e);
            }
        }

        for (Map.Entry<String, VariableOperations> entry :
                response.getSketchOperationsMap().entrySet()) {
            try {
                SketchPlugin sketchPlugin =
                    this.sketchPluginManager.getPlugin(entry.getKey());

                for (VariableOperation operation :
                        entry.getValue().getOperationsList()) {
                    sketchPlugin.handleVariableOperation(operation);
                }
            } catch (Exception e) {
                log.error("Failed to process VariableOperations "
                    + "for SketchPlugin '{}'", entry.getKey(), e);
            }
        }
    }
}
