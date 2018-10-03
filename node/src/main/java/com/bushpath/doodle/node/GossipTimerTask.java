package com.bushpath.doodle.node;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchPluginGossip;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

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
import java.util.List;
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
            .setControlHash(this.controlPluginManager.hashCode())
            .setSketchHash(this.sketchPluginManager.hashCode());

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

        // handle control plugins
        for (ControlPluginGossip pluginGossip : response.getControlPluginsList()) {
            ControlPlugin plugin;
            // handle plugin
            if (this.controlPluginManager.containsPlugin(pluginGossip.getId())) {
                // retrieve plugin
                plugin = this.controlPluginManager.getPlugin(pluginGossip.getId());
            } else {
                // create plugin if it doesn't exit
                try {
                    Class<? extends ControlPlugin> clazz = this.pluginManager
                        .getControlPlugin(pluginGossip.getClasspath());
                    Constructor constructor = clazz.getConstructor(String.class);
                    plugin = (ControlPlugin)
                        constructor.newInstance(pluginGossip.getId());

                    this.controlPluginManager
                        .addPlugin(pluginGossip.getId(), plugin);
                } catch (Exception e) {
                    log.error("Failed to add ControlPlugin: {}",
                        pluginGossip.getId(), e);
                    continue;
                }
            }

            // handle operations
            for (VariableOperation operation : pluginGossip.getOperationsList()) {
                plugin.handleVariableOperation(operation);
            }
        }
 
        // handle sketch plugins
        for (SketchPluginGossip pluginGossip : response.getSketchPluginsList()) {
            SketchPlugin plugin;
            // handle plugin
            if (this.sketchPluginManager.containsPlugin(pluginGossip.getId())) {
                // retrieve plugin
                plugin = this.sketchPluginManager.getPlugin(pluginGossip.getId());
            } else {
                // create plugin if it doesn't exit
                try {
                    List<String> list = pluginGossip.getControlPluginsList();
                    ControlPlugin[] controlPlugins = new ControlPlugin[list.size()];
                    for (int i=0; i<controlPlugins.length; i++) {
                        controlPlugins[i] =
                            this.controlPluginManager.getPlugin(list.get(i));
                    }

                    Class<? extends SketchPlugin> clazz = this.pluginManager
                        .getSketchPlugin(pluginGossip.getClasspath());
                    Constructor constructor = 
                        clazz.getConstructor(String.class, ControlPlugin[].class);
                    plugin = (SketchPlugin) constructor
                        .newInstance(pluginGossip.getId(), controlPlugins);

                    this.sketchPluginManager
                        .addPlugin(pluginGossip.getId(), plugin);
                } catch (Exception e) {
                    log.error("Failed to add SketchPlugin: {}",
                        pluginGossip.getId(), e);
                    continue;
                }
            }

            // handle operations
            for (VariableOperation operation : pluginGossip.getOperationsList()) {
                plugin.handleVariableOperation(operation);
            }
        }
    }
}
