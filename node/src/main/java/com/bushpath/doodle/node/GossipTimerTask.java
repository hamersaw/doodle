package com.bushpath.doodle.node;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipHashRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipHashResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipUpdateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.GossipUpdateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlPluginManager;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.sketch.CheckpointMetadata;
import com.bushpath.doodle.node.sketch.CheckpointManager;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.util.TimerTask;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class GossipTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(GossipTimerTask.class);

    protected ControlPluginManager controlPluginManager;
    protected NodeManager nodeManager;
    protected SketchManager sketchManager;
    protected CheckpointManager checkpointManager;

    public GossipTimerTask(ControlPluginManager controlPluginManager,
            NodeManager nodeManager, SketchManager sketchManager,
            CheckpointManager checkpointManager) {
        this.controlPluginManager = controlPluginManager;
        this.nodeManager = nodeManager;
        this.sketchManager = sketchManager;
        this.checkpointManager = checkpointManager;
    }

    @Override
    public void run() {
        log.trace("Executing");

        // retrieve random node
        NodeMetadata gossipNodeMetadata = this.nodeManager
            .getRandomNode(this.nodeManager.getThisNodeId());

        // check if there are other registered nodes
        if (gossipNodeMetadata == null) {
            gossipNodeMetadata = this.nodeManager.getRandomSeed();
        }

        // fallback to contacting a seed node
        if (gossipNodeMetadata == null) {
            return;
        }

        // create GossipHashRequest
        GossipHashRequest gossipHashRequest =
            GossipHashRequest.newBuilder().build();

        // send GossipHashRequest
        GossipHashResponse gossipHashResponse = null;
        try {
            gossipHashResponse = (GossipHashResponse) CommUtility.send(
                MessageType.GOSSIP_HASH.getNumber(), gossipHashRequest,
                gossipNodeMetadata.getIpAddress(),
                (short) gossipNodeMetadata.getPort());
        } catch (ConnectException e) {
            log.warn("Connection to {} unsuccessful",
                gossipNodeMetadata.toString());
            return;
        } catch (Exception e) {
            log.error("Unknown communication error", e);
            return;
        }

        // create GossipUpdateRequest
        boolean send = false;
        GossipUpdateRequest.Builder gossipUpdateBuilder =
            GossipUpdateRequest.newBuilder();

        if (gossipHashResponse.getNodesHash() !=
                this.nodeManager.hashCode()) {
            // if node hash != -> add all nodes
            for (NodeMetadata node : this.nodeManager.getValues()) {
                gossipUpdateBuilder.addNodes(node.toProtobuf());
            }
        }

        // handle control plugins
        if (gossipHashResponse.getControlHash() !=
                this.controlPluginManager.hashCode()) {
            for (Map.Entry<String, ControlPlugin> entry :
                    this.controlPluginManager.getEntrySet()) {
                gossipUpdateBuilder
                    .addControlPlugins(entry.getValue().toGossip());
            }
        }

        // handle sketch plugins
        if (gossipHashResponse.getSketchHash() !=
                this.sketchManager.hashCode()) {
            for (Map.Entry<String, SketchPlugin> entry :
                    this.sketchManager.getEntrySet()) {
                gossipUpdateBuilder
                    .addSketchPlugins(entry.getValue().toGossip());
            }
        }

        // handle checkpoints
        if (gossipHashResponse.getCheckpointHash() !=
                this.checkpointManager.hashCode()) {
            for (Map.Entry<String, CheckpointMetadata> entry :
                    this.checkpointManager.getEntrySet()) {
                gossipUpdateBuilder
                    .addCheckpoints(entry.getValue().toProtobuf());
            }
        }
 
        // send GossipUpdateRequest
        GossipUpdateRequest gossipUpdateRequest = gossipUpdateBuilder.build();
        GossipUpdateResponse gossipUpdateResponse = null;
        try {
            gossipUpdateResponse = (GossipUpdateResponse) CommUtility.send(
                MessageType.GOSSIP_UPDATE.getNumber(), gossipUpdateRequest,
                gossipNodeMetadata.getIpAddress(),
                (short) gossipNodeMetadata.getPort());
        } catch (ConnectException e) {
            log.warn("Connection to {} unsuccessful",
                gossipNodeMetadata.toString());
            return;
        } catch (Exception e) {
            log.error("Unknown communication error", e);
            return;
        }

        // TODO - handle GossipUpdateResponse
    }
}
