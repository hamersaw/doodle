package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.control.ControlManager;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public class SketchService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(SketchService.class);

    protected ControlManager controlManager;
    protected NodeManager nodeManager;
    protected PluginManager pluginManager;
    protected SketchManager sketchManager;

    public SketchService(ControlManager controlManager,
            NodeManager nodeManager, PluginManager pluginManager,
            SketchManager sketchManager) {
        this.controlManager = controlManager;
        this.nodeManager = nodeManager;
        this.pluginManager = pluginManager;
        this.sketchManager = sketchManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.SKETCH_LIST.getNumber(),
                MessageType.SKETCH_SHOW.getNumber(),
                MessageType.SKETCH_WRITE.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case SKETCH_LIST:
                    // parse request
                    SketchListRequest slRequest =
                        SketchListRequest.parseDelimitedFrom(in);

                    log.trace("handling SketchListRequest");

                    // init response
                    SketchListResponse.Builder slBuilder =
                        SketchListResponse.newBuilder();

                    // add plugins
                    for (Map.Entry<String, SketchPlugin> entry :
                            this.sketchManager.getEntrySet()) {
                        slBuilder.putPlugins(entry.getKey(),
                            entry.getValue().getClass().getName());
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    slBuilder.build().writeDelimitedTo(out);
                    break;
                case SKETCH_SHOW:
                    // parse request
                    SketchShowRequest ssRequest =
                        SketchShowRequest.parseDelimitedFrom(in);

                    String ssId = ssRequest.getId();
                    log.trace("handling SketchShowRequest '{}'", ssId);

                    // check if sketch exists
                    this.sketchManager.checkExists(ssId);

                    // init response
                    SketchShowResponse.Builder ssBuilder =
                        SketchShowResponse.newBuilder();

                    // handle operations
                    SketchPlugin showSketch =
                        this.sketchManager.get(ssId);

                    ssBuilder.setPlugin(showSketch
                        .getClass().getName());
                    ssBuilder.setInflatorClass(showSketch
                        .getInflatorClass());
                    ssBuilder.setReplicationFactor(showSketch
                        .getReplicationFactor());
                    ssBuilder.setFrozen(showSketch.frozen());
                    ssBuilder.addAllVariables(showSketch
                        .getVariables());

                    for (Integer nodeId : showSketch.getPrimaryReplicas(
                            this.nodeManager.getThisNodeId())) {
                        ssBuilder.putFlushTimestamps(nodeId,
                            showSketch.getFlushTimestamp(nodeId));

                        ssBuilder.putWriteTimestamps(nodeId,
                            showSketch.getWriteTimestamp(nodeId));
                    }

                    // write to out
                    out.writeInt(messageType);
                    ssBuilder.build().writeDelimitedTo(out);
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
