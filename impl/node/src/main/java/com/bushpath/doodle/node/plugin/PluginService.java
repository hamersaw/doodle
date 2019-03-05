package com.bushpath.doodle.node.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.Service;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListResponse;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class PluginService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(PluginService.class);

    protected PluginManager pluginManager;

    public PluginService(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.PLUGIN_LIST.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case PLUGIN_LIST:
                    // parse request
                    PluginListRequest plRequest =
                        PluginListRequest.parseDelimitedFrom(in);

                    log.trace("handling PluginListRequest");

                    // init response
                    PluginListResponse.Builder plBuilder =
                        PluginListResponse.newBuilder();

                    // populate builder
                    for (String controlPlugin :
                            pluginManager.getControlPlugins()) {
                        plBuilder.addControlPlugins(controlPlugin);
                    }

                    for (String sketchPlugin :
                            pluginManager.getSketchPlugins()) {
                        plBuilder.addSketchPlugins(sketchPlugin);
                    }

                    // write to out
                    out.writeInt(messageType);
                    plBuilder.build().writeDelimitedTo(out);
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
