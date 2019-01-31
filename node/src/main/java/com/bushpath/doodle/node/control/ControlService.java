package com.bushpath.doodle.node.control;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;

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

    protected ControlManager controlPluginManager;
    protected PluginManager pluginManager;

    public ControlService(ControlManager controlPluginManager,
            PluginManager pluginManager) {
        this.controlPluginManager = controlPluginManager;
        this.pluginManager = pluginManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.CONTROL_LIST.getNumber(),
                MessageType.CONTROL_SHOW.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case CONTROL_LIST:
                    // parse request
                    ControlListRequest clRequest =
                        ControlListRequest.parseDelimitedFrom(in);

                    log.trace("handling ControlListRequest");

                    // init response
                    ControlListResponse.Builder clBuilder =
                        ControlListResponse.newBuilder();

                    // add plugins
                    for (Map.Entry<String, ControlPlugin> entry :
                            this.controlPluginManager.getEntrySet()) {
                        clBuilder.putPlugins(entry.getKey(),
                            entry.getValue().getClass().getName());
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    clBuilder.build().writeDelimitedTo(out);
                    break;
                case CONTROL_SHOW:
                    // parse request
                    ControlShowRequest csRequest =
                        ControlShowRequest.parseDelimitedFrom(in);

                    String csId = csRequest.getId();
                    log.trace("handling ControlShowRequest '{}'", csId);

                    // check if control plugin exists
                    this.controlPluginManager.checkExists(csId);

                    // init response
                    ControlShowResponse.Builder csBuilder =
                        ControlShowResponse.newBuilder();

                    // handle operations
                    ControlPlugin showPlugin = 
                        this.controlPluginManager.get(csId);

                    csBuilder.setPlugin(showPlugin.getClass().getName());
                    csBuilder.setFrozen(showPlugin.frozen());
                    csBuilder.addAllVariables(showPlugin.getVariables());

                    // write to out
                    out.writeInt(messageType);
                    csBuilder.build().writeDelimitedTo(out);
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
