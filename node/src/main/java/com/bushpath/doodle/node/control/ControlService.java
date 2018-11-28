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
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.sketch.CheckpointManager;
import com.bushpath.doodle.node.sketch.CheckpointMetadata;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.util.Map;

public class ControlService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(ControlService.class);

    protected ControlPluginManager controlPluginManager;
    protected PluginManager pluginManager;

    public ControlService(ControlPluginManager controlPluginManager,
            PluginManager pluginManager) {
        this.controlPluginManager = controlPluginManager;
        this.pluginManager = pluginManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
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
                case CONTROL_INIT:
                    // parse request
                    ControlInitRequest ciRequest =
                        ControlInitRequest.parseDelimitedFrom(in);

                    String ciId = ciRequest.getId();
                    String ciPlugin = ciRequest.getPlugin();
                    log.trace("handling ControlInitRequest {}:{}",
                        ciId, ciPlugin);

                    // check if control plugin already exists
                    this.controlPluginManager.checkNotExists(ciId);

                    // init response
                    ControlInitResponse.Builder ciBuilder =
                        ControlInitResponse.newBuilder();

                    // add control plugin
                    Class<? extends ControlPlugin> clazz = 
                        this.pluginManager.getControlPlugin(ciPlugin);
                    Constructor constructor = 
                        clazz.getConstructor(String.class);
                    ControlPlugin controlPlugin = (ControlPlugin) 
                        constructor.newInstance(ciId);

                    this.controlPluginManager.add(ciId, controlPlugin);

                    // write to out
                    out.writeInt(messageType);
                    ciBuilder.build().writeDelimitedTo(out);
                    break;
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
                case CONTROL_MODIFY:
                    // parse request
                    ControlModifyRequest cmRequest =
                        ControlModifyRequest.parseDelimitedFrom(in);

                    String cmId = cmRequest.getId();
                    log.trace("handling ControlModifyRequest '{}'",
                        cmId);

                    // check if control plugin exists
                    this.controlPluginManager.checkExists(cmId);

                    // init response
                    ControlModifyResponse.Builder cmBuilder =
                        ControlModifyResponse.newBuilder();

                    // handle operations
                    ControlPlugin modifyPlugin =
                        this.controlPluginManager.get(cmId);

                    for (VariableOperation operation :
                            cmRequest.getOperationsList()) {
                        modifyPlugin.handleVariableOperation(operation);
                    }

                    // write to out
                    out.writeInt(messageType);
                    cmBuilder.build().writeDelimitedTo(out);
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
