package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchInitRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchInitResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchModifyRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchModifyResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public class SketchService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(SketchService.class);

    protected PluginManager pluginManager;
    protected SketchPluginManager sketchPluginManager;

    public SketchService(PluginManager pluginManager,
            SketchPluginManager sketchPluginManager) {
        this.pluginManager = pluginManager;
        this.sketchPluginManager = sketchPluginManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.SKETCH_INIT.getNumber(),
                MessageType.SKETCH_LIST.getNumber(),
                MessageType.SKETCH_MODIFY.getNumber(),
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
                case SKETCH_INIT:
                    // parse request
                    SketchInitRequest sketchInitRequest =
                        SketchInitRequest.parseDelimitedFrom(in);

                    log.info("handling SketchInitRequest {}:{}",
                        sketchInitRequest.getId(), sketchInitRequest.getPlugin());

                    // init response
                    SketchInitResponse.Builder sketchInitBuilder =
                        SketchInitResponse.newBuilder();

                    // add sketch plugin
                    Class<? extends SketchPlugin> clazz = this.pluginManager
                        .getSketchPlugin(sketchInitRequest.getPlugin());
                    Constructor constructor = clazz.getConstructor(String.class);
                    SketchPlugin sketchPlugin = (SketchPlugin)
                        constructor.newInstance(sketchInitRequest.getId());

                    this.sketchPluginManager.addPlugin(sketchInitRequest.getId(),
                        sketchPlugin);

                    // write to out
                    out.writeInt(messageType);
                    sketchInitBuilder.build().writeDelimitedTo(out);
                    break;
                case SKETCH_LIST:
                    // parse request
                    SketchListRequest sketchListRequest =
                        SketchListRequest.parseDelimitedFrom(in);

                    log.info("handling SketchListRequest");

                    // init response
                    SketchListResponse.Builder sketchListBuilder =
                        SketchListResponse.newBuilder();

                    // add plugins
                    for (Map.Entry<String, SketchPlugin> entry :
                            this.sketchPluginManager.getPluginEntrySet()) {
                        sketchListBuilder.putPlugins(entry.getKey(),
                            entry.getValue().getClass().getName());
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    sketchListBuilder.build().writeDelimitedTo(out);
                    break;
                case SKETCH_MODIFY:
                    // parse request
                    SketchModifyRequest sketchModifyRequest =
                        SketchModifyRequest.parseDelimitedFrom(in);

                    log.info("handling SketchModifyRequest '{}'",
                        sketchModifyRequest.getId());

                    // init response
                    SketchModifyResponse.Builder sketchModifyBuilder =
                        SketchModifyResponse.newBuilder();

                    // handle operations
                    SketchPlugin modifyPlugin = this.sketchPluginManager
                        .getPlugin(sketchModifyRequest.getId());

                    for (VariableOperation operation :
                            sketchModifyRequest.getOperationsList()) {
                        modifyPlugin.handleVariableOperation(operation);
                    }

                    // write to out
                    out.writeInt(messageType);
                    sketchModifyBuilder.build().writeDelimitedTo(out);
                    break;
                case SKETCH_SHOW:
                    // parse request
                    SketchShowRequest sketchShowRequest =
                        SketchShowRequest.parseDelimitedFrom(in);

                    log.info("handling SketchShowRequest '{}'",
                        sketchShowRequest.getId());

                    // init response
                    SketchShowResponse.Builder sketchShowBuilder =
                        SketchShowResponse.newBuilder();

                    // handle operations
                    SketchPlugin showPlugin = this.sketchPluginManager
                        .getPlugin(sketchShowRequest.getId());

                    sketchShowBuilder.setPlugin(showPlugin.getClass().getName());
                    sketchShowBuilder.addAllVariables(showPlugin.getVariables());

                    // write to out
                    out.writeInt(messageType);
                    sketchShowBuilder.build().writeDelimitedTo(out);
                    break;
                case SKETCH_WRITE:
                    // parse request
                    SketchWriteRequest sketchWriteRequest =
                        SketchWriteRequest.parseDelimitedFrom(in);

                    log.info("handling SketchWriteRequest '{}'",
                        sketchWriteRequest.getSketchId());

                    // init response
                    SketchWriteResponse.Builder sketchWriteBuilder =
                        SketchWriteResponse.newBuilder();

                    // handle
                    SketchPlugin writePlugin = this.sketchPluginManager
                        .getPlugin(sketchWriteRequest.getSketchId());

                    writePlugin.write(sketchWriteRequest.getData());

                    // write to out
                    out.writeInt(messageType);
                    sketchWriteBuilder.build().writeDelimitedTo(out);
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
