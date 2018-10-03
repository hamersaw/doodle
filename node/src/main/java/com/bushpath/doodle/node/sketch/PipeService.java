package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class PipeService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(PipeService.class);

    protected SketchPluginManager sketchPluginManager;
    protected PipeManager pipeManager;

    public PipeService(SketchPluginManager sketchPluginManager,
            PipeManager pipeManager) {
        this.sketchPluginManager = sketchPluginManager;
        this.pipeManager = pipeManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.PIPE_CLOSE.getNumber(),
                MessageType.PIPE_OPEN.getNumber(),
                MessageType.PIPE_WRITE.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case PIPE_CLOSE:
                    // parse request
                    PipeCloseRequest pipeCloseRequest =
                        PipeCloseRequest.parseDelimitedFrom(in);

                    log.info("handling PipeCloseRequest {}",
                        pipeCloseRequest.getId());

                    // init response
                    PipeCloseResponse.Builder pipeCloseBuilder =
                        PipeCloseResponse.newBuilder();

                    // handle
                    this.pipeManager.closePipe(pipeCloseRequest.getId());

                    // write to out
                    out.writeInt(messageType);
                    pipeCloseBuilder.build().writeDelimitedTo(out);
                    break;
                case PIPE_OPEN:
                    // parse request
                    PipeOpenRequest pipeOpenRequest =
                        PipeOpenRequest.parseDelimitedFrom(in);

                    log.info("handling PipeOpenRequest {}",
                        pipeOpenRequest.getSketchId());

                    // init response
                    PipeOpenResponse.Builder pipeOpenBuilder =
                        PipeOpenResponse.newBuilder();

                    // handle
                    SketchPlugin sketch = this.sketchPluginManager
                        .getPlugin(pipeOpenRequest.getSketchId());

                    int[] featureIndexes =
                        sketch.indexFeatures(pipeOpenRequest.getFeaturesList());

                    for (int featureIndex : featureIndexes) {
                        pipeOpenBuilder.addFeatureIndexes(featureIndex);
                    }

                    int id = this.pipeManager.openPipe(sketch,
                        pipeOpenRequest.getTransformThreadCount(),
                        pipeOpenRequest.getDistributorThreadCount(),
                        pipeOpenRequest.getBufferSize());

                    pipeOpenBuilder.setId(id);

                    // write to out
                    out.writeInt(messageType);
                    pipeOpenBuilder.build().writeDelimitedTo(out);
                    break;
                case PIPE_WRITE:
                    // parse request
                    PipeWriteRequest pipeWriteRequest =
                        PipeWriteRequest.parseDelimitedFrom(in);

                    log.info("handling PipeWriteRequest {}",
                        pipeWriteRequest.getId());

                    // init response
                    PipeWriteResponse.Builder pipeWriteBuilder =
                        PipeWriteResponse.newBuilder();

                    // handle
                    this.pipeManager.writePipe(pipeWriteRequest.getId(),
                        pipeWriteRequest.getData());

                    // write to out
                    out.writeInt(messageType);
                    pipeWriteBuilder.build().writeDelimitedTo(out);
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
