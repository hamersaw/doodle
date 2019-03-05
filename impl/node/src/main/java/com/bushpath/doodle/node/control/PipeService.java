package com.bushpath.doodle.node.control;

import com.bushpath.doodle.Service;
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

import com.bushpath.doodle.node.data.SketchManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

public class PipeService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(PipeService.class);

    protected SketchManager sketchManager;
    protected PipeManager pipeManager;

    public PipeService(SketchManager sketchManager,
            PipeManager pipeManager) {
        this.sketchManager = sketchManager;
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
                    PipeCloseRequest pcRequest =
                        PipeCloseRequest.parseDelimitedFrom(in);

                    int pcId = pcRequest.getId();
                    log.trace("handling PipeCloseRequest {}", pcId);

                    // init response
                    PipeCloseResponse.Builder pcBuilder =
                        PipeCloseResponse.newBuilder();

                    // handle
                    this.pipeManager.close(pcId);

                    // write to out
                    out.writeInt(messageType);
                    pcBuilder.build().writeDelimitedTo(out);
                    break;
                case PIPE_OPEN:
                    // parse request
                    PipeOpenRequest poRequest =
                        PipeOpenRequest.parseDelimitedFrom(in);

                    String poSketchId = poRequest.getSketchId();
                    List<String> poFeatures = poRequest.getFeaturesList();
                    log.trace("handling PipeOpenRequest {}", poSketchId);

                    // check if sketch exists
                    this.sketchManager.checkExists(poSketchId);
                    this.sketchManager.freeze(poSketchId);
                    SketchPlugin sketch = 
                        this.sketchManager.get(poSketchId);

                    // init response
                    PipeOpenResponse.Builder poBuilder =
                        PipeOpenResponse.newBuilder();

                    // handle
                    int[] featureIndexes =
                        sketch.indexFeatures(poFeatures);

                    for (int featureIndex : featureIndexes) {
                        poBuilder.addFeatureIndexes(featureIndex);
                    }

                    int id = this.pipeManager.open(sketch,
                        poRequest.getTransformThreadCount(),
                        poRequest.getDistributorThreadCount(),
                        poRequest.getBufferSize());

                    poBuilder.setId(id);

                    // write to out
                    out.writeInt(messageType);
                    poBuilder.build().writeDelimitedTo(out);
                    break;
                case PIPE_WRITE:
                    // parse request
                    PipeWriteRequest pwRequest =
                        PipeWriteRequest.parseDelimitedFrom(in);

                    int pwId = pwRequest.getId();
                    log.trace("handling PipeWriteRequest {}", pwId);

                    // init response
                    PipeWriteResponse.Builder pwBuilder =
                        PipeWriteResponse.newBuilder();

                    // handle
                    this.pipeManager.write(pwId, pwRequest.getData());

                    // write to out
                    out.writeInt(messageType);
                    pwBuilder.build().writeDelimitedTo(out);
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
