package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchCheckpointRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchCheckpointResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchRollbackRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchRollbackResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;

public class CheckpointService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(CheckpointService.class);

    protected CheckpointManager checkpointManager;
    protected SketchManager sketchManager;

    public CheckpointService(CheckpointManager checkpointManager,
            SketchManager sketchManager) {
        this.checkpointManager = checkpointManager;
        this.sketchManager = sketchManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.SKETCH_CHECKPOINT.getNumber(),
                MessageType.SKETCH_ROLLBACK.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case SKETCH_CHECKPOINT:
                    // parse request
                    SketchCheckpointRequest sketchCheckpointRequest =
                        SketchCheckpointRequest.parseDelimitedFrom(in);

                    log.info("handling SketchCheckpointRequest {}:{}",
                        sketchCheckpointRequest.getSketchId(),
                        sketchCheckpointRequest.getCheckpointId());

                    // init response
                    SketchCheckpointResponse.Builder sketchCheckpointBuilder =
                        SketchCheckpointResponse.newBuilder();

                    // retrieve sketch
                    SketchPlugin checkpointSketch = this.sketchManager
                        .getSketch(sketchCheckpointRequest.getSketchId());

                    // create checkpoint
                    CheckpointMetadata checkpoint =
                        this.checkpointManager.createCheckpoint(
                            sketchCheckpointRequest.getSketchId(),
                            sketchCheckpointRequest.getCheckpointId()
                        );

                    // TODO - checkpointSketch.serialize();

                    // add checkpoint
                    this.checkpointManager.addCheckpoint(checkpoint);
                    
                    // write to out
                    out.writeInt(messageType);
                    sketchCheckpointBuilder.build().writeDelimitedTo(out);
                    break;
                case SKETCH_ROLLBACK:
                    // parse request
                    SketchRollbackRequest sketchRollbackRequest =
                        SketchRollbackRequest.parseDelimitedFrom(in);

                    log.info("handling SketchRollbackRequest {}:{}",
                        sketchRollbackRequest.getSketchId(),
                        sketchRollbackRequest.getCheckpointId());

                    // init response
                    SketchRollbackResponse.Builder sketchRollbackBuilder =
                        SketchRollbackResponse.newBuilder();

                    // TODO - handle
                    
                    // write to out
                    out.writeInt(messageType);
                    sketchRollbackBuilder.build().writeDelimitedTo(out);
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
