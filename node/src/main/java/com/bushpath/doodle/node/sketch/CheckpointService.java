package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointCreateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointCreateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointRollbackRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointRollbackResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointTransferRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointTransferResponse;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.util.List;

public class CheckpointService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(CheckpointService.class);

    protected CheckpointManager checkpointManager;
    protected SketchManager sketchManager;
    protected int transferBufferSize;

    public CheckpointService(CheckpointManager checkpointManager,
            SketchManager sketchManager, int transferBufferSize) {
        this.checkpointManager = checkpointManager;
        this.sketchManager = sketchManager;
        this.transferBufferSize = transferBufferSize;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.CHECKPOINT_CREATE.getNumber(),
                MessageType.CHECKPOINT_ROLLBACK.getNumber(),
                MessageType.CHECKPOINT_TRANSFER.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case CHECKPOINT_CREATE:
                    // parse request
                    CheckpointCreateRequest sketchCheckpointRequest =
                        CheckpointCreateRequest.parseDelimitedFrom(in);

                    log.info("handling CheckpointCreateRequest {}:{}",
                        sketchCheckpointRequest.getSketchId(),
                        sketchCheckpointRequest.getCheckpointId());

                    // init response
                    CheckpointCreateResponse.Builder sketchCheckpointBuilder =
                        CheckpointCreateResponse.newBuilder();

                    // retrieve sketch
                    SketchPlugin checkpointSketch = this.sketchManager
                        .getSketch(sketchCheckpointRequest.getSketchId());

                    // create checkpoint
                    CheckpointMetadata checkpoint =
                        this.checkpointManager.createCheckpoint(
                            sketchCheckpointRequest.getSketchId(),
                            sketchCheckpointRequest.getCheckpointId()
                        );

                    // serialize sketch
                    String checkpointFile = this.checkpointManager
                        .getCheckpointFile(checkpoint.getCheckpointId());
                    File file = new File(checkpointFile);
                    file.getParentFile().mkdirs();
                    FileOutputStream fileOut = new FileOutputStream(file);
                    DataOutputStream dataOut =
                        new DataOutputStream(fileOut);
                    checkpointSketch.serialize(dataOut);
                    dataOut.close();
                    fileOut.close();

                    // add checkpoint
                    this.checkpointManager.addCheckpoint(checkpoint);
                    
                    // write to out
                    out.writeInt(messageType);
                    sketchCheckpointBuilder.build().writeDelimitedTo(out);
                    break;
                case CHECKPOINT_ROLLBACK:
                    // parse request
                    CheckpointRollbackRequest checkpointRollbackRequest =
                        CheckpointRollbackRequest.parseDelimitedFrom(in);

                    log.info("handling CheckpointRollbackRequest {}:{}",
                        checkpointRollbackRequest.getSketchId(),
                        checkpointRollbackRequest.getCheckpointId());

                    // init response
                    CheckpointRollbackResponse.Builder checkpointRollbackBuilder =
                        CheckpointRollbackResponse.newBuilder();

                    // TODO - handle
                    
                    // write to out
                    out.writeInt(messageType);
                    checkpointRollbackBuilder.build().writeDelimitedTo(out);
                    break;
                case CHECKPOINT_TRANSFER:
                    // parse request
                    CheckpointTransferRequest checkpointTransferRequest =
                        CheckpointTransferRequest.parseDelimitedFrom(in);

                    String checkpointId = checkpointTransferRequest
                        .getCheckpointId();
                    long offset = checkpointTransferRequest.getOffset();

                    log.info("handling CheckpointTransferRequest {}:{}",
                        checkpointId, offset);

                    // init response
                    CheckpointTransferResponse.Builder checkpointTransferBuilder =
                        CheckpointTransferResponse.newBuilder();

                    if (this.checkpointManager
                            .containsCheckpoint(checkpointId)) {
                        // get checkpoint data from offset
                        String transferFile = this.checkpointManager
                            .getCheckpointFile(checkpointId);
                        RandomAccessFile randomAccessFile =
                            new RandomAccessFile(transferFile, "r");
                        randomAccessFile.seek(offset);
                        int length = (int) Math.min(this.transferBufferSize, 
                            randomAccessFile.length() - offset);
                        byte[] data = new byte[length];
                        randomAccessFile.readFully(data);

                        checkpointTransferBuilder
                            .setData(ByteString.copyFrom(data));
                        checkpointTransferBuilder.setLastMessage(
                            offset + length
                                == randomAccessFile.length());
                        randomAccessFile.close();
                    } else {
                        checkpointTransferBuilder.setData(ByteString.EMPTY);
                        checkpointTransferBuilder.setLastMessage(false);
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    checkpointTransferBuilder.build().writeDelimitedTo(out);
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
