package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.ControlPlugin;
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
import com.bushpath.doodle.node.control.ControlPluginManager;
import com.bushpath.doodle.node.plugin.PluginManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.util.Set;

public class CheckpointService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(CheckpointService.class);

    protected CheckpointManager checkpointManager;
    protected ControlPluginManager controlPluginManager;
    protected PluginManager pluginManager;
    protected SketchManager sketchManager;
    protected int transferBufferSize;

    public CheckpointService(CheckpointManager checkpointManager,
            ControlPluginManager controlPluginManager,
            PluginManager pluginManager, SketchManager sketchManager,
            int transferBufferSize) {
        this.checkpointManager = checkpointManager;
        this.controlPluginManager = controlPluginManager;
        this.pluginManager = pluginManager;
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
                    CheckpointCreateRequest checkpointCreateRequest =
                        CheckpointCreateRequest.parseDelimitedFrom(in);

                    String ccSketchId =
                        checkpointCreateRequest.getSketchId();
                    String ccCheckpointId =
                        checkpointCreateRequest.getCheckpointId();
                    log.trace("handling CheckpointCreateRequest {}:{}",
                        ccSketchId, ccCheckpointId);

                    // init response
                    CheckpointCreateResponse.Builder sketchCheckpointBuilder =
                        CheckpointCreateResponse.newBuilder();

                    // retrieve sketch
                    SketchPlugin checkpointSketch = this.sketchManager
                        .getSketch(ccSketchId);

                    // create checkpoint
                    CheckpointMetadata checkpoint =
                        this.checkpointManager.createCheckpoint(
                            ccSketchId, ccCheckpointId);

                    // serialize sketch
                    String ccCheckpointFile = this.checkpointManager
                        .getCheckpointFile(ccCheckpointId);
                    File file = new File(ccCheckpointFile);
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

                    String crSketchId =
                        checkpointRollbackRequest.getSketchId();
                    String crCheckpointId =
                        checkpointRollbackRequest.getCheckpointId();
                    log.trace("handling CheckpointRollbackRequest {}:{}",
                        crSketchId, crCheckpointId);

                    // init response
                    CheckpointRollbackResponse.Builder
                        checkpointRollbackBuilder =
                            CheckpointRollbackResponse.newBuilder();

                    // remove sketch (if exists)
                    this.sketchManager.removeSketch(crSketchId);

                    // open DataInputStream on checkpoint
                    String crCheckpointFile = this.checkpointManager
                        .getCheckpointFile(crCheckpointId);
                    FileInputStream fileIn =
                        new FileInputStream(crCheckpointFile);
                    DataInputStream dataIn =
                        new DataInputStream(fileIn);

                    // read classpath
                    int classpathLength = dataIn.readInt(); 
                    byte[] classpathBytes = new byte[classpathLength];
                    dataIn.readFully(classpathBytes);
                    String classpath = new String(classpathBytes);

                    // initialize sketch
                    Class<? extends SketchPlugin> clazz = this.pluginManager
                        .getSketchPlugin(classpath);
                    Constructor constructor =
                        clazz.getConstructor(DataInputStream.class);
                    SketchPlugin rollbackSketch = 
                        (SketchPlugin) constructor.newInstance(dataIn);

                    rollbackSketch.replayVariableOperations();
                    rollbackSketch.loadData(dataIn);
                    
                    dataIn.close();
                    fileIn.close();

                    // initControlPlugin
                    Set<String> controlPluginIds = 
                        rollbackSketch.getControlPluginIds();
                    ControlPlugin[] controlPlugins = 
                        new ControlPlugin[controlPluginIds.size()];
                    int index=0;
                    for (String controlPluginId : controlPluginIds) {
                        controlPlugins[index++] = this.controlPluginManager
                            .getPlugin(controlPluginId);
                    }

                    rollbackSketch.initControlPlugins(controlPlugins);

                    // add new sketch
                    this.sketchManager.addSketch(crSketchId, rollbackSketch);
                    
                    // write to out
                    out.writeInt(messageType);
                    checkpointRollbackBuilder.build().writeDelimitedTo(out);
                    break;
                case CHECKPOINT_TRANSFER:
                    // parse request
                    CheckpointTransferRequest checkpointTransferRequest =
                        CheckpointTransferRequest.parseDelimitedFrom(in);

                    String ctCheckpointId = checkpointTransferRequest
                        .getCheckpointId();
                    long offset = checkpointTransferRequest.getOffset();

                    log.trace("handling CheckpointTransferRequest {}:{}",
                        ctCheckpointId, offset);

                    // init response
                    CheckpointTransferResponse.Builder checkpointTransferBuilder =
                        CheckpointTransferResponse.newBuilder();

                    if (this.checkpointManager
                            .containsCheckpoint(ctCheckpointId)) {
                        // get checkpoint data from offset
                        String transferFile = this.checkpointManager
                            .getCheckpointFile(ctCheckpointId);
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
