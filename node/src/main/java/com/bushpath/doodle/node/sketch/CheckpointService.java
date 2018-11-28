package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointInitRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointInitResponse;
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
                MessageType.CHECKPOINT_INIT.getNumber(),
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
                case CHECKPOINT_INIT:
                    // parse request
                    CheckpointInitRequest ciRequest =
                        CheckpointInitRequest.parseDelimitedFrom(in);

                    String ciSketchId = ciRequest.getSketchId();
                    String ciCheckpointId = ciRequest.getCheckpointId();
                    log.trace("handling CheckpointInitRequest {}:{}",
                        ciSketchId, ciCheckpointId);

                    // check if checkpointId and sketchId are valid
                    this.checkpointManager
                        .checkNotExists(ciCheckpointId);
                    this.sketchManager.checkExists(ciSketchId);

                    // init response
                    CheckpointInitResponse.Builder ciBuilder =
                        CheckpointInitResponse.newBuilder();

                    // retrieve sketch
                    SketchPlugin initSketch = this.sketchManager
                        .get(ciSketchId);

                    // create checkpoint
                    CheckpointMetadata checkpoint =
                        this.checkpointManager.initCheckpoint(
                            ciSketchId, ciCheckpointId);

                    // serialize sketch
                    String ciCheckpointFile = this.checkpointManager
                        .getCheckpointFile(ciCheckpointId);
                    File file = new File(ciCheckpointFile);
                    file.getParentFile().mkdirs();
                    FileOutputStream fileOut = new FileOutputStream(file);
                    DataOutputStream dataOut =
                        new DataOutputStream(fileOut);
                    initSketch.serialize(dataOut);
                    dataOut.close();
                    fileOut.close();

                    // add checkpoint
                    this.checkpointManager.add(checkpoint, true);
                    
                    // write to out
                    out.writeInt(messageType);
                    ciBuilder.build().writeDelimitedTo(out);
                    break;
                case CHECKPOINT_ROLLBACK:
                    // parse request
                    CheckpointRollbackRequest crRequest =
                        CheckpointRollbackRequest.parseDelimitedFrom(in);

                    String crSketchId = crRequest.getSketchId();
                    String crCheckpointId = crRequest.getCheckpointId();
                    log.trace("handling CheckpointRollbackRequest {}:{}",
                        crSketchId, crCheckpointId);
 
                    // check if checkpointId and sketchId are valid
                    this.checkpointManager.checkExists(crCheckpointId);
                    this.sketchManager.checkExists(crSketchId);

                    // init response
                    CheckpointRollbackResponse.Builder crBuilder =
                        CheckpointRollbackResponse.newBuilder();

                    // remove sketch
                    this.sketchManager.remove(crSketchId);

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
                    Class<? extends SketchPlugin> clazz =
                        this.pluginManager.getSketchPlugin(classpath);
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
                        controlPlugins[index++] = 
                            this.controlPluginManager.get(controlPluginId);
                    }

                    rollbackSketch.initControlPlugins(controlPlugins);

                    // add new sketch
                    this.sketchManager.add(crSketchId, rollbackSketch);
                    
                    // write to out
                    out.writeInt(messageType);
                    crBuilder.build().writeDelimitedTo(out);
                    break;
                case CHECKPOINT_TRANSFER:
                    // parse request
                    CheckpointTransferRequest ctRequest =
                        CheckpointTransferRequest.parseDelimitedFrom(in);

                    String ctCheckpointId = ctRequest.getCheckpointId();
                    long ctOffset = ctRequest.getOffset();
                    log.trace("handling CheckpointTransferRequest {}:{}",
                        ctCheckpointId, ctOffset);

                    // init response
                    CheckpointTransferResponse.Builder ctBuilder =
                        CheckpointTransferResponse.newBuilder();

                    if (this.checkpointManager
                            .contains(ctCheckpointId)) {
                        // get checkpoint data from offset
                        String transferFile = this.checkpointManager
                            .getCheckpointFile(ctCheckpointId);
                        RandomAccessFile randomAccessFile =
                            new RandomAccessFile(transferFile, "r");
                        randomAccessFile.seek(ctOffset);
                        int length = (int) Math.min(this.transferBufferSize, 
                            randomAccessFile.length() - ctOffset);
                        byte[] data = new byte[length];
                        randomAccessFile.readFully(data);

                        ctBuilder.setData(ByteString.copyFrom(data));
                        ctBuilder.setLastMessage(ctOffset + length
                            == randomAccessFile.length());
                        randomAccessFile.close();
                    } else {
                        ctBuilder.setData(ByteString.EMPTY);
                        ctBuilder.setLastMessage(false);
                    }
                    
                    // write to out
                    out.writeInt(messageType);
                    ctBuilder.build().writeDelimitedTo(out);
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
