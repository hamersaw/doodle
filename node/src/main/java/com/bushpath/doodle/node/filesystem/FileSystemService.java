package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.data.SketchManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.util.Random;

public class FileSystemService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(FileSystemService.class);

    protected FileManager fileManager;
    protected NodeManager nodeManager;
    protected SketchManager sketchManager;
    protected Random random;

    public FileSystemService(FileManager fileManager,
            NodeManager nodeManager, SketchManager sketchManager ) {
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
        this.sketchManager = sketchManager;
        this.random = new Random(System.nanoTime());
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.FILE_CREATE.getNumber(),
                MessageType.FILE_DELETE.getNumber(),
                MessageType.FILE_LIST.getNumber(),
                MessageType.FILE_MKDIR.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case FILE_CREATE:
                    // parse request
                    FileCreateRequest fcRequest =
                        FileCreateRequest.parseDelimitedFrom(in);

                    ByteString data = fcRequest.getQuery();
                    ObjectInputStream objectIn =
                        new ObjectInputStream(data.newInput());
                    Query query = (Query) objectIn.readObject();
                    objectIn.close();

                    String fcUser = fcRequest.getUser();
                    String fcGroup = fcRequest.getGroup();
                    String fcPath = fcRequest.getPath();
                    log.trace("handling FileCreateRequest {}", fcPath);

                    // init response
                    FileCreateResponse.Builder fcBuilder =
                        FileCreateResponse.newBuilder();

                    // populate builder
                    // check if sketch exists
                    this.sketchManager.checkExists(query.getEntity());

                    // get SketchPlugin
                    SketchPlugin sketch =
                        this.sketchManager.get(query.getEntity());
                    int featureCount = sketch.getFeatures().size();
                    long observationCount =
                        sketch.getObservationCount(query);

                    // create inode
                    String fcFilename =
                        this.fileManager.parseFilename(fcPath);
                    Format format =
                        Format.getFormat(fcRequest.getFileFormat());
                    DoodleFile fcFile = new DoodleFile(fcFilename, 
                        format, query, data, featureCount);

                    fcFile.addObservations(
                        this.nodeManager.getThisNodeId(),
                        (int) observationCount);

                    long fcTimestamp = System.currentTimeMillis();
                    int fcValue = random.nextInt();
                    DoodleInode fcInode = new DoodleInode(
                        fcValue, fcUser, fcGroup, fcTimestamp,
                        fcTimestamp, fcTimestamp, fcFile);

                    // add inode
                    this.fileManager.add(fcUser, fcGroup, 
                        fcPath, fcValue, fcInode);

                    // add to operations
                    FileOperation fcOp = FileOperation.newBuilder()
                        .setTimestamp(fcTimestamp)
                        .setPath(fcPath)
                        .setFile(fcInode.toProtobuf())
                        .setOperationType(OperationType.ADD)
                        .build();

                    this.fileManager.addOperation(fcOp);

                    // write to out
                    out.writeInt(messageType);
                    fcBuilder.build().writeDelimitedTo(out);
                    break;
                case FILE_DELETE:
                    // parse request
                    FileDeleteRequest fdRequest =
                        FileDeleteRequest.parseDelimitedFrom(in);

                    String fdUser = fdRequest.getUser();
                    String fdGroup = fdRequest.getGroup();
                    String fdPath = fdRequest.getPath();
                    log.trace("handling FileDeleteRequest {}", fdPath);

                    // init response
                    FileDeleteResponse.Builder fdBuilder =
                        FileDeleteResponse.newBuilder();

                    // populate builder
                    DoodleInode fdInode = this.fileManager
                        .delete(fdUser, fdGroup, fdPath);

                    // add to operations
                    long rdTimestamp = System.currentTimeMillis();
                    FileOperation fdOp = FileOperation.newBuilder()
                        .setTimestamp(rdTimestamp)
                        .setPath(fdPath)
                        .setFile(fdInode.toProtobuf())
                        .setOperationType(OperationType.DELETE)
                        .build();

                    this.fileManager.addOperation(fdOp);

                    // write to out
                    out.writeInt(messageType);
                    fdBuilder.build().writeDelimitedTo(out);
                    break;
                case FILE_LIST:
                    // parse request
                    FileListRequest flRequest =
                        FileListRequest.parseDelimitedFrom(in);

                    String flUser = flRequest.getUser();
                    String flGroup = flRequest.getGroup();
                    String flPath = flRequest.getPath();
                    log.trace("handling FileListRequest {}", flPath);

                    // init response
                    FileListResponse.Builder flBuilder =
                        FileListResponse.newBuilder();

                    // populate builder
                    for (DoodleInode inode :
                        this.fileManager.list(flUser, flGroup, flPath)) {
                        flBuilder.addFiles(inode.toProtobuf());
                    }

                    // write to out
                    out.writeInt(messageType);
                    flBuilder.build().writeDelimitedTo(out);
                    break;
                case FILE_MKDIR:
                    // parse request
                    FileMkdirRequest fmRequest =
                        FileMkdirRequest.parseDelimitedFrom(in);

                    String fmUser = fmRequest.getUser();
                    String fmGroup = fmRequest.getGroup();
                    String fmPath = fmRequest.getPath();
                    log.trace("handling FileMkdirRequest {}", fmPath);

                    // init response
                    FileMkdirResponse.Builder fmBuilder =
                        FileMkdirResponse.newBuilder();

                    // populate builder
                    String fmFilename =
                        this.fileManager.parseFilename(fmPath);
                    DoodleDirectory fmDirectory =
                        new DoodleDirectory(fmFilename);

                    long fmTimestamp = System.currentTimeMillis();
                    int fmValue = random.nextInt();
                    DoodleInode fmInode = new DoodleInode(
                        fmValue, fmUser, fmGroup, fmTimestamp,
                        fmTimestamp, fmTimestamp, fmDirectory);

                    this.fileManager.add(fmUser, fmGroup, 
                        fmPath, fmValue, fmInode);

                    // add to operations
                    FileOperation fmOp = FileOperation.newBuilder()
                        .setTimestamp(fmTimestamp)
                        .setPath(fmPath)
                        .setFile(fmInode.toProtobuf())
                        .setOperationType(OperationType.ADD)
                        .build();

                    this.fileManager.addOperation(fmOp);

                    // write to out
                    out.writeInt(messageType);
                    fmBuilder.build().writeDelimitedTo(out);
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
