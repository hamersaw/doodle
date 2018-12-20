package com.bushpath.doodle.node.analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;
import com.bushpath.doodle.protobuf.DoodleProtos.FileAddRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileAddResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Random;

public class AnalyticsService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(AnalyticsService.class);

    protected FileManager fileManager;
    protected Random random;

    public AnalyticsService(FileManager fileManager) {
        this.fileManager = fileManager;
        this.random = new Random(System.nanoTime());
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.FILE_ADD.getNumber(),
                MessageType.FILE_DELETE.getNumber(),
                MessageType.FILE_LIST.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case FILE_ADD:
                    // parse request
                    FileAddRequest faRequest =
                        FileAddRequest.parseDelimitedFrom(in);

                    FileType faFileType = faRequest.getFileType();
                    String faUser = faRequest.getUser();
                    String faGroup = faRequest.getGroup();
                    String faPath = faRequest.getPath();
                    log.trace("handling FileAddRequest {}", faPath);

                    // init response
                    FileAddResponse.Builder faBuilder =
                        FileAddResponse.newBuilder();

                    // populate builder
                    DoodleInode faInode = this.fileManager
                        .create(faFileType, faUser, faGroup, faPath);

                    this.fileManager.add(faUser, faGroup, 
                        faPath, random.nextInt(), faInode);

                    // write to out
                    out.writeInt(messageType);
                    faBuilder.build().writeDelimitedTo(out);
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
                    this.fileManager.delete(fdUser, fdGroup, fdPath);

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
