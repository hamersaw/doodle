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
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class AnalyticsService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(AnalyticsService.class);

    protected FileManager fileManager;

    public AnalyticsService(FileManager fileManager) {
        this.fileManager = fileManager;
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

                    log.trace("handling FileAddRequest");

                    // init response
                    FileAddResponse.Builder faBuilder =
                        FileAddResponse.newBuilder();

                    // TODO - populate builder

                    // write to out
                    out.writeInt(messageType);
                    faBuilder.build().writeDelimitedTo(out);
                    break;
                case FILE_DELETE:
                    // parse request
                    FileDeleteRequest fdRequest =
                        FileDeleteRequest.parseDelimitedFrom(in);

                    log.trace("handling FileDeleteRequest");

                    // init response
                    FileDeleteResponse.Builder fdBuilder =
                        FileDeleteResponse.newBuilder();

                    // TODO - populate builder

                    // write to out
                    out.writeInt(messageType);
                    fdBuilder.build().writeDelimitedTo(out);
                    break;
                case FILE_LIST:
                    // parse request
                    FileListRequest flRequest =
                        FileListRequest.parseDelimitedFrom(in);

                    log.trace("handling FileListRequest");

                    // init response
                    FileListResponse.Builder flBuilder =
                        FileListResponse.newBuilder();

                    // TODO - populate builder

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
