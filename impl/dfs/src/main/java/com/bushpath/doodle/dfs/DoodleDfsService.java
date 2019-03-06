package com.bushpath.doodle.dfs;

import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationRequest;

import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleDirectory;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;

import java.io.DataInputStream;
import java.util.Random;

public class DoodleDfsService {
    protected static final Logger log =
        LoggerFactory.getLogger(DoodleDfsService.class);

    protected FileManager fileManager;
    protected NodeManager nodeManager;
    protected Random random;

    public DoodleDfsService(FileManager fileManager,
            NodeManager nodeManager) {
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
        this.random = new Random(System.nanoTime());
    }

    public Message addOperation(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        FileOperationRequest request =
            FileOperationRequest.parseDelimitedFrom(in);

        this.fileManager.handleOperation(request.getOperation());
        // TODO - add to operations list
        
        return FileOperationResponse.newBuilder()
            .build();
    }

    public Message list(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        FileListRequest request =
            FileListRequest.parseDelimitedFrom(in);

        String path = request.getPath();
        String user = socketContext.getEffectiveUser();
        log.debug("Recv list request for '{}' from '{}'",
            path, user);

        // init response
        FileListResponse.Builder builder =
            FileListResponse.newBuilder();

        // populate builder
        for (DoodleInode inode :
            this.fileManager.list(user, user, path)) {
            builder.addFiles(inode.toProtobuf());
        }

        return builder.build();
    }
}
