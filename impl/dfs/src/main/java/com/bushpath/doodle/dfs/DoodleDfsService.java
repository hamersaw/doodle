package com.bushpath.doodle.dfs;

import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import com.bushpath.doodle.protobuf.DoodleProtos.FileGossipResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileGossipRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationRequest;

import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleDirectory;
import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;

import java.io.DataInputStream;
import java.util.Map;
import java.util.Random;

public class DoodleDfsService {
    protected static final Logger log =
        LoggerFactory.getLogger(DoodleDfsService.class);

    protected FileManager fileManager;
    protected NodeManager nodeManager;
    protected OperationJournal journal;
    protected Random random;

    public DoodleDfsService(FileManager fileManager,
            NodeManager nodeManager, OperationJournal journal) {
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
        this.journal = journal;
        this.random = new Random(System.nanoTime());
    }

    public Message addOperation(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        FileOperationRequest request =
            FileOperationRequest.parseDelimitedFrom(in);

        // add to journal
        this.journal.add(request.getOperation());

        return FileOperationResponse.newBuilder()
            .build();
    }

    public Message gossip(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        FileGossipRequest request =
            FileGossipRequest.parseDelimitedFrom(in);

        String user = socketContext.getEffectiveUser();
        log.trace("Recv gossip request from '{}'", user);

        // init response
        FileGossipResponse.Builder builder =
            FileGossipResponse.newBuilder();

        // populate builder
        for (Map.Entry<Long, FileOperation> entry : this.journal
                .search(request.getOperationTimestamp()).entrySet()) {
            builder.addOperations(entry.getValue());
        }

        // populate inode blocks
        for (Integer inodeValue : request.getIncompleteInodesList()) {
            DoodleInode inode = this.fileManager.getInode(inodeValue);
            if (inode == null) {
                continue; // this node hasn't processed yet
            }

            // TODO - lock blocks
            DoodleFile file = (DoodleFile) inode.getEntry();
            for (Map.Entry<Long, Integer> entry :
                    file.getBlocks().entrySet()) {
                builder.putBlocks(entry.getKey(), entry.getValue());
            }
        }

        return builder.build();
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
