package com.bushpath.doodle.node.filesystem;

import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.io.DataInputStream;
import java.util.Collection;

public class ClientNamenodeService {
    protected FileManager fileManager;

    public ClientNamenodeService(FileManager fileManager) {
        this.fileManager = fileManager;
    }

    public Message getFileInfo(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        ClientNamenodeProtocolProtos.GetFileInfoRequestProto req =
            ClientNamenodeProtocolProtos.GetFileInfoRequestProto
                .parseDelimitedFrom(in);

        // retrieve DoodleInode
        String user = socketContext.getEffectiveUser();
        DoodleInode inode =
            fileManager.getInode(user, user, req.getSrc());

        // pack results into protobuf
        ClientNamenodeProtocolProtos.GetFileInfoResponseProto.Builder
            gfiBuilder = ClientNamenodeProtocolProtos
                .GetFileInfoResponseProto.newBuilder();

        if (inode != null) {
            // write inode to protobuf
            gfiBuilder.setFs(
                this.toHdfsFileStatusProto(req.getSrc(), inode));
        }

        return gfiBuilder.build();
    }

    public Message getListing(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        ClientNamenodeProtocolProtos.GetListingRequestProto req =
            ClientNamenodeProtocolProtos.GetListingRequestProto
                .parseDelimitedFrom(in);

        String startAfter =
            new String(req.getStartAfter().toByteArray());

        // execute query
        String user = socketContext.getEffectiveUser();
        Collection<DoodleInode> doodleInodes =
            this.fileManager.list(user, user, req.getSrc());

        // pack results into protobuf
        HdfsProtos.DirectoryListingProto.Builder dlBuilder =  
            HdfsProtos.DirectoryListingProto.newBuilder();
        for (DoodleInode inode : doodleInodes) {
			// convert DoodleInode to HdfsFileStatusProto
            HdfsProtos.HdfsFileStatusProto hdfsFileStatusProto =
                this.toHdfsFileStatusProto(inode.getEntry().getName(), inode);

            dlBuilder.addPartialListing(hdfsFileStatusProto);
        }

        dlBuilder.setRemainingEntries(0);

		// return response
        return ClientNamenodeProtocolProtos
			.GetListingResponseProto.newBuilder()
				.setDirList(dlBuilder.build())
				.build();
    }

    protected HdfsProtos.HdfsFileStatusProto
            toHdfsFileStatusProto(String path, DoodleInode inode) {
        HdfsProtos.FsPermissionProto permission =
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(Integer.MAX_VALUE) // TODO - set permissions on inodes
                .build();

        DoodleEntry entry = inode.getEntry();
        HdfsProtos.HdfsFileStatusProto.Builder fsBuilder =
            HdfsProtos.HdfsFileStatusProto.newBuilder()
                .setPath(ByteString.copyFrom(path.getBytes()))
                .setLength(inode.getSize())
                .setPermission(permission)
                .setOwner(inode.getUser())
                .setGroup(inode.getGroup())
                .setModificationTime(inode.getModificationTime())
                .setAccessTime(inode.getAccessTime());

        switch (entry.getFileType()) {
            case DIRECTORY:
                fsBuilder.setFileType(
                        HdfsProtos.HdfsFileStatusProto.FileType.IS_DIR)
                    .setChildrenNum(0); // TODO - get children count
                break;
            case REGULAR:
                fsBuilder.setFileType(
                        HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE)
                    .setBlocksize(0); // TODO - get block size
                
                // TODO - if needLocation setLocations
                break;
        }

        return fsBuilder.build();
    }
}
