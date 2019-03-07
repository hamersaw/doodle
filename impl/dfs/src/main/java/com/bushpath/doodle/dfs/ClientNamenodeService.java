package com.bushpath.doodle.dfs;

import com.bushpath.anamnesis.ipc.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.ipc.rpc.SocketContext;

import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.security.proto.SecurityProtos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleDirectory;
import com.bushpath.doodle.dfs.file.DoodleEntry;
import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ClientNamenodeService {
    protected static final Logger log =
        LoggerFactory.getLogger(ClientNamenodeService.class);

    protected FileManager fileManager;
    protected NodeManager nodeManager;

    public ClientNamenodeService(FileManager fileManager,
            NodeManager nodeManager) {
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
    }

    public Message getBlockLocations(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req =
            ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto
                .parseDelimitedFrom(in);

        String user = socketContext.getEffectiveUser();
        log.debug("Recv getBlockLocations request for {} from '{}'",
            req.getSrc(), user);

        // retrieve DoodleInode
        DoodleInode inode =
            fileManager.getInode(user, user, req.getSrc());
        
        // retrieve DoodleFile
        if (inode.getFileType() != FileType.REGULAR) {
            throw new RuntimeException("file is not of type 'REGULAR'");
        }

        // return block locations
        return ClientNamenodeProtocolProtos
            .GetBlockLocationsResponseProto.newBuilder()
                .setLocations(this.toLocatedBlocksProto(inode))
                .build();
    }

    public Message getFileInfo(DataInputStream in,
            SocketContext socketContext) throws Exception {
        // parse request
        ClientNamenodeProtocolProtos.GetFileInfoRequestProto req =
            ClientNamenodeProtocolProtos.GetFileInfoRequestProto
                .parseDelimitedFrom(in);

        String user = socketContext.getEffectiveUser();
        log.debug("Recv getFileInfo request for {} from '{}'",
            req.getSrc(), user);

        // retrieve DoodleInode
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

        String user = socketContext.getEffectiveUser();
        log.debug("Recv getListing request for {} from '{}'",
            req.getSrc(), user);

        String startAfter =
            new String(req.getStartAfter().toByteArray());

        // execute query
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

    public Message getServerDefaults(DataInputStream in,
            SocketContext socketContext) throws Exception {
        log.debug("Recv getServerDefaults request");

        // parse request
        ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req =
            ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto
                .parseDelimitedFrom(in);
  
        // retrieve server defaults     
        HdfsProtos.FsServerDefaultsProto fsServerDefaultsProto =
            HdfsProtos.FsServerDefaultsProto.newBuilder()
                .setBlockSize(BlockManager.BLOCK_SIZE)
                .setBytesPerChecksum(DataTransferProtocol.CHUNK_SIZE)
                .setWritePacketSize(-1) // TODO
                .setReplication(1)
                .setFileBufferSize(-1) // TODO 
                .setChecksumType(
                    HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C)
                .build();      
        
        return ClientNamenodeProtocolProtos
            .GetServerDefaultsResponseProto.newBuilder()
                .setServerDefaults(fsServerDefaultsProto)
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
                DoodleDirectory directory = (DoodleDirectory) entry;
                fsBuilder.setFileType(
                        HdfsProtos.HdfsFileStatusProto.FileType.IS_DIR)
                    .setChildrenNum(directory.getInodes().size());
                break;
            case REGULAR:
                DoodleFile file = (DoodleFile) entry;
                fsBuilder.setFileType(
                        HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE)
                    .setBlocksize(BlockManager.BLOCK_SIZE);
                
                // TODO - if needLocation setLocations
                break;
        }

        return fsBuilder.build();
    }

    protected HdfsProtos.LocatedBlocksProto
            toLocatedBlocksProto(DoodleInode inode) throws Exception {
        DoodleFile file = (DoodleFile) inode.getEntry();

        // populate LocatedBlockProto list
        int offset = 0;
        List<HdfsProtos.LocatedBlockProto> list = new ArrayList();

        // iterate over blocks
        for (Map.Entry<Long, Integer> entry :
                file.getBlocks().entrySet()) {
            long blockId = entry.getKey();
            int blockSize = entry.getValue();

            int nodeId = BlockManager.getNodeId(blockId);

            log.debug("Creating LocatedBlockProto for "
                + "blockId:{} offset:{} blockSize:{} nodeId:{}",
                blockId, offset, blockSize, nodeId);

            // create DatanodeInfoProto
            NodeMetadata nodeMetadata = this.nodeManager.get(nodeId);

            HdfsProtos.DatanodeIDProto datanodeIdProto =
                HdfsProtos.DatanodeIDProto.newBuilder()
                    .setIpAddr(nodeMetadata.getIpAddress())
                    .setHostName(nodeMetadata.toString())
                    .setDatanodeUuid(
                        Integer.toString(nodeMetadata.getId()))
                    .setXferPort(nodeMetadata.getDatanodeXferPort())
                    .setInfoPort(nodeMetadata.getDatanodeInfoPort())
                    .setIpcPort(nodeMetadata.getDatanodeIpcPort())
                    .build();

            // TODO - fix capacity, dfsUsed, remaining, blockPoolUsed
            HdfsProtos.DatanodeInfoProto datanodeInfoProto =
                HdfsProtos.DatanodeInfoProto.newBuilder()
                    .setId(datanodeIdProto)
                    .setCapacity(5953756348416l)
                    .setDfsUsed(1250664165815l)
                    .setRemaining(3433347485696l)
                    .setBlockPoolUsed(1250664165815l)
                    .setLastUpdate(-1) // TODO
                    .setXceiverCount(1)
                    .setLocation("/default-rack")
                    .setNonDfsUsed(0)
                    .setAdminState(
                        HdfsProtos.DatanodeInfoProto.AdminState.NORMAL)
                    .setCacheCapacity(0)
                    .setCacheUsed(0)
                    .build();
 
            // create LocatedBlockProto
            HdfsProtos.ExtendedBlockProto extendedBlockProto =
                HdfsProtos.ExtendedBlockProto.newBuilder()
                    .setPoolId("")
                    .setBlockId(blockId)
                    .setGenerationStamp(inode.getModificationTime())
                    .setNumBytes(blockSize)
                    .build();

            SecurityProtos.TokenProto tokenProto =
                SecurityProtos.TokenProto.newBuilder()
                    .setIdentifier(ByteString.copyFrom(new byte[]{}))
                    .setPassword(ByteString.copyFrom(new byte[]{}))
                    .setKind("")
                    .setService("")
                    .build();

            HdfsProtos.LocatedBlockProto locatedBlockProto =
                HdfsProtos.LocatedBlockProto.newBuilder()
                    .setB(extendedBlockProto)
                    .setOffset(offset)
                    .addLocs(datanodeInfoProto)
                    .setCorrupt(false)
                    .setBlockToken(tokenProto)
                    .addIsCached(true)
                    .addStorageTypes(
                        HdfsProtos.StorageTypeProto.RAM_DISK)
                    .addStorageIDs("default-storage")
                    .build();

            list.add(locatedBlockProto);
            offset += blockSize;
        }

        // create LocatedBlocksProto
        return HdfsProtos.LocatedBlocksProto.newBuilder()
            .setFileLength(file.getSize())
            .addAllBlocks(list)
            .setUnderConstruction(false) // TODO
            .setIsLastBlockComplete(true) // TODO
            .build();
    }
}
