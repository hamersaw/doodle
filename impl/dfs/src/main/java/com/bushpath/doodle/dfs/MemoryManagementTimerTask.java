package com.bushpath.doodle.dfs;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

public class MemoryManagementTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(MemoryManagementTimerTask.class);

    protected BlockManager blockManager;
    protected FileManager fileManager;
    protected NodeManager nodeManager;

    public MemoryManagementTimerTask(BlockManager blockManager,
            FileManager fileManager, NodeManager nodeManager) {
        this.blockManager = blockManager;
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
    }

    @Override
    public void run() {
        // initialize files
        for (Map.Entry<Integer, DoodleInode> entry :
                fileManager.getEntrySet()) {
            DoodleInode inode = entry.getValue();
            if (inode.getFileType() != FileType.REGULAR) {
                continue;
            }

            // check if file has already been initialized
            DoodleFile file = (DoodleFile) inode.getEntry();
            if (file.getSize() != 0) {
                continue;
            }

            try {
                // retrieve sketch metadata
                NodeMetadata nodeMetadata = this.nodeManager
                    .get(this.nodeManager.getThisNodeId());

                // create SketchShowRequest
                SketchShowRequest request = 
                    SketchShowRequest.newBuilder()
                        .setId(file.getQuery().getEntity())
                        .build();
                SketchShowResponse response = null;

                // send request
                response = (SketchShowResponse) CommUtility.send(
                    MessageType.SKETCH_SHOW.getNumber(), request,
                    nodeMetadata.getIpAddress(),
                    nodeMetadata.getPort());

                // populate replicas
                Map<Integer, List<Integer>> replicas = new HashMap();
                for (Replica replica : response.getReplicasList()) {
                    replicas.put(replica.getPrimaryNodeId(),
                        replica.getSecondaryNodeIdsList());
                }

                // construct Inflator
                ClassLoader classLoader =
                    Thread.currentThread().getContextClassLoader();
                Class c =
                    classLoader.loadClass(response.getInflatorClass());
                Constructor constructor = c.getConstructor(List.class);
                Inflator inflator = (Inflator) constructor
                    .newInstance(response.getVariablesList());

                // initialize file
                file.initialize(inflator, replicas);

                // initialize file blocks
                this.blockManager.initializeFileBlocks(
                    inode.getInodeValue(), file,
                    nodeMetadata.toProtobuf());
            } catch (Exception e) {
                log.warn("failed to initialize inode '{}:{}'",
                    inode.getInodeValue(), file.getName());
            }
        }

        // TODO - evict unused blocks
    }
}
