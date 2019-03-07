package com.bushpath.doodle.dfs;

import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;

import java.util.TimerTask;
import java.util.Map;

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

            DoodleFile file = (DoodleFile) inode.getEntry();
            try {
                if (!file.getInitialized()) {
                    Map<Long, Integer> blocks = this.blockManager
                        .initializeFileBlocks(inode.getInodeValue(),
                            this.nodeManager.getThisNodeId());

                    // TODO - add blocks to file

                    file.setInitialized(true);
                }
            } catch (Exception e) {
                log.warn("failed to initialize inode '{}:{}'",
                    inode.getInodeValue(), file.getName());
            }
        }

        // TODO - evict unused blocks
    }
}
