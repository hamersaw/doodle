package com.bushpath.doodle.node.filesystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

public class BlockUtil {
    protected static final Logger log =
        LoggerFactory.getLogger(BlockUtil.class);
    public static final int BLOCK_SIZE = 67108864;

    public static LinkedHashMap<Long, Long>
            getBlockSizes(DoodleInode inode) {
        // retrieve DoodleFile
        DoodleFile file = (DoodleFile) inode.getEntry();

        // determine length of block for break along record boundaries
        int featureCount = file.getFeatureCount();
        Format format = file.getFormat();
        int recordSize = (int) format.length(featureCount, 1);

        int maxBlockSize = BLOCK_SIZE - (BLOCK_SIZE % recordSize);

        // iterate over observations
        LinkedHashMap<Long, Long> blockSizes = new LinkedHashMap();
        for (Map.Entry<Integer, Integer> entry :
                file.getObservationEntrySet()) {
            // get size of data at node
            long size = format.length(featureCount, entry.getValue());

            // iterate over blocks of size <= maxBlockSize
            short blockNum = 0;
            long offset = 0;
            while (offset < size) {
                long blockSize = Math.min(maxBlockSize, size - offset);

                // compute blockId
                // 4bytes - inode | 2bytes - nodeId | 2bytes blockNum
                long blockId = ((long) inode.getInodeValue()) << 32
                    | ((long) entry.getKey() << 16) | ((long) blockNum);

                blockSizes.put(blockId, blockSize);

                // increment values
                blockNum += 1;
                offset += blockSize;
            }
        }

        log.trace("Computed {} block size(s) for inode {}",
            blockSizes.size(), inode.getInodeValue());
        return blockSizes;
    }

    public static int getInode(long blockId) {
        return (int) (blockId >> 32);
    }

    public static int getNodeId(long blockId) {
        return (int) ((short) (blockId >> 16));
    }

    public static short getBlockNum(long blockId) {
        return (short) blockId;
    }
}
