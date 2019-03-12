package com.bushpath.doodle.dfs;

import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.ThreadedCursor;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;
import com.bushpath.doodle.dfs.format.Format;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BlockManager {
    protected static final Logger log =
        LoggerFactory.getLogger(BlockManager.class);
    public static final int BLOCK_SIZE = 67108864;

    protected FileManager fileManager;
    protected NodeManager nodeManager;
    protected Map<Long, byte[]> blocks;
    protected ReadWriteLock lock;

    public BlockManager(FileManager fileManager,
            NodeManager nodeManager) {
        this.fileManager = fileManager;
        this.nodeManager = nodeManager;
        this.blocks = new HashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void initializeFileBlocks(int inodeValue,
            DoodleFile doodleFile, Node node) throws Exception {
        Map<Long, Integer> blocksMap = new HashMap();

        // retrieve file metadata
        Format format = doodleFile.getFormat();

        List<Node> list = new ArrayList();
        for (Integer nodeId :
                doodleFile.getReplicas().get(node.getId())) {
            NodeMetadata secondaryNode = this.nodeManager.get(nodeId);
            list.add(secondaryNode.toProtobuf());
        }

        Map<Node, List<Node>> replicas = new HashMap();
        replicas.put(node, list);

        // construct ThreadedCursor
        long fileStartTime = System.currentTimeMillis();
        ThreadedCursor cursor = new ThreadedCursor(replicas,
            doodleFile.getInflator(), doodleFile.getQuery(), 5000, 4);
        // TODO - configure bufferSize (5000) and workerCount (4)

        // generate blocks
        long blockStartTime = System.currentTimeMillis();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        BufferedOutputStream bufOut = new BufferedOutputStream(byteOut);
        DataOutputStream out = new DataOutputStream(bufOut);

        float[] observation = null;
        int blockNum = 0;
        while ((observation = cursor.next()) != null) {
            // format observation
            format.format(observation, out);
            out.flush();

            // check block size
            if (out.size() >= BLOCK_SIZE) {
                long blockMaterializationTime =
                    System.currentTimeMillis() - blockStartTime;

                // close streams
                out.close();
                bufOut.close();
                byteOut.close();

                // initialize block
                // 4bytes - inode | 2bytes - nodeId | 2bytes blockNum
                long blockId = ((long) inodeValue << 32)
                    | ((long) node.getId() << 16) | ((long) blockNum);
                byte[] block = byteOut.toByteArray();
                blockNum += 1;
                
                log.info("generated block {} with length {} in {} ms",
                    blockId, block.length, blockMaterializationTime);

                // add block to blocksMap
                blocksMap.put(blockId, block.length);

                // TODO - write Lock
                this.blocks.put(blockId, block);

                // open new streams
                blockStartTime = System.currentTimeMillis();
                byteOut = new ByteArrayOutputStream();
                bufOut = new BufferedOutputStream(byteOut);
                out = new DataOutputStream(bufOut);
            }
        }

        // finalize last block
        out.close();
        bufOut.close();
        byteOut.close();
        if (out.size() != 0) {
            long blockMaterializationTime =
                System.currentTimeMillis() - blockStartTime;

            // 4bytes - inode | 2bytes - nodeId | 2bytes blockNum
            long blockId = ((long) inodeValue << 32)
                | ((long) node.getId() << 16) | ((long) blockNum);
            byte[] block = byteOut.toByteArray();

            log.info("generated block {} with length {} in {} ms",
                blockId, block.length, blockMaterializationTime);

            // add block to blocksMap
            blocksMap.put(blockId, block.length);

            // TODO - writeLock
            this.blocks.put(blockId, block);
        }

        long fileMaterializationTime =
            System.currentTimeMillis() - fileStartTime;
        long fileLength = 0;
        for (Integer blockSize : blocksMap.values()) {
            fileLength += blockSize;
        }

        log.info("generated file {} with length {} in {} ms",
            doodleFile.getName(), fileLength, fileMaterializationTime);

        // add blocks to file
        doodleFile.addBlocks(blocksMap);
    } 

    public byte[] getBlock(long blockId) {
        return this.blocks.get(blockId);
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
