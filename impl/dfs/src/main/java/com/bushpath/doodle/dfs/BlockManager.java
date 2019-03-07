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
        ThreadedCursor cursor = new ThreadedCursor(replicas,
            doodleFile.getInflator(), doodleFile.getQuery(), 5000, 4);
        // TODO - configure bufferSize (5000) and workerCount (4)

        // generate blocks
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);

        float[] observation = null;
        int blockNum = 0;
        while ((observation = cursor.next()) != null) {
            // format observation
            format.format(observation, out);
            out.flush();

            // check block size
            if (out.size() >= BLOCK_SIZE) {
                // close streams
                out.close();
                byteOut.close();

                // initialize block
                // 4bytes - inode | 2bytes - nodeId | 2bytes blockNum
                long blockId = ((long) inodeValue << 32)
                    | ((long) node.getId() << 16) | ((long) blockNum);
                byte[] block = byteOut.toByteArray();
                blockNum += 1;
                
                log.info("generated block {} with length {}",
                    blockId, block.length);

                // add block to DoodleFile
                doodleFile.addBlock(blockId, block.length);

                // TODO - write Lock
                this.blocks.put(blockId, block);

                // open new streams
                byteOut = new ByteArrayOutputStream();
                out = new DataOutputStream(byteOut);
            }
        }

        // finalize last block
        out.close();
        byteOut.close();
        if (out.size() != 0) {
            // 4bytes - inode | 2bytes - nodeId | 2bytes blockNum
            long blockId = ((long) inodeValue << 32)
                | ((long) node.getId() << 16) | ((long) blockNum);
            byte[] block = byteOut.toByteArray();

            log.info("generated block {} with length {}",
                blockId, block.length);

            // add block to DoodleFile
            doodleFile.addBlock(blockId, block.length);

            // TODO - writeLock
            this.blocks.put(blockId, block);
        }

    } 

    public byte[] getBlock(long blockId) {
        // TODO
        return null;
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
