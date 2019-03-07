package com.bushpath.doodle.dfs;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.ThreadedCursor;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.DoodleFile;
import com.bushpath.doodle.dfs.file.DoodleInode;
import com.bushpath.doodle.dfs.file.FileManager;
import com.bushpath.doodle.dfs.format.Format;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
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

    public Map<Long, Integer> initializeFileBlocks(int inode,
            int nodeId) throws Exception {
        Map<Long, Integer> blockMap = new HashMap();

        /**
         * retrieve DoodleFile
         */
        DoodleInode doodleInode = fileManager.getInode(inode);
        if (doodleInode.getFileType() != FileType.REGULAR) {
            throw new RuntimeException("Unable to initialize blocks for inode '" + inode + "'. file is a directory");
        }

        DoodleFile file = (DoodleFile) doodleInode.getEntry();
        Format format = file.getFormat();

        /**
         * retrieve sketch metadata
         */
        NodeMetadata nodeMetadata = this.nodeManager.get(nodeId);

        // create SketchShowRequest
        SketchShowRequest request = SketchShowRequest.newBuilder()
            .setId(file.getQuery().getEntity())
            .build();
        SketchShowResponse response = null;

        // send request
        response = (SketchShowResponse) CommUtility.send(
            MessageType.SKETCH_SHOW.getNumber(), request,
            nodeMetadata.getIpAddress(), nodeMetadata.getPort());

        // initialize replicas
        Map<Node, List<Node>> replicas = new HashMap();
        for (Replica replica : response.getReplicasList()) {
            if (replica.getPrimaryNodeId() == nodeId) {
                List<Node> nodes = new ArrayList();
                for (int secondaryNodeId :
                        replica.getSecondaryNodeIdsList()) {
                    NodeMetadata secondaryNodeMetadata =
                        this.nodeManager.get(secondaryNodeId);

                    nodes.add(secondaryNodeMetadata.toProtobuf());
                }

                NodeMetadata primaryNodeMetadata =
                    this.nodeManager.get(nodeId);
                replicas.put(primaryNodeMetadata.toProtobuf(), nodes);
            }
        }

        // initialize Inflator
        ClassLoader classLoader =
            Thread.currentThread().getContextClassLoader();
        Class c = classLoader.loadClass(response.getInflatorClass());
        Constructor constructor = c.getConstructor(List.class);
        Inflator inflator = (Inflator) constructor
            .newInstance(response.getVariablesList());

        /**
         * query sketch
         */
        ThreadedCursor cursor = new ThreadedCursor(replicas,
            inflator, file.getQuery(), 5000, 4);
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
                long blockId = ((long) inode << 32)
                    | ((long) nodeId << 16) | ((long) blockNum);
                byte[] block = byteOut.toByteArray();
                blockNum += 1;
                
                log.debug("generated block {} with length {}",
                    blockId, block.length);

                // add to blocks map
                blockMap.put(blockId, block.length);

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
            long blockId = ((long) inode << 32)
                | ((long) nodeId << 16) | ((long) blockNum);
            byte[] block = byteOut.toByteArray();

            log.debug("generated block {} with length {}",
                blockId, block.length);

            // add to blocks map
            blockMap.put(blockId, block.length);

            // TODO - writeLock
            this.blocks.put(blockId, block);
        }

        return blockMap;
    }

    /*public void initializeBlocks(long... blockIds) {
        // initialize blockIds structure for efficiency
        Map<Integer, Map<Integer, List<Long>>> inodeMap = new HashMap();
        for (long blockId : blockIds) {
            int inode = getInode(blockId);
            if (!inodeMap.containsKey(inode)) {
                inodeMap.put(inode, new HashMap());
            }
            Map<Integer, List<Long>> nodeIdMap = inodeMap.get(inode);

            int nodeId = getNodeId(blockId);
            if (!nodeIdMap.containsKey(nodeId)) {
                nodeIdMap.put(nodeId, new ArrayList());
            }
            List<Long> blockIdList = nodeIdMap.get(nodeId);

            blockIdList.add(blockId);
        }

        // query for blocks
        for (int inode : inodeMap.keySet()) {
            Map<Integer, List<Long>> nodeIdMap = inodeMap.get(inode);
            for (int nodeId : nodeIdMap.keySet()) {
                // TODO - query sketch

                List<Long> blockIdList = nodeIdMap.get(nodeId);
            }
        }
    }*/

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
