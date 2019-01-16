package com.bushpath.doodle.node.filesystem;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.checksum.Checksum;
import com.bushpath.anamnesis.checksum.ChecksumFactory;
import com.bushpath.anamnesis.ipc.datatransfer.BlockOutputStream;
import com.bushpath.anamnesis.ipc.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.ipc.datatransfer.Op;

import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.Poison;
import com.bushpath.doodle.SketchPlugin;

import com.bushpath.rutils.query.Query;

import com.bushpath.doodle.node.sketch.SketchManager;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;

public class DataTransferService extends Thread {
    protected ServerSocket serverSocket;
    protected ExecutorService executorService;
    protected FileManager fileManager;
    protected SketchManager sketchManager;
    protected Map<Long, byte[]> blocks;

    public DataTransferService(ServerSocket serverSocket,
            ExecutorService executorService, FileManager fileManager,
            SketchManager sketchManager) {
        this.serverSocket = serverSocket;
        this.executorService = executorService;
        this.fileManager = fileManager;
        this.sketchManager = sketchManager;
        this.blocks = new HashMap();
    }

    @Override
    public void run() {
        try {
            while (true) {
                Socket socket = this.serverSocket.accept();
                this.executorService.execute(new Worker(socket));
            }
        } catch (Exception e) {
            // TODO - handle exception
            e.printStackTrace();
        }
    }

    protected byte[] initBlock(DoodleInode inode,
            int nodeId, short blockNum) throws Exception {
        // retrieve sketch
        DoodleFile file = (DoodleFile) inode.getEntry();
        Query query = file.getQuery();
        SketchPlugin sketch = this.sketchManager.get(query.getEntity());

        // retrieve block sizes for this nodeId
        List<Long> blockSizes = new ArrayList();
        for (Map.Entry<Long, Long> entry :
                BlockUtil.getBlockSizes(inode).entrySet()) {
            int entryNodeId = BlockUtil.getNodeId(entry.getKey());
            if (entryNodeId == nodeId) {
                blockSizes.add(entry.getValue());
            }
        }

        // initialize instance variables
        ClassLoader classLoader =
            Thread.currentThread().getContextClassLoader();
        Class c = classLoader.loadClass(sketch.getInflatorClass());
        Constructor constructor = c.getConstructor(List.class);
        Inflator inflator = (Inflator) constructor
            .newInstance(sketch.getVariables());

        BlockingQueue<Serializable> queue = sketch.query(null, query);

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);

        Format format = file.getFormat();
        int featureCount = sketch.getFeatures().size();
        long recordSize = format.length(featureCount, 1);

        // generate data
        int currentBlockNum = 0;
        long blockSize = 0;
        long currentBlockSize = blockSizes.get(currentBlockNum);
        Serializable s = null;
        while (true) {
            // retrieve next "bytes" from queue
            try {
                s = queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                System.err.println("failed to poll queue: " + e);
            }

            if (s instanceof Exception) {
                throw (Exception) s;
            } else if (s instanceof Poison) {
                break;
            } else if (s == null) {
                continue;
            }

            // handle serializable
            List<float[]> observations = new ArrayList();
            inflator.process(s, observations);
            for (float[] observation : observations) {
                // check if we need to increment blockNum
                if (blockSize == currentBlockSize) {
                    blockSize = 0;
                    currentBlockNum += 1;
                    currentBlockSize = blockSizes.get(currentBlockNum);
                }

                // if this block write data to byteOut
                if (currentBlockNum == blockNum) {
                    format.format(observation, dataOut);
                }

                blockSize += recordSize;
            }
        }

        dataOut.close();
        byteOut.close();
        return byteOut.toByteArray();
    }

    protected class Worker implements Runnable {
        protected Socket socket;

        public Worker(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                DataInputStream in =
                    new DataInputStream(this.socket.getInputStream());
                DataOutputStream out =
                    new DataOutputStream(this.socket.getOutputStream());
  
                while (true) {
                    // read operation
                    Op op = DataTransferProtocol.readOp(in);

                    switch(op) {
                    case READ_BLOCK:
                        DataTransferProtos.OpReadBlockProto
                            readBlockProto =
                                DataTransferProtocol.recvReadOp(in);

                        HdfsProtos.ExtendedBlockProto
                            readExtendedBlockProto =
                                readBlockProto.getHeader()
                                    .getBaseHeader().getBlock();

                        // parse blockId
                        long blockId =
                            readExtendedBlockProto.getBlockId();
                        int inodeValue = BlockUtil.getInode(blockId);
                        int nodeId = BlockUtil.getNodeId(blockId);
                        short blockNum = BlockUtil.getBlockNum(blockId);

                        /*System.out.println("block:" + blockId +
                            "\n\tinode:" + inodeValue +
                            "\n\tnodeId:" + nodeId +
                            "\n\tblockNum:" + blockNum +
                            "\n\toffset:" + readBlockProto.getOffset() +
                            "\n\tlength:" + readBlockProto.getLen());*/

                        // retrieve block
                        byte[] block = null;
                        if (blocks.containsKey(blockId)) {
                            block = blocks.get(blockId);
                        } else {
                            // initialize block
                            DoodleInode inode =
                                fileManager.getInode(inodeValue);
                            block = initBlock(inode, nodeId, blockNum);
                            blocks.put(blockId, block);

                            System.out.println("generated block "
                                + blockId + " with size "
                                + block.length);
                        }

                        // send op response
                        DataTransferProtocol.sendBlockOpResponse(out,
                            DataTransferProtos.Status.SUCCESS,
                            HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C,
                            DataTransferProtocol.CHUNK_SIZE,
                            readBlockProto.getOffset());
        
                        // create checksum
                        Checksum readChecksum =
                            ChecksumFactory.buildDefaultChecksum();
      
                        // send stream block chunks
                        BlockOutputStream blockOut =
                            new BlockOutputStream(in, out,
                            readChecksum,
                            readBlockProto.getOffset() + 1);
                        blockOut.write(block, 
                            (int) readBlockProto.getOffset(),
                            (int) readBlockProto.getLen());
                        blockOut.close();

                        // TODO - read client read status proto
                        DataTransferProtos.ClientReadStatusProto
                            readProto = DataTransferProtocol
                                .recvClientReadStatus(in);
                        break;
                    default:
                        break;
                    }
                }
            } catch (EOFException | SocketException e) {
                // socket was closed by client
            } catch (Exception e) {
                // TODO - handle exception
                e.printStackTrace();
            }
        }
    }
}
