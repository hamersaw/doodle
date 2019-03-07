package com.bushpath.doodle.dfs;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.file.FileManager;

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
    protected static final Logger log =
        LoggerFactory.getLogger(DataTransferService.class);

    protected ServerSocket serverSocket;
    protected ExecutorService executorService;
    protected BlockManager blockManager;

    public DataTransferService(ServerSocket serverSocket,
            ExecutorService executorService,
            BlockManager blockManager) {
        this.serverSocket = serverSocket;
        this.executorService = executorService;
        this.blockManager = blockManager;
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

                        // retreive block
                        long blockId =
                            readExtendedBlockProto.getBlockId();
                        byte[] block = blockManager.getBlock(blockId);

                        log.debug("Recv READ_BLOCK op for blockId:{}"
                            + " offset:{} length:{}", blockId,
                            readBlockProto.getOffset(),
                            readBlockProto.getLen());

                        /*int inodeValue = BlockUtil.getInode(blockId);
                        int nodeId = BlockUtil.getNodeId(blockId);
                        short blockNum = BlockUtil.getBlockNum(blockId);

                        log.debug("Recv READ_BLOCK op for blockId:{}"
                            + " offset:{} length:{}", blockId,
                            readBlockProto.getOffset(),
                            readBlockProto.getLen());

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

                            log.info("Initialized blockId:{}"
                                + " with length:{}", blockId,
                                block.length);
                        }*/

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
