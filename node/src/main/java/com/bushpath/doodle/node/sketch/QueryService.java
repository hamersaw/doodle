package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryResponse;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueryService implements Service {
    protected static final Logger log =
        LoggerFactory.getLogger(QueryService.class);

    protected SketchPluginManager sketchPluginManager;

    public QueryService(SketchPluginManager sketchPluginManager) {
        this.sketchPluginManager = sketchPluginManager;
    }

    @Override
    public int[] getMessageTypes() {
        return new int[]{
                MessageType.QUERY.getNumber()
            };
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {

        // handle message
        try {
            switch (MessageType.forNumber(messageType)) {
                case QUERY:
                    // parse request
                    QueryRequest queryRequest =
                        QueryRequest.parseDelimitedFrom(in);

                    ByteString data = queryRequest.getQuery();
                    ObjectInputStream objectIn =
                        new ObjectInputStream(data.newInput());
                    Query query = (Query) objectIn.readObject();
                    objectIn.close();

                    log.info("handling QueryRequest {}",
                        query.getEntity());

                    // start ResponseHandler
                    BlockingQueue<byte[]> queue =
                        new ArrayBlockingQueue(2048);

                    ResponseHandler responseHandler = 
                        new ResponseHandler(queue, out, 
                            queryRequest.getBufferSize());
                    responseHandler.start();

                    // TODO - handle

                    responseHandler.shutdown(); 
                    responseHandler.join();
                    break;
                default:
                    log.warn("Unreachable");
            }
        } catch (Exception e) {
            log.warn("Handling exception", e);

            // create Failure
            Failure.Builder builder = Failure.newBuilder()
                .setType(e.getClass().getName());

            if (e.getMessage() != null) {
                builder.setText(e.getMessage());
            }

            // write to out
            out.writeInt(MessageType.FAILURE.getNumber());
            builder.build().writeDelimitedTo(out);
        }
    }

    protected class ResponseHandler extends Thread {
        protected BlockingQueue<byte[]> in;
        protected DataOutputStream out;
        protected int bufferSize;
        protected ByteString.Output byteString;
        protected boolean shutdown;

        public ResponseHandler(BlockingQueue<byte[]> in,
                DataOutputStream out, int bufferSize) {
            this.in = in;
            this.out = out;
            this.bufferSize = bufferSize;
            this.byteString = ByteString.newOutput();
            this.shutdown = false;
        }

        @Override
        public void run() {
            byte[] bytes = null;
            while (!this.in.isEmpty() || !this.shutdown) {
                // retrieve next "bytes" from queue
                try {
                    bytes = this.in.poll(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("failed to poll queue", e);
                }

                if (bytes == null) {
                    continue;
                }

                // write bytes to ByteString
                this.byteString.write(bytes, 0, bytes.length);

                if (this.byteString.size() >= this.bufferSize) {
                    // write QueryResponse
                    QueryResponse queryResponse = 
                        QueryResponse.newBuilder()
                            .setData(this.byteString.toByteString())
                            .setLastMessage(false)
                            .build();

                    try {
                        out.writeInt(MessageType.QUERY.getNumber());
                        queryResponse.writeDelimitedTo(out);
                    } catch (IOException e) {
                        log.error("failed to write QueryResponse", e);
                    }

                    this.byteString = ByteString.newOutput();
                }
            }

            // write QueryResponse
            QueryResponse queryResponse = 
                QueryResponse.newBuilder()
                    .setData(this.byteString.toByteString())
                    .setLastMessage(true)
                    .build();

            try {
                out.writeInt(MessageType.QUERY.getNumber());
                queryResponse.writeDelimitedTo(out);
            } catch (IOException e) {
                log.error("failed to write QueryResponse", e);
            }
        }

        public void shutdown() {
            this.shutdown = true;
        }
    }
}
