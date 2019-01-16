package com.bushpath.doodle.cli;

import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.protobuf.DoodleProtos.Failure;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryResponse;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ThreadedCursor {
    protected Map<Integer, List<Integer>> replicas;
    protected Map<Integer, Node> nodeLookup;
    protected Inflator inflator;
    protected ByteString queryByteString;
    protected int bufferSize;

    protected BlockingQueue<ByteString> in;
    protected BlockingQueue<float[]> out;
    protected Worker[] workers;

    public ThreadedCursor(Map<Integer, List<Integer>> replicas, 
            Map<Integer, Node> nodeLookup, Inflator inflator, 
            Query query, int bufferSize, int workerCount) 
            throws Exception {
        this.replicas = replicas;
        this.nodeLookup = nodeLookup;
        this.inflator = inflator;
        this.queryByteString = ByteString.copyFrom(query.toByteArray());
        this.bufferSize = bufferSize;

        // initialize instance variables
        this.in = new ArrayBlockingQueue(4096);
        this.out = new ArrayBlockingQueue(4096);
        this.workers = new Worker[workerCount];
        for (int i=0; i<workers.length; i++) {
            this.workers[i] = new Worker();
            this.workers[i].start();
        }

        Dispatcher dispatcher = new Dispatcher();
        dispatcher.start();
    }

    public float[] next() {
        float[] observation = null;
        while (true) {
            // retrieve observation from queue
            try {
                observation = this.out.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                System.err.println(e.getMessage());
            }

            if (observation != null) {
                return observation;
            }

            // check if any workers are alive
            boolean cont = false;
            for (Worker worker : this.workers) {
                if (worker.isAlive()) {
                    cont = true;
                    break;
                }
            }

            // if no workers alive -> return null
            if (!cont) {
                return null;
            }
        }
    }

    protected class Dispatcher extends Thread {
        @Override
        public void run() {
            // process all replicas
            for (Map.Entry<Integer, List<Integer>> replica :
                    replicas.entrySet()) {
                // attempt primary node
                Node node = null;
                try {
                    node = nodeLookup.get(replica.getKey());
                    this.submitQuery(replica.getKey(),
                        node.getIpAddress(), node.getPort());

                    continue; // success
                } catch (Exception e) {
                    System.err.println("failed to contact primary node "                        + node.getIpAddress() + ":" + node.getPort()
                        + " - " + e.getMessage());
                }

                // attempt secondary nodes
                for (Integer secondaryNodeId : replica.getValue()) {
                    try {
                        node = nodeLookup.get(secondaryNodeId);
                        this.submitQuery(replica.getKey(),
                            node.getIpAddress(), node.getPort());

                        break; // success
                    } catch (Exception e) {
                        System.err.println("failed to contact secondary node "                        + node.getIpAddress() + ":" + node.getPort()
                            + " - " + e.getMessage());
                    }
                }
            }

            // shutdown workers
            for (Worker worker : workers) {
                worker.shutdown = true;
            }
        }

        protected void submitQuery(int nodeId, String ipAddress,
                int port) throws Exception {
            // initialize QueryRequest
            QueryRequest queryRequest = QueryRequest.newBuilder()
                .setQuery(queryByteString)
                .setNodeId(nodeId)
                .setBufferSize(bufferSize)
                .build();

            // send request
            Socket socket = new Socket(ipAddress, port);

            DataOutputStream dataOut =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream dataIn = 
                new DataInputStream(socket.getInputStream());

            dataOut.writeInt(MessageType.QUERY.getNumber());
            queryRequest.writeDelimitedTo(dataOut);

            // recv response
            while (true) {
                int responseMessageType = dataIn.readInt();
                if (responseMessageType == 
                        MessageType.FAILURE.getNumber()) {
                    // recv failure -> throw exception
                    Failure failure = Failure.parseDelimitedFrom(dataIn);
                    throw new RuntimeException(failure.getText());
                } else if (responseMessageType !=
                        MessageType.QUERY.getNumber()) {
                    // recv unexpected response message type
                    throw new RuntimeException("Received unexpected "
                        + "message type '" + responseMessageType + "'");
                }

                // parse QueryResponse
                QueryResponse queryResponse =
                    QueryResponse.parseDelimitedFrom(dataIn);

                ByteString data = queryResponse.getData();
                if (data.size() != 0) {
                    while (!in.offer(data)) {}
                }
                
                // TODO - send Success response?

                // if last message then return
                if (queryResponse.getLastMessage()) {
                    break;
                }
            }
        }
    }

    protected class Worker extends Thread {
        public boolean shutdown;

        public Worker() {
            this.shutdown = false;
        }

        @Override
        public void run() {
            ByteString byteString = null;
            while (!in.isEmpty() || !this.shutdown) {
                // retrieve next byteString
                try {
                    byteString = in.poll(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }

                if (byteString == null) {
                    continue;
                }

                // process byteString
                List<float[]> observations = null;
                try {
                    ObjectInputStream objectIn =
                        new ObjectInputStream(byteString.newInput());

                    observations = inflator.process(objectIn);
                } catch (EOFException e) {
                    // processed all data
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("failed to inflate ByteString: "
                        + e.getMessage());
                    continue;
                }

                // put observations in out queue
                for (float[] observation : observations) {
                    while (!out.offer(observation)) {}
                }
            }
        }
    }
}
