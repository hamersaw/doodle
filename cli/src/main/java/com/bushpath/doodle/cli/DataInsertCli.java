package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteResponse;

import com.bushpath.rutils.reader.ThreadedCsvReader;

import com.google.protobuf.ByteString;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.DataOutputStream;
import java.util.List;
import java.util.Random;

@Command(name = "insert",
    description = "Insert data to a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class DataInsertCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String sketchId;

    @Parameters(index="1", description="CSV file to load.")
    private String filename;

    @Option(names={"-p", "--pipe-count"},
        description="The nubmer of nodes to initialize pipes at [default=5].")
    private int pipeCount = 5;

    @Option(names={"-t", "--transform-thread-count"},
        description="Number of transform threads at each node [default=3].")
    private int transformThreadCount = 3;

    @Option(names={"-d", "--distributor-thread-count"},
        description="Number of distributor threads at each node [default=3].")
    private int distributorThreadCount = 3;

    @Option(names={"-b", "--sketch-write-buffer-size"},
        description="Size of SketchWriteRequest data (in bytes) [default=2000].")
    private int sketchWriteBufferSize = 2000;

    @Option(names={"-s", "--pipe-write-buffer-size"},
        description="Size of PipeWriteRequest data (in bytes) [default=2000].")
    private int pipeWriteBufferSize = 2000;

    @Override
    public void run() {
        /**
         * open file for writing
         */

        ThreadedCsvReader reader = null;
        try {
            reader = new ThreadedCsvReader(this.filename, 4);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        /**
         * get random nodes
         */

        // create NodeListRequest
        NodeListRequest nodeListRequest = NodeListRequest.newBuilder().build();
        NodeListResponse nodeListResponse = null;

        // send NodeListRequest
        try {
            nodeListResponse = (NodeListResponse) CommUtility.send(
                MessageType.NODE_LIST.getNumber(),
                nodeListRequest, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        List<Node> nodes = nodeListResponse.getNodesList();
        Random random = new Random(System.currentTimeMillis());
        while (nodes.size() > this.pipeCount) {
            nodes.remove(random.nextInt(nodes.size()));
        }
 
        /**
         * open pipes
         */

        // create PipeOpenRequest
        PipeOpenRequest.Builder pipeOpenBuilder = PipeOpenRequest.newBuilder()
            .setSketchId(this.sketchId)
            .setTransformThreadCount(this.transformThreadCount)
            .setDistributorThreadCount(this.distributorThreadCount)
            .setBufferSize(this.sketchWriteBufferSize);

        // add features to pipeOpenBuilder
        for (String feature : reader.getHeader()) {
            pipeOpenBuilder.addFeatures(feature);
        }
 
        PipeOpenRequest pipeOpenRequest = pipeOpenBuilder.build();
        PipeOpenResponse pipeOpenResponse = null;
        Integer[] pipeIds = new Integer[nodes.size()];
        int[] featureIndexes = null;
        for (int i=0; i<nodes.size(); i++) {
            Node node = nodes.get(i);

            // send NodeListRequest
            try {
                pipeOpenResponse = (PipeOpenResponse) CommUtility.send(
                    MessageType.PIPE_OPEN.getNumber(),
                    pipeOpenRequest, node.getIpAddress(), (short) node.getPort());

                pipeIds[i] = pipeOpenResponse.getId();

                if (featureIndexes == null ) {
                    List<Integer> pipeFeatureIndexes =
                        pipeOpenResponse.getFeatureIndexesList();
                    featureIndexes = new int[pipeFeatureIndexes.size()];
                    for (int j=0; j<featureIndexes.length; j++) {
                        featureIndexes[j] = pipeFeatureIndexes.get(j);
                    }
                } else {
                    // TODO - check if featureIndexes have changed
                }
            } catch (Exception e) {
                pipeIds[i] = null;
                System.err.println(e.getMessage());
            }
        }

        // TODO - check if any pipe ids initialized valid

        // send PipeWriteRequests
        ByteString.Output byteOut = ByteString.newOutput();
        DataOutputStream out = new DataOutputStream(byteOut);
        double[] record = null;
        int index = 0;
        try {
            while ((record = reader.next()) != null) {
                // write record to out
                for (int i=0; i<featureIndexes.length; i++) {
                    out.writeFloat((float) record[featureIndexes[i]]);
                }

                if (byteOut.size() >= this.pipeWriteBufferSize) {
                    // close DataOutputStream
                    out. close();

                    // find value node to write to
                    while (pipeIds[index] == null) {
                        index = (index + 1) % pipeIds.length;
                    }

                    // create PipeWriteRequest
                    PipeWriteRequest pipeWriteRequest =
                        PipeWriteRequest.newBuilder()
                        .setId(pipeIds[index])
                        .setData(byteOut.toByteString())
                        .build();
     
                    // write to node
                    PipeWriteResponse pipeWriteResponse = null;
                    Node node = nodes.get(index);
                    pipeWriteResponse = (PipeWriteResponse) CommUtility.send(
                        MessageType.PIPE_WRITE.getNumber(),
                        pipeWriteRequest, node.getIpAddress(),
                        (short) node.getPort());

                    // update variables
                    index = (index + 1) % pipeIds.length;
                    byteOut = ByteString.newOutput();
                    out = new DataOutputStream(byteOut);
                }
            }
 
            // close DataOutputStream
            out. close();

            // if byteOut.size() > 0 the nwrite
            if (byteOut.size() > 0) {
                // find value node to write to
                while (pipeIds[index] == null) {
                    index = (index + 1) % pipeIds.length;
                }

                // create PipeWriteRequest
                PipeWriteRequest pipeWriteRequest =
                    PipeWriteRequest.newBuilder()
                    .setId(pipeIds[index])
                    .setData(byteOut.toByteString())
                    .build();
 
                // write to node
                PipeWriteResponse pipeWriteResponse = null;
                Node node = nodes.get(index);
                pipeWriteResponse = (PipeWriteResponse) CommUtility.send(
                    MessageType.PIPE_WRITE.getNumber(),
                    pipeWriteRequest, node.getIpAddress(),
                    (short) node.getPort());
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        /**
         * close pipes
         */
        for (int i=0; i<nodes.size(); i++) {
            // check if pipe was opened
            if (pipeIds[i] == null) {
                continue;
            }

            // create PipeCloseRequest
            PipeCloseRequest pipeCloseRequest = PipeCloseRequest.newBuilder()
                .setId(pipeIds[i])
                .build();
 
            // send PipeCloseRequest
            PipeCloseResponse pipeCloseResponse = null;
            Node node = nodes.get(i);
            try {
                pipeCloseResponse = (PipeCloseResponse) CommUtility.send(
                    MessageType.PIPE_CLOSE.getNumber(),
                    pipeCloseRequest, node.getIpAddress(), (short) node.getPort());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
