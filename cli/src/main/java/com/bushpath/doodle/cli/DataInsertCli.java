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

import com.bushpath.rutils.reader.BinaryReader;
import com.bushpath.rutils.reader.Reader;
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

    @Parameters(index="1..*", description="File(s) to load.")
    private String[] filenames;

    @Option(names={"-p", "--pipe-count"},
        description="The nubmer of nodes to initialize pipes at [default=5].")
    private int pipeCount = 5;

    @Option(names={"-t", "--transform-thread-count"},
        description="Number of transform threads at each node [default=3].")
    private int transformThreadCount = 3;

    @Option(names={"-d", "--distributor-thread-count"},
        description="Number of distributor threads at each node [default=3].")
    private int distributorThreadCount = 3;

    @Option(names={"-b", "--journal-write-buffer-size"},
        description="Size of JournalWriteRequest data (in bytes) [default=5000].")
    private int journalWriteBufferSize = 5000;

    @Option(names={"-s", "--pipe-write-buffer-size"},
        description="Size of PipeWriteRequest data (in bytes) [default=5000].")
    private int pipeWriteBufferSize = 5000;

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        long recordCount = 0;

        /**
         * open file for writing
         */

        Reader<float[]> reader = null;
        try {
            reader = this.openReader(this.filenames[0]);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        /**
         * get random nodes
         */

        // create NodeListRequest
        NodeListRequest nodeListRequest =
            NodeListRequest.newBuilder().build();
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
            .setBufferSize(this.journalWriteBufferSize);

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
        float[] record = null;
        int pipeIndex = 0;
        int fileIndex = 1;
        try {
            while (true) {
                while ((record = reader.next()) == null) {
                    reader.close();
                    if (fileIndex == this.filenames.length) {
                        break;
                    } 

                    reader = this.openReader(this.filenames[fileIndex]);
                    // TODO - check if header is still correct
                    fileIndex += 1;
                }

                if (record == null) {
                    break;
                }

                // write record to out
                for (int i=0; i<featureIndexes.length; i++) {
                    out.writeFloat(record[featureIndexes[i]]);
                }

                recordCount += 1;

                if (byteOut.size() >= this.pipeWriteBufferSize) {
                    // close DataOutputStream
                    out. close();

                    // find value node to write to
                    while (pipeIds[pipeIndex] == null) {
                        pipeIndex = (pipeIndex + 1) % pipeIds.length;
                    }

                    // create PipeWriteRequest
                    PipeWriteRequest pipeWriteRequest =
                        PipeWriteRequest.newBuilder()
                        .setId(pipeIds[pipeIndex])
                        .setData(byteOut.toByteString())
                        .build();
     
                    // write to node
                    PipeWriteResponse pipeWriteResponse = null;
                    Node node = nodes.get(pipeIndex);
                    pipeWriteResponse = (PipeWriteResponse) CommUtility.send(
                        MessageType.PIPE_WRITE.getNumber(),
                        pipeWriteRequest, node.getIpAddress(),
                        (short) node.getPort());

                    // update variables
                    pipeIndex = (pipeIndex + 1) % pipeIds.length;
                    byteOut = ByteString.newOutput();
                    out = new DataOutputStream(byteOut);
                }
            }
 
            // close DataOutputStream
            out. close();

            // if byteOut.size() > 0 the nwrite
            if (byteOut.size() > 0) {
                // find value node to write to
                while (pipeIds[pipeIndex] == null) {
                    pipeIndex = (pipeIndex + 1) % pipeIds.length;
                }

                // create PipeWriteRequest
                PipeWriteRequest pipeWriteRequest =
                    PipeWriteRequest.newBuilder()
                    .setId(pipeIds[pipeIndex])
                    .setData(byteOut.toByteString())
                    .build();
 
                // write to node
                PipeWriteResponse pipeWriteResponse = null;
                Node node = nodes.get(pipeIndex);
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

        long time = System.currentTimeMillis() - startTime;
        System.out.println("Wrote " + recordCount + " record(s) in "
            + time + "ms (" + (recordCount / time * 1000)
            + " records/sec)");
    }

    protected Reader<float[]> openReader(String filename)
            throws Exception {
        if (filename.endsWith(".csv")) {
            return new ThreadedCsvReader(filename, 4);
        } else if (filename.endsWith(".bin")) {
            return new BinaryReader(filename);
        } else {
            throw new RuntimeException("Unsupported file format ''"
                + filename);
       }
    }
}
