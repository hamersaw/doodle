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

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.Random;

@Command(name = "write",
    description = "Write data to a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class SketchWriteCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String id;

    @Option(names={"-p", "--pipe-count"}, description="")
    private int pipeCount = 5;

    @Option(names={"-s", "--buffer-size"}, description="")
    private int bufferSize = 2000;

    @Option(names={"-t", "--transform-thread-count"}, description="")
    private int transformThreadCount = 3;

    @Option(names={"-d", "--distributor-thread-count"}, description="")
    private int distributorThreadCount = 3;

    @Override
    public void run() {
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
            .setSketchId(this.id)
            .setTransformThreadCount(this.transformThreadCount)
            .setDistributorThreadCount(this.distributorThreadCount);

        // TODO - add features to pipeOpenBuilder
 
        PipeOpenRequest pipeOpenRequest = pipeOpenBuilder.build();
        PipeOpenResponse pipeOpenResponse = null;
        Integer[] pipeIds = new Integer[nodes.size()];
        for (int i=0; i<nodes.size(); i++) {
            Node node = nodes.get(i);

            // send NodeListRequest
            try {
                pipeOpenResponse = (PipeOpenResponse) CommUtility.send(
                    MessageType.PIPE_OPEN.getNumber(),
                    pipeOpenRequest, node.getIpAddress(), (short) node.getPort());

                pipeIds[i] = pipeOpenResponse.getId();
            } catch (Exception e) {
                pipeIds[i] = null;
                System.err.println(e.getMessage());
            }
        }

        // TODO - check if any pipe ids initialized valid

        // TODO - send PipeWriteRequests

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
