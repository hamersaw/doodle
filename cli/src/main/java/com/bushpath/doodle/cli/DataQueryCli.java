package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.parser.FeatureRangeParser;
import com.bushpath.rutils.query.parser.Parser;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.Map;

@Command(name = "query",
    description = "Query a sketch for data.",
    mixinStandardHelpOptions = true)
public class DataQueryCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String sketchId;

    @Option(names={"-b", "--buffer-size"},
        description="Size of buffer data (in bytes) [default=2000].")
    private int bufferSize = 2000;

    @Option(names = {"-q", "--query"},
        description = "Feature range query (eq. 'f0:0..10', 'f1:0..', 'f2:..10').")
    private String[] queries;

    @Option(names={"-w", "--worker-count"},
        description="Number of workers in ThreadedCursor [default=8].")
    private int workerCount = 8;

    @Override
    public void run() {
        // parse query
        Query query = null;
        try {
            Parser parser = new FeatureRangeParser();
            query = parser.evaluate(queries);
            query.setEntity(this.sketchId);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // create NodeListRequest
        NodeListRequest request = NodeListRequest.newBuilder()
            .build();
        NodeListResponse response = null;

        // send request
        try {
            response = (NodeListResponse) CommUtility.send(
                MessageType.NODE_LIST.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        try {
            // initialize ThreadedCursor
            ThreadedCursor cursor = new ThreadedCursor(query,
                this.bufferSize, response.getNodesList(), 
                this.workerCount);

            // iterate over observations
            float[] observation = null;
            while ((observation = cursor.next()) != null) {
                for (int i=0; i<observation.length; i++) {
                    System.out.print((i == 0 ? "" : ",")
                        + observation[i]);
                }
                System.out.println("");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }
    }
}
