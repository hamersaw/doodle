package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryProfileRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.QueryProfileResponse;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.parser.FeatureRangeParser;
import com.bushpath.rutils.query.parser.Parser;

import com.google.protobuf.ByteString;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "query",
    description = "Query a sketch for data.",
    mixinStandardHelpOptions = true)
public class ProfileQueryCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String sketchId;

    @Parameters(index="1", description="Node ID of data to query.")
    private int nodeId;

    @Option(names = {"-q", "--query"},
        description = "Feature range query (eq. 'f0:0..10', 'f1:0..', 'f2:..10').")
    private String[] queries;

    @Override
    public void run() {
        // parse query
        ByteString byteString = null;
        try {
            Parser parser = new FeatureRangeParser();
            Query query = parser.evaluate(queries);
            query.setEntity(this.sketchId);

            byteString = ByteString.copyFrom(query.toByteArray());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // initialize QueryProfileRequest
        QueryProfileRequest qpRequest =
            QueryProfileRequest.newBuilder()
                .setQuery(byteString)
                .setNodeId(this.nodeId)
                .build();
        QueryProfileResponse qpResponse = null;

        // send request
        try {
            qpResponse = (QueryProfileResponse) CommUtility.send(
                MessageType.QUERY_PROFILE.getNumber(),
                qpRequest, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle QueryProfileResponse
        System.out.println("observationCount: "
                + qpResponse.getObservationCount()
            + "\nexecutionTime: "
                + qpResponse.getExecutionTimeMilliSeconds() + "ms");
    }
}
