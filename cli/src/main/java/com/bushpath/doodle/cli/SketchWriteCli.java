package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeCloseResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeOpenResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PipeWriteResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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
        // TODO - chose random nodes
 
        // create PipeOpenRequest
        PipeOpenRequest.Builder pipeOpenBuilder = PipeOpenRequest.newBuilder()
            .setSketchId(this.id)
            .setTransformThreadCount(this.transformThreadCount)
            .setDistributorThreadCount(this.distributorThreadCount);

        // TODO - add features to pipeOpenBuilder
 
        PipeOpenRequest pipeOpenRequest = pipeOpenBuilder.build();

        // TODO - send PipeWriteRequests

        // TODO - create PipeCloseRequests
    }
}
