package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchCheckpointRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchCheckpointResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(name = "checkpoint",
    description = "Create a checkpoint for a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class SketchCheckpointCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String sketchId;

    @Parameters(index="1", description="Id of checkpoint.")
    private String checkpointId;

    @Override
    public void run() {
        // create SketchCheckpointRequest
        SketchCheckpointRequest request = SketchCheckpointRequest.newBuilder()
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId)
            .build();
        SketchCheckpointResponse response = null;

        // send request
        try {
            response = (SketchCheckpointResponse) CommUtility.send(
                MessageType.SKETCH_CHECKPOINT.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle SketchCheckpointResponse
    }
}
