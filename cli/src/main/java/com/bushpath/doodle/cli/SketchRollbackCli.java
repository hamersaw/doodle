package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchRollbackRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchRollbackResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(name = "rollback",
    description = "Rollback a SketchPlugin to a specific checkpoint.",
    mixinStandardHelpOptions = true)
public class SketchRollbackCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String sketchId;

    @Parameters(index="1", description="Id of checkpoint.")
    private String checkpointId;

    @Override
    public void run() {
        // create SketchRollbackRequest
        SketchRollbackRequest request = SketchRollbackRequest.newBuilder()
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId)
            .build();
        SketchRollbackResponse response = null;

        // send request
        try {
            response = (SketchRollbackResponse) CommUtility.send(
                MessageType.SKETCH_ROLLBACK.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle SketchRollbackResponse
    }
}
