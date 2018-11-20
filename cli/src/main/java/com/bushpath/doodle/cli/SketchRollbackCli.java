package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointRollbackRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointRollbackResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

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
        // create CheckpointRollbackRequest
        CheckpointRollbackRequest request = CheckpointRollbackRequest.newBuilder()
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId)
            .build();
        CheckpointRollbackResponse response = null;

        // send request
        try {
            response = (CheckpointRollbackResponse) CommUtility.send(
                MessageType.CHECKPOINT_ROLLBACK.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle CheckpointRollbackResponse
    }
}
