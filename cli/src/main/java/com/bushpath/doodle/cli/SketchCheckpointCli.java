package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointCreateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointCreateResponse;

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
        // create CheckpointCreateRequest
        CheckpointCreateRequest request = CheckpointCreateRequest.newBuilder()
            .setSketchId(this.sketchId)
            .setCheckpointId(this.checkpointId)
            .build();
        CheckpointCreateResponse response = null;

        // send request
        try {
            response = (CheckpointCreateResponse) CommUtility.send(
                MessageType.CHECKPOINT_CREATE.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle CheckpointCreateResponse
    }
}
