package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointRollbackRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.CheckpointRollbackResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;

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
        // create NodeListRequest
        NodeListRequest nlRequest = NodeListRequest.newBuilder()
            .build();
        NodeListResponse nlResponse = null;

        // send request
        try {
            nlResponse = (NodeListResponse) CommUtility.send(
                MessageType.NODE_LIST.getNumber(),
                nlRequest, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }
 
        // create CheckpointRollbackRequest
        CheckpointRollbackRequest crRequest =
            CheckpointRollbackRequest.newBuilder()
                .setSketchId(this.sketchId)
                .setCheckpointId(this.checkpointId)
                .build();
        CheckpointRollbackResponse crResponse = null;

        // handle NodeListResponse
        for (Node node : nlResponse.getNodesList()) {
            // send request
            try {
                crResponse = (CheckpointRollbackResponse) CommUtility.send(
                    MessageType.CHECKPOINT_ROLLBACK.getNumber(),
                    crRequest, node.getIpAddress(), (short) node.getPort());
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return;
            }

            // TODO - handle CheckpointRollbackResponse
        }
    }
}
