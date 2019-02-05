package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(name = "init",
    description = "Initialize a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class SketchInitCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String id;

    @Parameters(index="1", description="SketchPlugin classpath.")
    private String plugin;

    @Parameters(index="2",
        description="ControlPlugin ID to link to SketchPlugin.")
    private String controlPlugin;

    @Option(names={"-r", "--replication-factor"},
        description="Number of sketch replicas in cluster [default: 3].")
    private int replicationFactor = 3;

    @Override
    public void run() {
        // create Operation
        Operation operation = Operation.newBuilder()
            .setTimestamp(System.nanoTime())
            .setOperationType(OperationType.INIT)
            .setPluginId(this.id)
            .setPluginType(PluginType.SKETCH)
            .setPluginClass(this.plugin)
            .setControlPluginId(this.controlPlugin)
            .setReplicationFactor(this.replicationFactor)
            .build();

        // create JournalOperationRequest
        JournalOperationRequest request =
            JournalOperationRequest.newBuilder()
                .addOperations(operation)
                .build();
        JournalOperationResponse response = null;

        // send request
        try {
            response = (JournalOperationResponse) CommUtility.send(
                MessageType.JOURNAL_OPERATION.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle JournalOperationResponse
    }
}
