package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Map;

@Command(name = "init",
    description = "Initialize a ControlPlugin.",
    mixinStandardHelpOptions = true)
public class ControlInitCli implements Runnable {
    @Parameters(index="0", description="Id of ControlPlugin instance.")
    private String id;

    @Parameters(index="1", description="ControlPlugin classpath.")
    private String plugin;

    @Override
    public void run() {
        // create Operation
        Operation operation = Operation.newBuilder()
            .setTimestamp(System.nanoTime())
            .setOperationType(OperationType.INIT)
            .setPluginId(this.id)
            .setPluginType(PluginType.CONTROL)
            .setPluginClass(this.plugin)
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
