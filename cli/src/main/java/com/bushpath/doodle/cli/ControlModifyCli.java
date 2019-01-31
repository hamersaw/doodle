package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginType;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(name = "modify",
    description = "Modifyialize a ControlPlugin.",
    mixinStandardHelpOptions = true)
public class ControlModifyCli implements Runnable {
    @Parameters(index="0", description="Id of ControlPlugin instance.")
    private String id;

    @Option(names={"-a", "--add"},
        description="Add variable [format: \"type:name:v1,v2,..\"].")
    private List<String> addVariables = new ArrayList();

    @Option(names={"-d", "--delete"},
        description="Delete variable [format: \"type:name:v1,v2,..\"].")
    private List<String> deleteVariables = new ArrayList();

    @Override
    public void run() {
        JournalOperationRequest.Builder builder =
            JournalOperationRequest.newBuilder();

        // parse variables into VariableOperations
        try {
            for (String variable : this.addVariables) {
                builder.addOperations(
                    Operation.newBuilder()
                        .setTimestamp(System.nanoTime())
                        .setOperationType(OperationType.ADD)
                        .setPluginId(id)
                        .setPluginType(PluginType.CONTROL)
                        .setVariable(
                            this.parseArgument(variable))
                        .build());
            }
        } catch (Exception e) {
            System.err.println(e);
            return;
        }

        try {
            for (String variable : this.deleteVariables) {
                builder.addOperations(
                    Operation.newBuilder()
                        .setTimestamp(System.nanoTime())
                        .setOperationType(OperationType.DELETE)
                        .setPluginId(id)
                        .setPluginType(PluginType.CONTROL)
                        .setVariable(
                            this.parseArgument(variable))
                        .build());
            }
        } catch (Exception e) {
            System.err.println(e);
            return;
        }

        JournalOperationRequest request = builder.build();
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

    protected Variable parseArgument(String arg) throws Exception {
        String[] fields = arg.split(":");
        if (fields.length != 3) {
            throw new RuntimeException("Failed to parse variable fields '" +
                arg + "'");
        }

        String[] values = fields[2].split(",");
        if (values.length < 1) {
            throw new RuntimeException("Failed to parse variable values '" +
                fields[2] + "'");
        }

        Variable.Builder builder = Variable.newBuilder()
            .setType(fields[0])
            .setName(fields[1]);

        for (String value : values) {
            builder.addValues(value);
        }

        return builder.build();
    }
}
