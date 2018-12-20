package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchModifyRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchModifyResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;
import com.bushpath.doodle.protobuf.DoodleProtos.VariableOperation;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(name = "modify",
    description = "Add/delete variables from a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class SketchModifyCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String id;

    @Option(names={"-a", "--add"},
        description="Add variable [format: \"type:name:v1,v2,..\"].")
    private List<String> addVariables = new ArrayList();

    @Option(names={"-d", "--delete"},
        description="Delete variable [format: \"type:name:v1,v2,..\"].")
    private List<String> deleteVariables = new ArrayList();

    @Override
    public void run() {
        // create SketchModifyRequest
        SketchModifyRequest.Builder builder = SketchModifyRequest.newBuilder()
            .setId(this.id);

        // parse variables into VariableOperations
        try {
            for (String variable : this.addVariables) {
                builder.addOperations(
                    this.parseArgument(variable, Operation.ADD));
            }
        } catch (Exception e) {
            System.err.println(e);
            return;
        }

        try {
            for (String variable : this.deleteVariables) {
                builder.addOperations(
                    this.parseArgument(variable, Operation.DELETE));
            }
        } catch (Exception e) {
            System.err.println(e);
            return;
        }

        SketchModifyRequest request = builder.build();
        SketchModifyResponse response = null;

        // send request
        try {
            response = (SketchModifyResponse) CommUtility.send(
                MessageType.SKETCH_MODIFY.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle SketchModifyResponse
    }

    protected VariableOperation parseArgument(String arg,
            Operation operation) throws Exception {
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

        return VariableOperation.newBuilder()
            .setTimestamp(System.nanoTime())
            .setOperation(operation)
            .setVariable(builder.build())
            .build();
    }
}
