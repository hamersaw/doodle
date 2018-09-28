package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchInitRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchInitResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "init",
    description = "Initialize a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class SketchInitCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String id;

    @Parameters(index="1", description="SketchPlugin classpath.")
    private String plugin;

    @Override
    public void run() {
        // create SketchInitRequest
        SketchInitRequest request = SketchInitRequest.newBuilder()
            .setId(this.id)
            .setPlugin(this.plugin)
            .build();
        SketchInitResponse response = null;

        // send request
        try {
            response = (SketchInitResponse) CommUtility.send(
                MessageType.SKETCH_INIT.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle SketchInitResponse
    }
}
