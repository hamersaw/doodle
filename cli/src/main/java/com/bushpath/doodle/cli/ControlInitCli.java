package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlInitRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlInitResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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
        // create ControlInitRequest
        ControlInitRequest request = ControlInitRequest.newBuilder()
            .setId(this.id)
            .setPlugin(this.plugin)
            .build();
        ControlInitResponse response = null;

        // send request
        try {
            response = (ControlInitResponse) CommUtility.send(
                MessageType.CONTROL_INIT.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle ControlInitResponse
    }
}
