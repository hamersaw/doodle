package com.bushpath.doodle.cli;

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

        try {
            // send request
            Socket socket = new Socket(Main.ipAddress, Main.port);

            DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeInt(MessageType.CONTROL_INIT.getNumber());
            request.writeDelimitedTo(out);

            // recv response
            // TODO - validate we have the correct message type
            in.readInt();
            response = ControlInitResponse.parseDelimitedFrom(in);
        } catch (IOException e) {
            System.err.println("Unknown communication error: " +
                e.getClass() + ":" + e.getMessage());
            return;
        }

        // handle ControlInitResponse
    }
}
