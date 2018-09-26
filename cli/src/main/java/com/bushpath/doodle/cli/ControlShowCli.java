package com.bushpath.doodle.cli;

import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginVariable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

@Command(name = "show",
    description = "Show a ControlPlugin.",
    mixinStandardHelpOptions = true)
public class ControlShowCli implements Runnable {
    @Parameters(index="0", description="Id of ControlPlugin instance.")
    private String id;

    @Override
    public void run() {
        // create ControlShowRequest
        ControlShowRequest request = ControlShowRequest.newBuilder()
            .setId(this.id)
            .build();
        ControlShowResponse response = null;

        try {
            // send request
            Socket socket = new Socket(Main.ipAddress, Main.port);

            DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeInt(MessageType.CONTROL_SHOW.getNumber());
            request.writeDelimitedTo(out);

            // recv response
            // TODO - validate we have the correct message type
            in.readInt();
            response = ControlShowResponse.parseDelimitedFrom(in);
        } catch (IOException e) {
            System.err.println("Unknown communication error: " +
                e.getClass() + ":" + e.getMessage());
            return;
        }

        // handle ControlShowResponse
        System.out.println(this.id + "\n" + response.getPlugin());
        for (PluginVariable variable : response.getVariablesList()) {
            System.out.print("\t" + variable.getType()
                + ":" + variable.getName() + "\n\t\t[");

            for (int i=0; i<variable.getValuesCount(); i++) {
                System.out.print((i!=0 ? ", " : "") + variable.getValues(i));
            }
            System.out.println("]");
        }
    }
}
