package com.bushpath.doodle.cli;

import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListResponse;

import picocli.CommandLine.Command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

@Command(name = "list",
    description = "List a ControlPlugin.",
    mixinStandardHelpOptions = true)
public class ControlListCli implements Runnable {
    @Override
    public void run() {
        // create ControlListRequest
        ControlListRequest request = ControlListRequest.newBuilder()
            .build();
        ControlListResponse response = null;

        try {
            // send request
            Socket socket = new Socket(Main.ipAddress, Main.port);

            DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeInt(MessageType.CONTROL_LIST.getNumber());
            request.writeDelimitedTo(out);

            // recv response
            // TODO - validate we have the correct message type
            in.readInt();
            response = ControlListResponse.parseDelimitedFrom(in);
        } catch (IOException e) {
            System.err.println("Unknown communication error: " +
                e.getClass() + ":" + e.getMessage());
            return;
        }

        // handle ControlListResponse
        for (Map.Entry<String, String> entry :
                response.getPluginsMap().entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
