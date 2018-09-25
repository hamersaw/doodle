package com.bushpath.doodle.cli;

import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginListResponse;

import picocli.CommandLine.Command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

@Command(name = "list",
    description = "List PluginPlugins and instances.",
    mixinStandardHelpOptions = true)
public class PluginListCli implements Runnable {
    @Override
    public void run() {
        // create PluginListRequest
        PluginListRequest request = PluginListRequest.newBuilder()
            .build();
        PluginListResponse response = null;

        try {
            // send request
            Socket socket = new Socket(Main.ipAddress, Main.port);

            DataOutputStream out =
                new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeInt(MessageType.PLUGIN_LIST.getNumber());
            request.writeDelimitedTo(out);

            // recv response
            // TODO - validate we have the correct message type
            in.readInt();
            response = PluginListResponse.parseDelimitedFrom(in);
        } catch (IOException e) {
            System.err.println("Unknown communication error: " +
                e.getClass() + ":" + e.getMessage());
            return;
        }

        // handle PluginListResponse
        System.out.println("control:");
        for (String controlPlugin : response.getControlPluginsList()) {
            System.out.println("\t- '" + controlPlugin + "'");
        }

        System.out.println("sketch:");
        for (String sketchPlugin : response.getSketchPluginsList()) {
            System.out.println("\t- '" + sketchPlugin + "'");
        }
    }
}
