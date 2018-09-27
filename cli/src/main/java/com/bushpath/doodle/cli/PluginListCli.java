package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
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
    description = "List available ControlPlugins and SketchPlugins.",
    mixinStandardHelpOptions = true)
public class PluginListCli implements Runnable {
    @Override
    public void run() {
        // create PluginListRequest
        PluginListRequest request = PluginListRequest.newBuilder()
            .build();
        PluginListResponse response = null;

        // send request
        try {
            response = (PluginListResponse) CommUtility.send(
                MessageType.PLUGIN_LIST.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
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
