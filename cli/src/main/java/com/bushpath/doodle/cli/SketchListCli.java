package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchListResponse;

import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "list",
    description = "List all initialized SketchPlugins.",
    mixinStandardHelpOptions = true)
public class SketchListCli implements Runnable {
    @Override
    public void run() {
        // create SketchListRequest
        SketchListRequest request = SketchListRequest.newBuilder()
            .build();
        SketchListResponse response = null;

        // send request
        try {
            response = (SketchListResponse) CommUtility.send(
                MessageType.SKETCH_LIST.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle SketchListResponse
        int i=0;
        for (Map.Entry<String, String> entry :
                response.getPluginsMap().entrySet()) {
            System.out.println((i != 0 ? "\n" : "") + "[[sketch.plugin]]"
                + "\nid = \"" + entry.getKey() + "\""
                + "\nclass = \"" + entry.getValue() + "\"");

            i++;
        }
    }
}
