package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlListResponse;

import picocli.CommandLine.Command;

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

        // send request
        try {
            response = (ControlListResponse) CommUtility.send(
                MessageType.CONTROL_LIST.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle ControlListResponse
        int i=0;
        for (Map.Entry<String, String> entry :
                response.getPluginsMap().entrySet()) {
            System.out.println((i != 0 ? "\n" : "") + "[[control.plugin]]"
                + "\nid = \"" + entry.getKey() + "\""
                + "\nclass = \"" + entry.getValue() + "\"");

            i++;
        }
    }
}
