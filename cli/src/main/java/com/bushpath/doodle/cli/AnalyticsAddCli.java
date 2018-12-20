package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileAddRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileAddResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "add",
    description = "Add a file to the analytics plane.",
    mixinStandardHelpOptions = true)
public class AnalyticsAddCli implements Runnable {
    @Override
    public void run() {
        // create FileAddRequest
        FileAddRequest request = FileAddRequest.newBuilder()
            .build();
        FileAddResponse response = null;

        // send request
        try {
            response = (FileAddResponse) CommUtility.send(
                MessageType.FILE_ADD.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle FileAddResponse
    }
}
