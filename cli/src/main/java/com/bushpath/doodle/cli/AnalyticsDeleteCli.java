package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileDeleteResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Map;

@Command(name = "delete",
    description = "Delete a file to the analytics plane.",
    mixinStandardHelpOptions = true)
public class AnalyticsDeleteCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Override
    public void run() {
        String user = System.getProperty("user.name");
        String group = user;

        // create FileDeleteRequest
        FileDeleteRequest request = FileDeleteRequest.newBuilder()
            .setUser(user)
            .setGroup(group)
            .setPath(this.path)
            .build();
        FileDeleteResponse response = null;

        // send request
        try {
            response = (FileDeleteResponse) CommUtility.send(
                MessageType.FILE_DELETE.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle FileDeleteResponse
    }
}
