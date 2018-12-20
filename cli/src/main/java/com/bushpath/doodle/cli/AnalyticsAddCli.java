package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileAddRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileAddResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "add",
    description = "Add a file to the analytics plane.",
    mixinStandardHelpOptions = true)
public class AnalyticsAddCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Option(names={"-d", "--directory"},
        description="Create directory.")
    private boolean directory;

    @Override
    public void run() {
        String user = System.getProperty("user.name");
        String group = user;

        // create FileAddRequest
        FileAddRequest request = FileAddRequest.newBuilder()
            .setFileType(this.directory ? 
                FileType.DIRECTORY : FileType.REGULAR)
            .setUser(user)
            .setGroup(group)
            .setPath(path)
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
