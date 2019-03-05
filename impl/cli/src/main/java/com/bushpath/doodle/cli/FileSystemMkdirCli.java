package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileMkdirResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "mkdir",
    description = "Create a directory.",
    mixinStandardHelpOptions = true)
public class FileSystemMkdirCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Override
    public void run() {
        String user = System.getProperty("user.name");
        String group = user;

        // create FileMkdirRequest
        FileMkdirRequest request = FileMkdirRequest.newBuilder()
            .setUser(user)
            .setGroup(group)
            .setPath(path)
            .build();
        FileMkdirResponse response = null;

        // send request
        try {
            response = (FileMkdirResponse) CommUtility.send(
                MessageType.FILE_MKDIR.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle FileMkdirResponse
    }
}
