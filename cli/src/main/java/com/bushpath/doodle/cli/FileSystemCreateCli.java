package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "create",
    description = "Create a file.",
    mixinStandardHelpOptions = true)
public class FileSystemCreateCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Override
    public void run() {
        String user = System.getProperty("user.name");
        String group = user;

        // create FileCreateRequest
        FileCreateRequest request = FileCreateRequest.newBuilder()
            .setUser(user)
            .setGroup(group)
            .setPath(path)
            .build();
        FileCreateResponse response = null;

        // send request
        try {
            response = (FileCreateResponse) CommUtility.send(
                MessageType.FILE_CREATE.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle FileCreateResponse
    }
}
