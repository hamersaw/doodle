package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Map;

@Command(name = "list",
    description = "List files.",
    mixinStandardHelpOptions = true)
public class FileSystemListCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Override
    public void run() {
        String user = System.getProperty("user.name");
        String group = user;

        // create FileListRequest
        FileListRequest request = FileListRequest.newBuilder()
            .setUser(user)
            .setGroup(group)
            .setPath(this.path)
            .build();
        FileListResponse response = null;

        // send request
        try {
            response = (FileListResponse) CommUtility.send(
                MessageType.FILE_LIST.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle FileListResponse
        int i=0;
        for (File file : response.getFilesList()) {
            System.out.println((i != 0 ? "\n" : "")
                + "[[files]]"
                + "\nfileType: \"" + file.getFileType() + "\""
                + "\nuser: \"" + file.getUser() + "\""
                + "\ngroup: \"" + file.getGroup() + "\""
                + "\nname: \"" + file.getName() + "\"");

            switch (file.getFileType()) {
                case DIRECTORY:
                    break;
                case REGULAR:
                    // TODO - remove or fix print observations
                    for (Map.Entry<Integer, Integer> entry :
                            file.getObservationsMap().entrySet()) {
                        System.out.println(entry.getKey() + ":"
                            + entry.getValue());
                    }

                    break;
            }

            i += 1;
        }
    }
}
