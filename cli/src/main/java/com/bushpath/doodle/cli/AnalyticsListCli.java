package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "list",
    description = "List files.",
    mixinStandardHelpOptions = true)
public class AnalyticsListCli implements Runnable {
    @Override
    public void run() {
        // create FileListRequest
        FileListRequest request = FileListRequest.newBuilder()
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

        // TODO - handle FileListResponse
    }
}
