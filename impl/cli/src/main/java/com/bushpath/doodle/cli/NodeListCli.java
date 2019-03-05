package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;

import picocli.CommandLine.Command;

import java.util.Map;

@Command(name = "list",
    description = "List nodes.",
    mixinStandardHelpOptions = true)
public class NodeListCli implements Runnable {
    @Override
    public void run() {
        // create NodeListRequest
        NodeListRequest request = NodeListRequest.newBuilder()
            .build();
        NodeListResponse response = null;

        // send request
        try {
            response = (NodeListResponse) CommUtility.send(
                MessageType.NODE_LIST.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle NodeListResponse
        int i=0;
        for (Node node : response.getNodesList()) {
            System.out.println((i != 0 ? "\n" : "") + "[[node]]"
                + "\nid = \"" + node.getId() + "\""
                + "\nipAddress = \"" + node.getIpAddress() + "\""
                + "\nport = \"" + node.getPort() + "\"");
            i++;
        }
    }
}
