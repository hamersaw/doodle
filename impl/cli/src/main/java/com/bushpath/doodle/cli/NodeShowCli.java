package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeShowResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "show",
    description = "Show a node.",
    mixinStandardHelpOptions = true)
public class NodeShowCli implements Runnable {
    @Parameters(index="0", description="Id of node.")
    private int id;

    @Override
    public void run() {
        // create NodeShowRequest
        NodeShowRequest request = NodeShowRequest.newBuilder()
            .setId(this.id)
            .build();
        NodeShowResponse response = null;

        // send request
        try {
            response = (NodeShowResponse) CommUtility.send(
                MessageType.NODE_SHOW.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle NodeShowResponse
        Node node = response.getNode();
        System.out.println("id = \"" + this.id + "\""
            + "\nipAddress = \"" + node.getIpAddress() + "\""
            + "\nport = \"" + node.getPort() + "\"");
    }
}
