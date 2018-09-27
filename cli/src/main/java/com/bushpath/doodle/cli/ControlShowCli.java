package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.ControlShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginVariable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

@Command(name = "show",
    description = "Show a ControlPlugin.",
    mixinStandardHelpOptions = true)
public class ControlShowCli implements Runnable {
    @Parameters(index="0", description="Id of ControlPlugin instance.")
    private String id;

    @Override
    public void run() {
        // create ControlShowRequest
        ControlShowRequest request = ControlShowRequest.newBuilder()
            .setId(this.id)
            .build();
        ControlShowResponse response = null;

        // send request
        try {
            response = (ControlShowResponse) CommUtility.send(
                MessageType.CONTROL_SHOW.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle ControlShowResponse
        System.out.println("id = \"" + this.id + "\""
            + "\nclass = \"" + response.getPlugin() + "\"");

        for (PluginVariable variable : response.getVariablesList()) {
            System.out.print("\n[[variable]]"
                + "\ntype = \"" + variable.getType() + "\""
                + "\nname = \"" + variable.getName() + "\""
                + "\nvalues = [");

            for (int i=0; i<variable.getValuesCount(); i++) {
                System.out.print((i!=0 ? ", " : "")
                    + "\"" + variable.getValues(i) + "\"");
            }
            System.out.println("]");
        }
    }
}
