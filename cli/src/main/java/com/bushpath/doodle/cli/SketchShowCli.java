package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Variable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.Map;

@Command(name = "show",
    description = "Show a SketchPlugin.",
    mixinStandardHelpOptions = true)
public class SketchShowCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String id;

    @Override
    public void run() {
        // create SketchShowRequest
        SketchShowRequest request = SketchShowRequest.newBuilder()
            .setId(this.id)
            .build();
        SketchShowResponse response = null;

        // send request
        try {
            response = (SketchShowResponse) CommUtility.send(
                MessageType.SKETCH_SHOW.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // handle SketchShowResponse
        System.out.println("id = \"" + this.id + "\""
            + "\nclass = \"" + response.getPlugin() + "\""
            + "\ninflatorClass = \""
                + response.getInflatorClass() + "\""
            + "\nreplicationFactor = "
                + response.getReplicationFactor()
            + "\nfrozen = \"" + response.getFrozen() + "\"");

        for (Variable variable : response.getVariablesList()) {
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

        for (Replica replica : response.getReplicasList()) {
            System.out.print("\n[[replica]]"
                + "\nprimaryNodeId = " + replica.getPrimaryNodeId()
                + "\nsecondaryNodeIds = [");

            int i=0;
            for (Integer secondaryNodeId :
                    replica.getSecondaryNodeIdsList()) {
                System.out.print((i != 0 ? ", " : "")
                    + secondaryNodeId);
                i += 1;
            }

            System.out.println("]");
        }
    }
}
