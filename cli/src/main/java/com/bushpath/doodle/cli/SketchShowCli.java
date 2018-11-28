package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.Checkpoint;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.PluginVariable;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

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
            + "\nfrozen = \"" + response.getFrozen() + "\""
            + "\nclass = \"" + response.getPlugin() + "\""
            + "\ninflatorClass = \"" + response.getInflatorClass() + "\"");

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

        for (Checkpoint checkpoint : response.getCheckpointsList()) {
            System.out.print("\n[[checkpoint]]"
                + "\ntimestamp = \"" + checkpoint.getTimestamp() + "\""
                + "\nsketchId = \"" + checkpoint.getSketchId() + "\""
                + "\ncheckpointId = \"" + checkpoint.getCheckpointId() + "\"");

            for (Replica replica : checkpoint.getReplicasList()) {
                System.out.print("\n[[checkpoint.replica]]"
                    + "\nprimaryNodeId = \"" 
                    + replica.getPrimaryNodeId() + "\""
                    + "\nsecondaryNodeIds = [");

                for (int i=0; i<replica.getSecondaryNodeIdsCount(); i++) {
                    System.out.print((i!=0 ? ", " : "")
                        + "\"" + replica.getSecondaryNodeIds(i) + "\"");
                }

                System.out.print("]");
            }

            System.out.println();
        }
    }
}
