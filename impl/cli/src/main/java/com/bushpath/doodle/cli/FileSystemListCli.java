package com.bushpath.doodle.cli;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileListResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.DataInputStream;
import java.util.Map;

@Command(name = "list",
    description = "List files.",
    mixinStandardHelpOptions = true)
public class FileSystemListCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Option(names = {"-v", "--verbose"},
        description = "Include verbose output.")
    private boolean verbose;

    @Override
    public void run() {
        String user = System.getProperty("user.name");

        // create FileListRequest
        FileListRequest request = FileListRequest.newBuilder()
            .setPath(this.path)
            .build();
        FileListResponse response = null;

        // send request
        try {
            RpcClient rpcClient = new RpcClient(Main.ipAddress,
                Main.port, user,
                "com.bushpath.doodle.protocol.DfsProtocol");

            // read response
            DataInputStream in = rpcClient.send("list", request);
            response = FileListResponse.parseDelimitedFrom(in);

            // close DataInputStream and RpcClient
            in.close();
            rpcClient.close();
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

            if (this.verbose) {
                switch (file.getFileType()) {
                    case DIRECTORY:
                        break;
                    case REGULAR:
                        // print observation counts
                        for (Map.Entry<Integer, Integer> entry :
                                file.getObservationsMap().entrySet()) {
                            System.out.println("[[files.observations]]"
                                + "\nnodeId: " + entry.getKey()
                                + "\ncount: " + entry.getValue());
                        }

                        break;
                }
            }

            i += 1;
        }
    }
}
