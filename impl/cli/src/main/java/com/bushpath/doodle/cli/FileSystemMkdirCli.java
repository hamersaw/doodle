package com.bushpath.doodle.cli;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.io.DataInputStream;
import java.util.Map;
import java.util.Random;

@Command(name = "mkdir",
    description = "Create a directory.",
    mixinStandardHelpOptions = true)
public class FileSystemMkdirCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Override
    public void run() {
        String user = System.getProperty("user.name");

        // create FileOperationRequest
        Random random = new Random(System.nanoTime());
        String filename = FileSystemCli.parseFilename(this.path);

        long timestamp = System.currentTimeMillis();
        File file = File.newBuilder()
            .setInode(random.nextInt())
            .setFileType(FileType.DIRECTORY)
            .setUser(user)
            .setGroup(user)
            .setName(filename)
            .setSize(-1)
            .setChangeTime(timestamp)
            .setModificationTime(timestamp)
            .setAccessTime(timestamp)
            .build();

        FileOperation fileOperation = FileOperation.newBuilder()
            .setTimestamp(timestamp)
            .setPath(this.path)
            .setFile(file)
            .setOperationType(OperationType.ADD)
            .build();

        FileOperationRequest request = FileOperationRequest.newBuilder()
            .setOperation(fileOperation)
            .build();

        FileOperationResponse response = null;

        // send request
        try {
            RpcClient rpcClient = new RpcClient(Main.ipAddress,
                Main.port, user,
                "com.bushpath.doodle.protocol.DfsProtocol");

            // read response
            DataInputStream in =
                rpcClient.send("addOperation", request);
            response = FileOperationResponse.parseDelimitedFrom(in);

            // close DataInputStream and RpcClient
            in.close();
            rpcClient.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle FileOperationResponse
    }
}
