package com.bushpath.doodle.cli;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperationResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.OperationType;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.parser.FeatureRangeParser;
import com.bushpath.rutils.query.parser.Parser;

import com.google.protobuf.ByteString;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Random;

@Command(name = "create",
    description = "Create a file.",
    mixinStandardHelpOptions = true)
public class FileSystemCreateCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Parameters(index="1", description="SketchId to build file from.")
    private String sketchId;

    @Option(names = {"-f", "--format"},
        converter = FileFormatConverter.class,
        description = "File format [default: CSV].")
    private FileFormat fileFormat = FileFormat.CSV;

    @Option(names = {"-q", "--query"},
        description = "Feature range query (eq. 'f0:0..10', 'f1:0..', 'f2:..10').")
    private String[] queries;

    @Override
    public void run() {
        String user = System.getProperty("user.name");

        // parse query
        Query query = null;
        try {
            Parser parser = new FeatureRangeParser();
            query = parser.evaluate(queries);
            query.setEntity(this.sketchId);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // pack query into ByteString
        ByteString queryByteString = null;
        try {
            queryByteString = ByteString.copyFrom(query.toByteArray());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // create FileOperationRequest
        Random random = new Random(System.nanoTime());
        String filename = FileSystemCli.parseFilename(this.path);

        long timestamp = System.currentTimeMillis();
        File file = File.newBuilder()
            .setInode(random.nextInt())
            .setFileType(FileType.REGULAR)
            .setUser(user)
            .setGroup(user)
            .setName(filename)
            .setSize(-1)
            .setChangeTime(timestamp)
            .setModificationTime(timestamp)
            .setAccessTime(timestamp)
            .setFileFormat(this.fileFormat)
            .setQuery(queryByteString)
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
