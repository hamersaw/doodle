package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.FileCreateResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.parser.FeatureRangeParser;
import com.bushpath.rutils.query.parser.Parser;

import com.google.protobuf.ByteString;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Option;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

@Command(name = "create",
    description = "Create a file.",
    mixinStandardHelpOptions = true)
public class FileSystemCreateCli implements Runnable {
    @Parameters(index="0", description="Path of file.")
    private String path;

    @Parameters(index="1", description="SketchId to build file from.")
    private String sketchId;

    @Option(names = {"-q", "--query"},
        description = "Feature range query (eq. 'f0:0..10', 'f1:0..', 'f2:..10').")
    private String[] queries;

    @Override
    public void run() {
        String user = System.getProperty("user.name");
        String group = user;

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

        // create FileCreateRequest
        FileCreateRequest request = FileCreateRequest.newBuilder()
            .setUser(user)
            .setGroup(group)
            .setPath(path)
            .setSketchId(this.sketchId)
            .setQuery(queryByteString)
            .build();
        FileCreateResponse response = null;

        // send request
        try {
            response = (FileCreateResponse) CommUtility.send(
                MessageType.FILE_CREATE.getNumber(),
                request, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // TODO - handle FileCreateResponse
    }
}
