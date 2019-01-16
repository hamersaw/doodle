package com.bushpath.doodle.cli;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.protobuf.DoodleProtos.Checkpoint;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.Node;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.NodeListResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.Replica;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.SketchShowResponse;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.parser.FeatureRangeParser;
import com.bushpath.rutils.query.parser.Parser;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Command(name = "query",
    description = "Query a sketch for data.",
    mixinStandardHelpOptions = true)
public class DataQueryCli implements Runnable {
    @Parameters(index="0", description="Id of SketchPlugin instance.")
    private String sketchId;

    @Option(names={"-b", "--buffer-size"},
        description="Size of buffer data (in bytes) [default=2000].")
    private int bufferSize = 2000;

    @Option(names = {"-p", "--plugin-directory"},
        description = "Directory containing Doodle plugins [default=../plugins].")
    private String pluginDirectory = "../plugins";

    @Option(names = {"-q", "--query"},
        description = "Feature range query (eq. 'f0:0..10', 'f1:0..', 'f2:..10').")
    private String[] queries;

    @Option(names={"-w", "--worker-count"},
        description="Number of workers in ThreadedCursor [default=8].")
    private int workerCount = 8;

    @Override
    public void run() {
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

        // initialize SketchShowRequest
        SketchShowRequest sketchShowRequest =
            SketchShowRequest.newBuilder()
                .setId(this.sketchId)
                .build();
        SketchShowResponse sketchShowResponse = null;

        // send SketchShowRequest
        try {
            sketchShowResponse = (SketchShowResponse) CommUtility.send(
                MessageType.SKETCH_SHOW.getNumber(),
                sketchShowRequest, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // intialize most recent checkpoint replication data
        Checkpoint mostRecentCheckpoint = null;
        for (Checkpoint checkpoint :
                sketchShowResponse.getCheckpointsList()) {
            if (mostRecentCheckpoint == null || 
                    checkpoint.getTimestamp() > 
                    mostRecentCheckpoint.getTimestamp()) {
                mostRecentCheckpoint = checkpoint;
            }
        }

        Map<Integer, List<Integer>> replicas = new HashMap();
        if (mostRecentCheckpoint != null) {
            for (Replica replica : 
                    mostRecentCheckpoint.getReplicasList()) {
                replicas.put(replica.getPrimaryNodeId(),
                    replica.getSecondaryNodeIdsList());
            }
        }

        // load plugins
        URLClassLoader urlClassLoader = null;
        try {
            // find plugin jars
            Object[] paths =
                Files.walk(Paths.get(this.pluginDirectory))
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.endsWith("jar"))
                    .toArray();

            URL[] urls = new URL[paths.length];
            for (int i=0; i<urls.length; i++) {
                String absoluteName =
                    ((Path) paths[i]).toUri().toString();
                urls[i] = new URL(absoluteName);
            }

            // initialize new class loader as child
            urlClassLoader =
                new URLClassLoader(urls, Main.class.getClassLoader());

            // must set ContextClassLoader of current thread
            // it is adopted by all threads created by this
            //Thread.currentThread().setContextClassLoader(urlClassLoader);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // initialize Inflator
        Inflator inflator = null;
        try {
            Class c = urlClassLoader
                .loadClass(sketchShowResponse.getInflatorClass());
            Constructor constructor = c.getConstructor(List.class);
            inflator = (Inflator) constructor
                .newInstance(sketchShowResponse.getVariablesList());
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("failed to initialize Inflator: "
                + e.getMessage());
            return;
        }

        // initialize NodeListRequest
        NodeListRequest nodeListRequest =
            NodeListRequest.newBuilder().build();
        NodeListResponse nodeListResponse = null;

        // send NodeListRequest
        try {
            nodeListResponse = (NodeListResponse) CommUtility.send(
                MessageType.NODE_LIST.getNumber(),
                nodeListRequest, Main.ipAddress, Main.port);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }

        // initialize checkpoint and node lookup variables
        Map<Integer, Node> nodeLookup = new HashMap();
        for (Node node : nodeListResponse.getNodesList()) {
            nodeLookup.put(node.getId(), node);
        }

        if (replicas.isEmpty()) {
            for (Integer nodeId : nodeLookup.keySet()) {
                replicas.put(nodeId, new ArrayList());
            }
        }

        try {
            // initialize ThreadedCursor
            ThreadedCursor cursor = new ThreadedCursor(
                replicas, nodeLookup, inflator,
                query, this.bufferSize, this.workerCount);

            // iterate over observations
            long count = 0;
            float[] observation = null;
            while ((observation = cursor.next()) != null) {
                // TODO - TMP don't print observations
                /*for (int i=0; i<observation.length; i++) {
                    System.out.print((i == 0 ? "" : ",")
                        + observation[i]);
                }
                System.out.println("");*/
                count += 1;
            }

            System.out.println("generated " + count + " record(s)");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }
    }
}
