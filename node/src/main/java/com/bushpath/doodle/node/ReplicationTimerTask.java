package com.bushpath.doodle.node;

import com.bushpath.doodle.CommUtility;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteSearchRequest;
import com.bushpath.doodle.protobuf.DoodleProtos.JournalWriteSearchResponse;
import com.bushpath.doodle.protobuf.DoodleProtos.MessageType;
import com.bushpath.doodle.protobuf.DoodleProtos.WriteUpdate;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.control.NodeMetadata;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReplicationTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(ReplicationTimerTask.class);

    protected NodeManager nodeManager;
    protected Map<Integer, Set<SketchPlugin>> replicas;
    protected ReadWriteLock lock;

    public ReplicationTimerTask(NodeManager nodeManager) {
        this.nodeManager = nodeManager;
        this.replicas = new HashMap();
        this.lock = new ReentrantReadWriteLock();
    }

    public void addReplica(int nodeId, SketchPlugin sketch) {
        this.lock.writeLock().lock();
        try {
            Set<SketchPlugin> sketches = null;
            if (this.replicas.containsKey(nodeId)) {
                sketches = this.replicas.get(nodeId);
            } else {
                sketches = new HashSet();
                this.replicas.put(nodeId, sketches);
            }

            sketches.add(sketch);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public void run() {
        this.lock.readLock().lock();
        try {
            for (Map.Entry<Integer, Set<SketchPlugin>> entry :
                    replicas.entrySet()) {
                // initialize JournalWriteSearchRequest
                JournalWriteSearchRequest.Builder jwsBuilder =
                    JournalWriteSearchRequest.newBuilder()
                        .setNodeId(this.nodeManager.getThisNodeId());

                for (SketchPlugin sketch : entry.getValue()) {
                    jwsBuilder.putPersistTimestamps(sketch.getId(),
                        sketch.getPersistTimestamp(entry.getKey()));

                    jwsBuilder.putWriteTimestamps(sketch.getId(),
                        sketch.getWriteTimestamp(entry.getKey()));
                }

                // get primary replica node
                if (!this.nodeManager.contains(entry.getKey())) {
                    continue;
                }

                NodeMetadata nodeMetadata = null;
                try {
                    nodeMetadata = this.nodeManager.get(entry.getKey());
                } catch (Exception e) {
                    log.warn("Failed to retrieve node metadata", e);
                    continue;
                }

                // send JournalWriteSearchRequest
                JournalWriteSearchRequest jwsRequest =
                    jwsBuilder.build();
                JournalWriteSearchResponse jwsResponse;

                try {
                    jwsResponse = (JournalWriteSearchResponse)
                        CommUtility.send(
                        MessageType.JOURNAL_WRITE_SEARCH.getNumber(),
                        jwsRequest, nodeMetadata.getIpAddress(),
                        (short) nodeMetadata.getPort());
                } catch (ConnectException e) {
                    log.warn("Connection to {} unsuccessful",
                        nodeMetadata.toString());
                    continue;
                } catch (Exception e) {
                    log.error("Unknown communication error", e);
                    continue;
                }

                // process JournalWriteSearchResponse
                for (WriteUpdate writeUpdate :
                        jwsResponse.getWriteUpdatesList()) {
                    SketchPlugin sketch = null;
                    for (SketchPlugin tmp : entry.getValue()) {
                        if (writeUpdate.getSketchId()
                                .equals(tmp.getId())) {
                            sketch = tmp;
                        }
                    }

                    for (Map.Entry<Long, ByteString> e:
                            writeUpdate.getDataMap().entrySet()) {
                        // write data to sketch
                        sketch.write(entry.getKey(),
                            e.getKey(), e.getValue());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("unknown failure", e);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
