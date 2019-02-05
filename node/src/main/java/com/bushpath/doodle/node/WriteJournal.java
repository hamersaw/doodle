package com.bushpath.doodle.node;

import com.bushpath.doodle.SketchPlugin;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.sketch.SketchManager;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteJournal extends Journal {
    protected static final Logger log =
        LoggerFactory.getLogger(WriteJournal.class);

    protected SketchManager sketchManager;
    protected Map<String, TreeMap<Long, ByteString>> journal;
    protected Map<String, Map<Integer, Long>> journalTimestamps;
    protected ReadWriteLock lock;

    public WriteJournal(String directory, int maximumFileSize,
            SketchManager sketchManager) throws Exception {
        super(directory, maximumFileSize);

        this.sketchManager = sketchManager;
        this.journal = new HashMap();
        this.journalTimestamps = new HashMap();
        this.lock = new ReentrantReadWriteLock();

        // parse journaled data
        File file = new File(this.directory);
        for (String filename : file.list()) {
            String filePath = this.directory + "/" + filename;
            DataInputStream in = null;
            long lastTimestamp = 0;

            byte[] buffer = new byte[1024];
            try {
                in = new DataInputStream(new FileInputStream(filePath));

                while (true) {
                    // parse data
                    long timestamp = in.readLong();
                    int nodeId = in.readInt();
                    String sketchId = in.readUTF();
                    int size = in.readInt();

                    // read data
                    if (size > buffer.length) {
                        buffer = new byte[size];
                    }

                    in.readFully(buffer, 0, size);
                    ByteString data =
                        ByteString.copyFrom(buffer, 0, size);

                    // add to sketch
                    SketchPlugin sketch =
                        this.sketchManager.get(sketchId);
                    if (sketch.getPersistTimestamp(nodeId)
                            < timestamp) {
                        sketch.write(nodeId, timestamp, data);
                    }

                    // get memoryTable
                    TreeMap<Long, ByteString> memoryTable = null;
                    if (this.journal.containsKey(sketchId)) {
                        memoryTable = this.journal.get(sketchId);
                    } else {
                        memoryTable = new TreeMap();
                        this.journal.put(sketchId, memoryTable);
                    }

                    // add buffer
                    memoryTable.put(timestamp, data);
                }
            } catch (EOFException e) {
            } finally {
                in.close();
            }

            this.completedFiles.put(lastTimestamp, filePath);
        }

        // delete any unnecessary journal files
        this.cleanCompletedFiles();
    }

    public void add(int nodeId, String sketchId,
            ByteString data) throws Exception  {
        this.lock.writeLock().lock();
        try {
            long timestamp = System.nanoTime();

            // write to journal
            DataOutputStream out = this.lockWriteOutputStream();
            out.writeLong(timestamp);
            out.writeInt(nodeId);
            out.writeUTF(sketchId);
            out.writeInt(data.size());
            data.writeTo(out);
            this.unlockWriteOutputStream(timestamp);

            // write to local sketch
            SketchPlugin sketch = this.sketchManager.get(sketchId);
            sketch.write(nodeId, timestamp, data);

            // get memoryTable
            TreeMap<Long, ByteString> memoryTable = null;
            if (this.journal.containsKey(sketchId)) {
                memoryTable = this.journal.get(sketchId);
            } else {
                memoryTable = new TreeMap();
                this.journal.put(sketchId, memoryTable);
            }

            // add buffer
            memoryTable.put(timestamp, data);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    protected void cleanCompletedFiles() {
        // find minimum timestamp
        Long minimumTimestamp = Long.MAX_VALUE;
        for (Map.Entry<String, Map<Integer, Long>> entry :
                this.journalTimestamps.entrySet()) {
            // check if map.size() < replicationFactor
            // gossip is not complete -> return
            SketchPlugin sketchPlugin =
                this.sketchManager.get(entry.getKey());
            Map<Integer, Long> map = entry.getValue();
            if (sketchPlugin == null ||
                    map.size() < sketchPlugin.getReplicationFactor()) {
                minimumTimestamp = Long.MAX_VALUE;
                break;
            }

            // calculate minimum timestamp
            for (Long timestamp : map.values()) {
                if (timestamp < minimumTimestamp) {
                    minimumTimestamp = timestamp;
                }
            }
        }

        if (minimumTimestamp == Long.MAX_VALUE) {
            return;
        }

        // delete all files less than minimum timestamp
        for (Map.Entry<Long, String> entry : this.completedFiles
                .headMap(minimumTimestamp, true).entrySet()) {
            File file = new File(entry.getValue());
            file.delete();

            log.debug("deleted file '{}'", entry.getValue());
        }
    }

    public Map<Long, ByteString> search(String sketchId,
            long timestamp) throws Exception {
        this.lock.readLock().lock();
        try {
            if (!this.journal.containsKey(sketchId)) {
                return new HashMap(); // wait until gossip inits sketch
            }

            return this.journal.get(sketchId).tailMap(timestamp, false);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void updateJournalTimestamp(String sketchId, int nodeId,
            long timestamp) throws Exception {
        this.lock.writeLock().lock();
        try {
            // get sketch map
            Map<Integer, Long> map = null;
            if (this.journalTimestamps.containsKey(sketchId)) {
                map = this.journalTimestamps.get(sketchId);
            } else {
                map = new HashMap();
                this.journalTimestamps.put(sketchId, map);
            }

            // if our current timestamp is greater -> return
            if (map.containsKey(nodeId) &&
                    map.get(nodeId) >= timestamp) {
                return;
            }

            // update timestamp
            map.put(nodeId, timestamp);

            log.debug("Updated timestamp for {}:{} to {}",
                sketchId, nodeId, timestamp);

            // delete any unnecessary journal files
            this.cleanCompletedFiles();
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}
