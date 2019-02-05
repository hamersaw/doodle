package com.bushpath.doodle.node;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;
import com.bushpath.doodle.protobuf.DoodleProtos.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.ControlManager;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OperationJournal extends Journal {
    protected static final byte OPERATION = 0;
    protected static final byte TIMESTAMP = 1;
    protected static final Logger log =
        LoggerFactory.getLogger(OperationJournal.class);

    protected ControlManager controlManager;
    protected SketchManager sketchManager;
    protected TreeMap<Long, Operation> journal;
    protected ReadWriteLock lock;
    protected Map<Integer, Long> journalTimestamps;
    protected long journalTimestamp;

    public OperationJournal(String directory, int maximumFileSize,
            ControlManager controlManager,
            SketchManager sketchManager) throws Exception {
        super(directory, maximumFileSize);

        this.controlManager = controlManager;
        this.sketchManager = sketchManager;
        this.journal = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
        this.journalTimestamps = new HashMap();

        // set journalTimestamp
        this.journalTimestamp = 0;
        for (Map.Entry<String, ControlPlugin> entry :
                this.controlManager.getEntrySet()) {
            long lastUpdated = entry.getValue().getLastUpdated();
            if (lastUpdated > this.journalTimestamp) {
                this.journalTimestamp = lastUpdated;
            }
        }

        for (Map.Entry<String, SketchPlugin> entry :
                this.sketchManager.getEntrySet()) {
            long lastUpdated = entry.getValue().getLastUpdated();
            if (lastUpdated > this.journalTimestamp) {
                this.journalTimestamp = lastUpdated;
            }
        }

        // parse journaled data
        File file = new File(this.directory);
        for (String filename : file.list()) {
            String filePath = this.directory + "/" + filename;
            DataInputStream in = null;
            long lastTimestamp = 0;

            try {
                in = new DataInputStream(new FileInputStream(filePath));

                while (true) {
                    byte dataType = (byte) in.readByte();
                    switch(dataType) {
                        case OPERATION:
                            Operation operation =
                                Operation.parseDelimitedFrom(in);

                            this.journal.put(operation.getTimestamp(),
                                operation);

                            if (operation.getTimestamp() >
                                    lastTimestamp) {
                                lastTimestamp =
                                    operation.getTimestamp();
                            }

                            log.trace("parsed operation {}",
                                operation.getTimestamp());
                            break;
                        case TIMESTAMP:
                            int nodeId = in.readInt();
                            long timestamp = in.readLong();
                            this.journalTimestamps
                                .put(nodeId, timestamp);

                            log.trace("parsed timestamp {}:{}",
                                nodeId, timestamp);
                            break;
                    }
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

    public void add(Operation operation) throws Exception {
        this.lock.writeLock().lock();
        try {
            log.debug("adding operation {}:{}:{}",
                operation.getTimestamp(), operation.getPluginId(),
                operation.getOperationType());

            // execute operation
            switch (operation.getPluginType()) {
                case CONTROL:
                    this.controlManager.handleOperation(operation);
                    break;
                case SKETCH:
                    this.sketchManager.handleOperation(operation);
                    break;
            }

            // add operation to journal
            this.journalTimestamp = operation.getTimestamp();
            this.journal.put(operation.getTimestamp(), operation);

            DataOutputStream out = this.lockWriteOutputStream();
            out.writeByte(OPERATION);
            operation.writeDelimitedTo(out);
            this.unlockWriteOutputStream(this.journalTimestamp);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    protected void cleanCompletedFiles() {
        // find minimum timestamp
        long minimumTimestamp = Long.MAX_VALUE;
        for (Long jTimestamp : this.journalTimestamps.values()) {
            if (jTimestamp < minimumTimestamp) {
                minimumTimestamp = jTimestamp;
            }
        }

        // delete all files less than minimum timestamp
        for (Map.Entry<Long, String> entry : this.completedFiles
                .headMap(minimumTimestamp, true).entrySet()) {
            File file = new File(entry.getValue());
            file.delete();

            log.debug("deleted file '{}'", entry.getValue());
        }
    }

    public long getTimestamp() {
        this.lock.readLock().lock();
        try {
            return this.journalTimestamp;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Map<Long, Operation> search(long timestamp) {
        this.lock.readLock().lock();
        try {
            return this.journal.tailMap(timestamp, false);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void updateJournalTimestamp(int nodeId,
            long timestamp) throws Exception {
        this.lock.writeLock().lock();
        try {
            // if our current timestamp is greater -> return
            if (this.journalTimestamps.containsKey(nodeId) &&
                    this.journalTimestamps.get(nodeId) >= timestamp) {
                return;
            }

            // update timestamp
            this.journalTimestamps.put(nodeId, timestamp);

            DataOutputStream out = this.lockWriteOutputStream();
            out.writeByte(TIMESTAMP);
            out.writeInt(nodeId);
            out.writeLong(timestamp);
            this.unlockWriteOutputStream(this.journalTimestamp);

            log.debug("Updated timestamp for node {} to {}",
                nodeId, timestamp);

            // delete any unnecessary journal files
            this.cleanCompletedFiles();
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}
