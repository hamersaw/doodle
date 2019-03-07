package com.bushpath.doodle.dfs.file;

import com.bushpath.doodle.Inflator;
import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import com.bushpath.doodle.dfs.BlockManager;
import com.bushpath.doodle.dfs.format.Format;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DoodleFile extends DoodleEntry {
    protected Format format;
    protected Query query;
    protected ByteString queryByteString;

    protected ReadWriteLock lock;
    protected Map<Long, Integer> blocks;
    protected Inflator inflator;
    protected Map<Integer, List<Integer>> replicas;

    public DoodleFile(String name, Format format, Query query,
            ByteString queryByteString, int featureCount) {
        super(name);
        this.format = format;
        this.query = query;
        this.queryByteString = queryByteString;

        this.lock = new ReentrantReadWriteLock();
        this.blocks = new TreeMap();
    }

    /*public void addBlock(long blockId, int blockSize) {
        this.lock.writeLock().lock();
        try {
            if (!this.blocks.containsKey(blockId)) {
                this.blocks.put(blockId, blockSize);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }*/
    public void addBlocks(Map<Long, Integer> blocks) {
        this.lock.writeLock().lock();
        try {
            for (Map.Entry<Long, Integer> entry : blocks.entrySet()) {
                this.blocks.put(entry.getKey(), entry.getValue());
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void initialize(Inflator inflator,
            Map<Integer, List<Integer>> replicas) {
        this.inflator = inflator;
        this.replicas = replicas;
    }

    public boolean isComplete() {
        this.lock.readLock().lock();
        try {
            Set<Integer> nodeIds = new HashSet();
            for (long blockId : this.blocks.keySet()) {
                nodeIds.add(BlockManager.getNodeId(blockId));
            }

            return nodeIds.size() == this.replicas.size();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Map<Long, Integer> getBlocks() {
        return this.blocks;
    }

    public Format getFormat() {
        return this.format;
    }

    public Inflator getInflator() {
        return this.inflator;
    }

    public Map<Integer, List<Integer>> getReplicas() {
        return this.replicas;
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public void buildProtobuf(File.Builder builder) {
        /*builder.setQuery(this.queryByteString);
        builder.setFileFormat(this.format.getFileFormat());*/
    }

    @Override
    public FileType getFileType() {
        return FileType.REGULAR;
    }

    @Override
    public long getSize() {
        this.lock.readLock().lock();
        try {
            long size = 0;
            for (int blockSize : this.blocks.values()) {
                size += blockSize;
            }

            return size;
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
