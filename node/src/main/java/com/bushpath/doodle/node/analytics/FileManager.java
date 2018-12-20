package com.bushpath.doodle.node.analytics;

import com.bushpath.doodle.protobuf.DoodleProtos.Operation;
import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class FileManager {
    protected static final Logger log =
        LoggerFactory.getLogger(FileManager.class);

    protected Map<Integer, DoodleInode> inodes;
    protected Map<Long, FileOperation> operations;
    protected ReadWriteLock lock;
    protected Random random;

    public FileManager() {
        this.inodes = new HashMap();
        this.operations = new TreeMap();
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random(System.nanoTime());

        // add root directory to inodes
        DoodleInode inode =
            this.create(FileType.DIRECTORY, "root", "root", "");

        this.inodes.put(2, inode);
    }

    public void add(String user, String group, String path,
            int value, DoodleInode inode) throws Exception {
        this.lock.writeLock().lock();
        try {
            // parse path
            List<String> elements = this.parsePath(path);
            String filename = elements.remove(elements.size() - 1);

            // retrieve parent directory
            DoodleEntry doodleEntry = 
                this.getInode(user, group, elements).getEntry();
            if (!(doodleEntry instanceof DoodleDirectory)) {
                throw new RuntimeException("TODO - not directory");
            }

            DoodleDirectory directory = (DoodleDirectory) doodleEntry;

            // add inode
            this.inodes.put(value, inode);
            directory.put(filename, value);

            // add to operations
            long timestamp = System.currentTimeMillis();
            FileOperation fileOperation = FileOperation.newBuilder()
                .setTimestamp(timestamp)
                .setInode(value)
                .setPath(path)
                .setFile(inode.toProtobuf())
                .setOperation(Operation.ADD)
                .build();

            this.operations.put(timestamp, fileOperation);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public DoodleInode create(FileType fileType, String user,
            String group, String path) {
        long time = System.currentTimeMillis();
        return this.create(fileType, user,
            group, path, 0, time, time, time);
    }

    public DoodleInode create(FileType fileType, String user,
            String group, String path, long size, long changeTime,
            long modificationTime, long accessTime) {
        List<String> elements = this.parsePath(path);
        String filename = elements.remove(elements.size() - 1);

        // create entry
        DoodleEntry entry = null;
        switch (fileType) {
            case DIRECTORY:
                entry = new DoodleDirectory(filename);
                break;
            case REGULAR:
                entry = new DoodleFile(filename);
                break;
        }

        // create inode
        return new DoodleInode(fileType, user, group, size,
            changeTime, modificationTime, accessTime, entry);
    }

    public void delete(String user, String group,
            String path) throws Exception {
        this.lock.writeLock().lock();
        try {
            // parse path
            List<String> elements = this.parsePath(path);
            String filename = elements.remove(elements.size() - 1);

            // retrieve parent directory
            DoodleEntry parentEntry = 
                this.getInode(user, group, elements).getEntry();
            if (!(parentEntry instanceof DoodleDirectory)) {
                throw new RuntimeException("TODO - not directory");
            }

            DoodleDirectory parentDirectory =
                (DoodleDirectory) parentEntry;

            // get inode
            int value = parentDirectory.get(filename);
            DoodleInode inode = this.inodes.get(value);

            // check if inode is a non-empty directory
            if (inode.getFileType() == FileType.DIRECTORY) {
                DoodleDirectory directory =
                    (DoodleDirectory) inode.getEntry();

                if (directory.getInodes().size() != 0) {
                    throw new RuntimeException("Directory is not empty");
                }
            }

            // delete inode
            parentDirectory.remove(filename);
            this.inodes.remove(inode);

            // add to operations
            long timestamp = System.currentTimeMillis();
            FileOperation fileOperation = FileOperation.newBuilder()
                .setTimestamp(timestamp)
                .setInode(value)
                .setPath(path)
                .setFile(inode.toProtobuf())
                .setOperation(Operation.DELETE)
                .build();

            this.operations.put(timestamp, fileOperation);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    protected DoodleInode getInode(String user, String group,
            List<String> elements) throws Exception {
        // get root entry
        DoodleInode inode = inodes.get(2);
        if (elements.size() == 1 && elements.get(0).isEmpty()) {
            return inode;
        }

        // traverse elements
        for (int i=0; i<elements.size(); i++) {
            // check if inode is a directory
            if (inode.getFileType() != FileType.DIRECTORY) {
                throw new RuntimeException("TODO - entry not directory");
            }
 
            // check if directory contains entry
            DoodleDirectory directory =
                (DoodleDirectory) inode.getEntry();
            if (!directory.contains(elements.get(i))) {
                throw new RuntimeException("TODO - element missing");
            }

            // get entry inode
            inode = this.inodes.get(directory.get(elements.get(i)));
        }

        return inode;
    }

    public Set<Map.Entry<Integer, DoodleInode>> getInodeEntrySet() {
        this.lock.readLock().lock();
        try {
            return this.inodes.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Set<Map.Entry<Long, FileOperation>> getOperationsEntrySet() {
        this.lock.readLock().lock();
        try {
            return this.operations.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Collection<DoodleInode> list(String user, String group,
            String path) throws Exception {
        this.lock.readLock().lock();
        try {
            // parse path
            List<String> elements = this.parsePath(path);

            // retrieve inode
            DoodleInode inode = this.getInode(user, group, elements);

            // populate list
            List<DoodleInode> list = new ArrayList();
            switch (inode.getFileType()) {
                case DIRECTORY:
                    DoodleDirectory directory =
                        (DoodleDirectory) inode.getEntry();
                    for (int value : directory.getInodes()) {
                        list.add(this.inodes.get(value));
                    }

                    break;
                case REGULAR:
                    list.add(inode);
                    break;
            }

            return list;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    protected List<String> parsePath(String path) {
        List<String> list = new ArrayList();
        for (String element : path.replaceAll("/$", "")
                .replaceAll("^/", "").split("/")) {
            list.add(element);
        }

        return list;
    }

    @Override
    public int hashCode() {
        CRC32 crc32 = new CRC32();

        this.lock.readLock().lock();
        try {
            for (FileOperation operation : this.operations.values()) {
                crc32.update((int) operation.getTimestamp());
                crc32.update(operation.getPath().getBytes());
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }

    public int filesHashCode() {
        CRC32 crc32 = new CRC32();

        this.lock.readLock().lock();
        try {
            for (Map.Entry<Integer, DoodleInode> entry :
                    this.inodes.entrySet()) {
                // skip root node
                if (entry.getKey() == 2) {
                    continue;
                }

                crc32.update(entry.getKey());

                DoodleInode inode = entry.getValue();
                crc32.update(inode.getUser().getBytes());
                crc32.update(inode.getGroup().getBytes());
                crc32.update((int) inode.getSize());
                crc32.update((int) inode.getChangeTime());
                crc32.update((int) inode.getModificationTime());
                crc32.update((int) inode.getAccessTime());

                // TODO - add observations for files
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }
}
