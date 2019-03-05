package com.bushpath.doodle.node.filesystem;

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
        DoodleDirectory directory = new DoodleDirectory("");
        DoodleInode inode =
            new DoodleInode(2, "root", "root", 0, 0, 0, directory);

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

            log.info("Added file '{}' with inode:{}", path, value);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void addOperation(FileOperation operation)
            throws Exception {
        this.operations.put(operation.getTimestamp(), operation);
    }

    public boolean containsOperation(long timestamp) {
        return this.operations.containsKey(timestamp);
    }

    public DoodleInode delete(String user, String group,
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

            log.info("Deleted file '{}' with inode:{}", path, value);

            return inode;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public DoodleInode getInode(int value) {
        this.lock.readLock().lock();
        try {
            return this.inodes.get(value);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public DoodleInode getInode(String user, String group, String path)
            throws Exception {
        return this.getInode(user, group, this.parsePath(path));
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
                throw new RuntimeException("TODO - element missing '"
                    + elements.get(i) + "'");
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

    public String parseFilename(String path) {
        List<String> elements = this.parsePath(path);
        return elements.get(elements.size() - 1);
    }

    protected List<String> parsePath(String path) {
        List<String> list = new ArrayList();
        for (String element : path.replaceAll("/$", "")
                .replaceAll("^/", "").split("/")) {
            list.add(element);
        }

        return list;
    }

    public void update(File file) throws Exception {
        this.lock.writeLock().lock();
        try {
            // check if inode exists
            if (!this.inodes.containsKey(file.getInode())) {
                throw new RuntimeException("Unable to update inode '"
                    + file.getInode() + "', it does not exist");
            }

            this.inodes.get(file.getInode()).update(file);
        } finally {
            this.lock.writeLock().unlock();
        }
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

                // add observations for files
                if (inode.getFileType() == FileType.REGULAR) {
                    DoodleFile file = (DoodleFile) inode.getEntry();
                    for (Map.Entry<Integer, Integer> observations :
                            file.getObservationEntrySet()) {
                        crc32.update(observations.getKey());
                        crc32.update(observations.getValue());
                    }
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }
}
