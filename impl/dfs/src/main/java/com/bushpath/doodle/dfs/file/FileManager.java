package com.bushpath.doodle.dfs.file;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileOperation;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import com.bushpath.rutils.query.Query;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.dfs.format.Format;

import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileManager {
    protected static final Logger log =
        LoggerFactory.getLogger(FileManager.class);

    protected Map<Integer, DoodleInode> inodes;
    protected ReadWriteLock lock;

    public FileManager() {
        this.inodes = new HashMap();
        this.lock = new ReentrantReadWriteLock();

        // add root directory to inodes
        DoodleDirectory directory = new DoodleDirectory("");
        DoodleInode inode =
            new DoodleInode(2, "root", "root", 0, 0, 0, directory);

        this.inodes.put(2, inode);
    }

    public Set<Map.Entry<Integer, DoodleInode>> getEntrySet() {
        this.lock.readLock().lock();
        try {
            return this.inodes.entrySet();
        } finally {
            this.lock.readLock().unlock();
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

    protected String parseFilename(String path) {
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

    public void handleOperation(FileOperation operation)
            throws Exception {
        File file = operation.getFile();

        // check if parent direcotry exists
        DoodleDirectory parentDirectory = null;
        this.lock.readLock().lock();
        try {
            // parse path
            List<String> elements =
                this.parsePath(operation.getPath());
            String filename =
                elements.remove(elements.size() - 1);

            // retrieve parent directory
            DoodleEntry doodleEntry = this.getInode(
                file.getUser(), file.getGroup(), elements).getEntry();

            if (!(doodleEntry instanceof DoodleDirectory)) {
                throw new RuntimeException("TODO - not directory ");
            }

            parentDirectory = (DoodleDirectory) doodleEntry;
        } finally {
            this.lock.readLock().unlock();
        }

        // perform operation
        String filename = this.parseFilename(operation.getPath());
        switch(operation.getOperationType()) {
            case ADD:
                // create inode
                DoodleEntry doodleEntry = null;
                switch (file.getFileType()) {
                    case DIRECTORY:
                        doodleEntry = new DoodleDirectory(filename);
                        break;
                    case REGULAR:
                        // parse query
                        ByteString data = file.getQuery();
                        ObjectInputStream objectIn =
                            new ObjectInputStream(data.newInput());
                        Query query = (Query) objectIn.readObject();
                        objectIn.close();

                        // create inode
                        Format format =
                            Format.getFormat(file.getFileFormat());
                        doodleEntry = new DoodleFile(filename,
                            format, query, data, -1);
                        // TODO - featureCount
                        break;
                }

                DoodleInode doodleInode = new DoodleInode(
                    file.getInode(), file.getUser(), file.getGroup(),
                    file.getChangeTime(), file.getModificationTime(),
                    file.getAccessTime(), doodleEntry);

                // add inode
                this.lock.writeLock().lock();
                try {
                    // add inode
                    this.inodes.put(file.getInode(), doodleInode);
                    parentDirectory.put(filename, file.getInode());

                    log.info("Added file '{}' with inode:{}",
                        operation.getPath(), file.getInode());
                } finally {
                    this.lock.writeLock().unlock();
                }

                break;
            case DELETE:
                // get inode
                int inodeValue = parentDirectory.get(filename);
                DoodleInode inode = this.inodes.get(inodeValue);

                // check if inode is a non-empty directory
                if (inode.getFileType() == FileType.DIRECTORY) {
                    DoodleDirectory directory =
                        (DoodleDirectory) inode.getEntry();

                    if (directory.getInodes().size() != 0) {
                        throw new
                            RuntimeException("Directory is not empty");
                    }
                }

                this.lock.writeLock().lock();
                try {
                    // delete inode
                    parentDirectory.remove(filename);
                    this.inodes.remove(inodeValue);

                    log.info("Deleted file '{}' with inode:{}",
                        operation.getPath(), inodeValue);
                } finally {
                    this.lock.writeLock().unlock();
                }

                break;
            default:
                // TODO
                break;
        }
    }
}
