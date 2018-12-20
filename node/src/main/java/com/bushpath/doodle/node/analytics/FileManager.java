package com.bushpath.doodle.node.analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileManager {
    protected static final Logger log =
        LoggerFactory.getLogger(FileManager.class);

    protected Map<Integer, DoodleInode> inodes;
    protected ReadWriteLock lock;
    protected Random random;

    public FileManager() {
        this.inodes = new HashMap();
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random(System.nanoTime());

        // add root directory to inodes
        DoodleDirectory directory = new DoodleDirectory("");
        DoodleInode inode =
            new DoodleInode(DoodleInode.FileType.DIRECTORY,
                "root", "root", directory);

        this.inodes.put(2, inode);
    }

    public void add(DoodleInode.FileType fileType, String user,
            String group, String path) throws Exception {
        this.lock.writeLock().lock();
        try {
            // retrieve parent directory
            String[] elements = this.parsePath(path);
            DoodleDirectory directory =
                this.getParentDirectory(user, group, elements);

            // create inode
            String filename = elements[elements.length - 1];
            DoodleEntry entry = null;
            switch (fileType) {
                case DIRECTORY:
                    entry = new DoodleDirectory(filename);
                    break;
                case REGULAR:
                    entry = new DoodleFile(filename);
                    break;
            }

            // add inode
            int value = this.random.nextInt();
            DoodleInode inode =
                new DoodleInode(fileType, user, group, entry);
            this.inodes.put(value, inode);
            directory.put(filename, value);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void delete(String user, String group,
            String path) throws Exception {
        throw new RuntimeException("unimplemented");
    }

    protected DoodleDirectory getParentDirectory(String user,
            String group, String[] elements) throws Exception {
        // get root entry
        DoodleDirectory directory =
            (DoodleDirectory) inodes.get(2).getEntry();

        // find parent directory
        for (int i=0; i<elements.length - 1; i++) {
            // check if directory contains entry
            if (!directory.contains(elements[i])) {
                throw new RuntimeException("TODO - element missing");
            }

            // get entry inode
            DoodleInode inode =
                this.inodes.get(directory.get(elements[i]));
            switch (inode.getFileType()) {
                case DIRECTORY:
                    directory = (DoodleDirectory) inode.getEntry();
                    break;
                case REGULAR:
                    throw new RuntimeException("TODO - found file");
            }
        }

        return directory;
    }

    public Collection<DoodleInode> list(String user, String group,
            String path) throws Exception {
        throw new RuntimeException("unimplemented");
    }

    protected String[] parsePath(String path) {
        return path.replaceAll("/$", "")
            .replaceAll("^/", "").split("/");
    }
}
