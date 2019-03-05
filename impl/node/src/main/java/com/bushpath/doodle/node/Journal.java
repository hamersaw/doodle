package com.bushpath.doodle.node;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Journal {
    protected String directory;
    protected TreeMap<Long, String> completedFiles;
    protected int maximumFileSize;
    protected ReadWriteLock lock;
    protected DataOutputStream out;
    protected String filename;

    public Journal(String directory, int maximumFileSize) {
        this.directory = directory;
        this.completedFiles = new TreeMap();
        this.maximumFileSize = maximumFileSize;
        this.lock = new ReentrantReadWriteLock();

        // make directory if doesn't exist
        File file = new File(directory);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    protected DataOutputStream lockWriteOutputStream()
            throws Exception {
        this.lock.writeLock().lock();

        // create DataOutputStream if needed
        if (this.out == null) {
            this.filename = this.directory + "/"
                + UUID.randomUUID().toString() + ".bin";
            this.out = new DataOutputStream(
                new FileOutputStream(this.filename));
        }

        return this.out;
    }

    protected void unlockWriteOutputStream(long timestamp)
            throws Exception {
        // flush current data and check size
        this.out.flush();
        if (this.out.size() >= this.maximumFileSize) {
            this.out.close();
            this.out = null;

            this.completedFiles.put(timestamp, this.filename);
        }

        this.lock.writeLock().unlock();
    }
}
