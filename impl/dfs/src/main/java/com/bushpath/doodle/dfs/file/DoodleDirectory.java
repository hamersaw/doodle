package com.bushpath.doodle.dfs.file;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DoodleDirectory extends DoodleEntry {
    protected Map<String, Integer> inodes;

    public DoodleDirectory(String name) {
        super(name);
        this.inodes = new HashMap();
    }

    public boolean contains(String entry) {
        return this.inodes.containsKey(entry);
    }

    public int get(String entry) {
        return this.inodes.get(entry);
    }

    public Collection<Integer> getInodes() {
        return this.inodes.values();
    }

    public void put(String entry, int inode) {
        this.inodes.put(entry, inode);
    }

    public void remove(String entry) {
        this.inodes.remove(entry);
    }

    @Override
    public void buildProtobuf(File.Builder builder) {
    }

    @Override
    public FileType getFileType() {
        return FileType.DIRECTORY;
    }

    @Override
    public long getSize() {
        return 4096;
    }

    @Override
    public void update(File file) throws Exception {
        // TODO - perhaps update with inodes allowing mv operations
    }
}