package com.bushpath.doodle.node.analytics;

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
}
