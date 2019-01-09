package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.protobuf.DoodleProtos.File;

public abstract class DoodleEntry {
    protected String name;

    public DoodleEntry(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public abstract void update(File file) throws Exception;
}
