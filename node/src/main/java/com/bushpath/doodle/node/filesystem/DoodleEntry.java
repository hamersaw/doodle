package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

public abstract class DoodleEntry {
    protected String name;

    public DoodleEntry(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public abstract void buildProtobuf(File.Builder builder);
    public abstract FileType getFileType();
    public abstract long getSize();
    public abstract void update(File file) throws Exception;
}
