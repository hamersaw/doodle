package com.bushpath.doodle.node.analytics;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

public class DoodleInode {
    protected FileType fileType;
    protected String user;
    protected String group;
    protected long size;
    protected long changeTime;
    protected long modificationTime;
    protected long accessTime;
    protected DoodleEntry entry;

    public DoodleInode(FileType fileType, String user,
            String group, DoodleEntry entry) {
        this.fileType = fileType;
        this.user = user;
        this.group = group;
        this.size = 0;
        this.changeTime = System.currentTimeMillis();
        this.modificationTime = this.changeTime;
        this.accessTime = this.changeTime;
        this.entry = entry;
    }

    public DoodleEntry getEntry() {
        return this.entry;
    }

    public FileType getFileType() {
        return this.fileType;
    }

    public File toProtobuf() {
        return File.newBuilder()
            .setFileType(this.fileType)
            .setUser(this.user)
            .setGroup(this.group)
            .setName(this.entry.getName())
            .setChangeTime(this.changeTime)
            .setModificationTime(this.modificationTime)
            .setAccessTime(this.accessTime)
            .build();
    }
}
