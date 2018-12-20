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

    public DoodleInode(FileType fileType, String user, String group,
            long size, long changeTime, long modificationTime,
            long accessTime, DoodleEntry entry) {
        this.fileType = fileType;
        this.user = user;
        this.group = group;
        this.size = size;
        this.changeTime = changeTime;
        this.modificationTime = modificationTime;
        this.accessTime = accessTime;
        this.entry = entry;
    }

    public FileType getFileType() {
        return this.fileType;
    }

    public String getUser() {
        return this.user;
    }

    public String getGroup() {
        return this.group;
    }

    public long getSize() {
        return this.size;
    }

    public long getChangeTime() {
        return this.changeTime;
    }

    public long getModificationTime() {
        return this.modificationTime;
    }

    public long getAccessTime() {
        return this.accessTime;
    }

    public DoodleEntry getEntry() {
        return this.entry;
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
