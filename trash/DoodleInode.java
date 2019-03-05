package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.protobuf.DoodleProtos.File;
import com.bushpath.doodle.protobuf.DoodleProtos.FileType;

public class DoodleInode {
    protected int inodeValue;
    protected String user;
    protected String group;
    protected long changeTime;
    protected long modificationTime;
    protected long accessTime;
    protected DoodleEntry entry;

    public DoodleInode(int inodeValue, String user,
            String group, long changeTime, long modificationTime,
            long accessTime, DoodleEntry entry) {
        this.inodeValue = inodeValue;
        this.user = user;
        this.group = group;
        this.changeTime = changeTime;
        this.modificationTime = modificationTime;
        this.accessTime = accessTime;
        this.entry = entry;
    }

    public int getInodeValue() {
        return this.inodeValue;
    }

    public FileType getFileType() {
        return this.entry.getFileType();
    }

    public String getUser() {
        return this.user;
    }

    public String getGroup() {
        return this.group;
    }

    public long getSize() {
        return this.entry.getSize();
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

    public void update(File file) throws Exception {
        // update inode
        if (file.getChangeTime() > this.changeTime) {
            this.user = file.getUser();
            this.group = file.getGroup();
            this.changeTime = file.getChangeTime();
        }

        // update entry
        if (file.getModificationTime() > this.modificationTime) {
            this.entry.update(file);
            this.modificationTime = file.getModificationTime();
        }

        // update access
        if (file.getAccessTime() > this.accessTime) {
            this.accessTime = file.getAccessTime();
        }

        this.entry.update(file);
    }

    public File toProtobuf() {
        File.Builder builder = File.newBuilder()
            .setInode(this.inodeValue)
            .setFileType(this.getFileType())
            .setUser(this.user)
            .setGroup(this.group)
            .setName(this.entry.getName())
            .setSize(this.entry.getSize())
            .setChangeTime(this.changeTime)
            .setModificationTime(this.modificationTime)
            .setAccessTime(this.accessTime);

        this.entry.buildProtobuf(builder);
        return builder.build();
    }
}
