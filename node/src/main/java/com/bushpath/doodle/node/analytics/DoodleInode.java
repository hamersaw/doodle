package com.bushpath.doodle.node.analytics;

public class DoodleInode {
    protected FileType fileType;
    protected String user;
    protected String group;
    protected long size;
    protected long inodeChangeTime;
    protected long inodeModificationTime;
    protected long inodeAccessTime;
    protected int blockCount;
    protected DoodleEntry entry;

    public DoodleInode(FileType fileType, String user,
            String group, DoodleEntry entry) {
        this.fileType = fileType;
        this.user = user;
        this.group = group;
        this.size = 0;
        this.inodeChangeTime = System.currentTimeMillis();
        this.inodeModificationTime = this.inodeChangeTime;
        this.inodeAccessTime = this.inodeChangeTime;
        this.blockCount = 0;
        this.entry = entry;
    }

    public DoodleEntry getEntry() {
        return this.entry;
    }

    public FileType getFileType() {
        return this.fileType;
    }

    public enum FileType {
        DIRECTORY,
        REGULAR
    }
}
