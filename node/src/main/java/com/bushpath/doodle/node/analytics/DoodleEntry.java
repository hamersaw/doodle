package com.bushpath.doodle.node.analytics;

public abstract class DoodleEntry {
    protected String name;

    public DoodleEntry(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }
}
