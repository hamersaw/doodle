package com.bushpath.doodle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public abstract class ControlPlugin extends Plugin {
    public ControlPlugin(String id) {
        super(id);
    }

    public ControlPlugin(DataInputStream in) throws IOException {
        super(in);
    }

    protected void serializeControlPlugin(DataOutputStream out)
            throws IOException {
        this.serializePlugin(out);
    }

    public abstract Map<Integer, Set<Integer>>
        getReplicas(int replicationFactor);
    public abstract void serialize(DataOutputStream out)
        throws IOException;
}
