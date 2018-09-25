package com.bushpath.doodle.node.plugin;

import com.bushpath.doodle.node.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class PluginService implements Service {
    @Override
    public int[] getMessageTypes() {
        return new int[]{};
    }

    @Override
    public void handleMessage(int messageType,
        DataInputStream in, DataOutputStream out) throws Exception  {
    }
}
