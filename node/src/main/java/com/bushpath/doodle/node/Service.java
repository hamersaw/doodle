package com.bushpath.doodle.node;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface Service {
    public int[] getMessageTypes();
    public void handleMessage(int messageType, DataInputStream in,
        DataOutputStream out) throws Exception;
}
