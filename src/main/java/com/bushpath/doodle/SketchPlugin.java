package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public abstract class SketchPlugin extends Plugin {
    protected String id;

    public SketchPlugin(String id) {
        this.id = id;
    }

    public abstract void write(ByteString byteString) throws Exception;
    public abstract int[] indexFeatures(List<String> features);
    public abstract Transform getTransform(BlockingQueue<ByteString> in,
        BlockingQueue<SketchWriteRequest> out);
}
