package com.bushpath.doodle;

import com.bushpath.doodle.protobuf.DoodleProtos.SketchWriteRequest;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public abstract class SketchPlugin extends Plugin {
    public abstract Transform getTransform(BlockingQueue<List<Float>> in,
        BlockingQueue<SketchWriteRequest> out);
}
