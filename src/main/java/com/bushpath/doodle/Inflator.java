package com.bushpath.doodle;

import com.google.protobuf.ByteString;

import java.util.List;

public abstract class Inflator {
    public abstract List<float[]> process(ByteString byteString)
        throws Exception;
}
