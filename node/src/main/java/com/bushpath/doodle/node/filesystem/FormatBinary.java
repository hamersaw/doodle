package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;

import java.io.DataOutputStream;

public class FormatBinary implements Format {
    public FormatBinary() {
    }

    @Override
    public int format(float[] observation, DataOutputStream out)
            throws Exception {
        // TODO - write observation to out
        for (float f : observation) {
            out.writeFloat(f);
        }

        return (int) this.length(observation.length, 1);
    }

    @Override
    public FileFormat getFileFormat() {
        return FileFormat.BINARY;
    }

    @Override
    public long length(int featureCount, int observationCount) {
        return 8 * featureCount * observationCount;
    }
}
