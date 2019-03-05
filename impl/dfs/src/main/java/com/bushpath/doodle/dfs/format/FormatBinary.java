package com.bushpath.doodle.dfs.format;

import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;

import java.io.DataOutputStream;

public class FormatBinary implements Format {
    public FormatBinary() {
    }

    @Override
    public void format(float[] observation, DataOutputStream out)
            throws Exception {
        for (float f : observation) {
            out.writeFloat(f);
        }
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
