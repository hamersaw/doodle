package com.bushpath.doodle.node.filesystem;

import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;

import java.io.DataOutputStream;

public class FormatCsv implements Format {
    protected int precision;
    protected char delimiter;

    public FormatCsv(int precision, char delimiter) {
        this.precision = precision;
        this.delimiter = delimiter;
    }

    @Override
    public int format(float[] observation, DataOutputStream out)
            throws Exception {
        // TODO - write observation to out
        return (int) this.length(observation.length, 1);
    }

    @Override
    public FileFormat getFileFormat() {
        return FileFormat.CSV;
    }

    @Override
    public long length(int featureCount, int observationCount) {
        return (this.precision + 1) * featureCount * observationCount;
    }
}
