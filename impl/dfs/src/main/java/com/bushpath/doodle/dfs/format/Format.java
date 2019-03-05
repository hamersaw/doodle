package com.bushpath.doodle.dfs.format;

import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;

import java.io.DataOutputStream;

public interface Format {
    public abstract long length(int featureCount, int observationCount);
    public abstract void format(float[] observation,
        DataOutputStream out) throws Exception;
    public abstract FileFormat getFileFormat();

    public static Format getFormat(FileFormat fileFormat) {
        switch (fileFormat) {
            case BINARY:
                return new FormatBinary();
            case CSV:
                return new FormatCsv(10, ',');
        }

        return null; // unreachable
    }
}
