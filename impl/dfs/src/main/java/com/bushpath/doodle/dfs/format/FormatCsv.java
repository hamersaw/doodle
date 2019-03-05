package com.bushpath.doodle.dfs.format;

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
    public void format(float[] observation, DataOutputStream out)
            throws Exception {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i=0; i<observation.length; i++) {
            // TODO - optimize this         
            String s = Double.toString(observation[i]);
            int length = s.length();
            if (length > this.precision) {
                s = s.substring(0, this.precision);
            } else if (length < this.precision) {
                s = String.format("%1$-" + this.precision + "s", s)
                    .replace(' ', '0');
            }

            stringBuilder.append((i != 0 ? this.delimiter : "") + s); 
        }

        stringBuilder.append("\n");
        out.write(stringBuilder.toString().getBytes());
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
