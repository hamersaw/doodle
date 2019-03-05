package com.bushpath.doodle.cli;

import com.bushpath.doodle.protobuf.DoodleProtos.FileFormat;

import picocli.CommandLine.ITypeConverter;

public class FileFormatConverter implements ITypeConverter<FileFormat> {
    public FileFormatConverter() {
    }

    @Override
    public FileFormat convert(String value) throws Exception {
        switch (value) {
            case "BINARY":
                return FileFormat.BINARY;
            case "CSV":
                return FileFormat.CSV;
            default:
                throw new RuntimeException("unknown file format");
        }
    }
}
