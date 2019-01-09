package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "fs",
    description = "Configure the file system.",
    mixinStandardHelpOptions = true,
    subcommands = {FileSystemCreateCli.class, FileSystemDeleteCli.class,
        FileSystemListCli.class, FileSystemMkdirCli.class})
public class FileSystemCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new FileSystemCli(), System.out);
        return;
    }
}
