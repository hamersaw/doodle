package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;

@Command(name = "fs",
    description = "Configure the file system.",
    mixinStandardHelpOptions = true,
    subcommands = {FileSystemCreateCli.class,
        FileSystemListCli.class, FileSystemMkdirCli.class})
public class FileSystemCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new FileSystemCli(), System.out);
        return;
    }

    public static String parseFilename(String path) {
        List<String> list = new ArrayList();
        for (String element : path.replaceAll("/$", "")
                .replaceAll("^/", "").split("/")) {
            list.add(element);
        }

        return list.get(list.size() - 1);
    }
}
