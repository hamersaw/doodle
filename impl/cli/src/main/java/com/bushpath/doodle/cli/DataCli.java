package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "data",
    description = "Insert / query data to / from sketches.",
    mixinStandardHelpOptions = true,
    subcommands = {DataInsertCli.class, DataQueryCli.class})
public class DataCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new DataCli(), System.out);
        return;
    }
}
