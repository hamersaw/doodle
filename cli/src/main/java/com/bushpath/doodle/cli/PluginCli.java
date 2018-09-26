package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "plugin",
    description = "View available plugins.",
    mixinStandardHelpOptions = true,
    subcommands = {PluginListCli.class})
public class PluginCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new PluginCli(), System.out);
        return;
    }
}
