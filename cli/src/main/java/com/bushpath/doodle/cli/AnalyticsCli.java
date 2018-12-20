package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "analytics",
    description = "Configure files on the analytics plane.",
    mixinStandardHelpOptions = true,
    subcommands = {AnalyticsAddCli.class, AnalyticsDeleteCli.class,
        AnalyticsListCli.class})
public class AnalyticsCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new AnalyticsCli(), System.out);
        return;
    }
}
