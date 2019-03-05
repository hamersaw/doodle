package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "node",
    description = "View node status.",
    mixinStandardHelpOptions = true,
    subcommands = {NodeListCli.class, NodeShowCli.class})
public class NodeCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new NodeCli(), System.out);
        return;
    }
}
