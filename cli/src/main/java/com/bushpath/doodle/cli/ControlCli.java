package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "control",
    description = "Configure control plane.",
    mixinStandardHelpOptions = true,
    subcommands = {ControlInitCli.class, ControlListCli.class})
public class ControlCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new ControlCli(), System.out);
        return;
    }
}
