package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "sketch",
    description = "Configure sketch plane.",
    mixinStandardHelpOptions = true,
    subcommands = {SketchInitCli.class, SketchListCli.class,
        SketchModifyCli.class, SketchShowCli.class})
public class SketchCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new SketchCli(), System.out);
        return;
    }
}
