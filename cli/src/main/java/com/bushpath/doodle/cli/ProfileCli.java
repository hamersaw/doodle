package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "profile",
    description = "Profile various aspects of SketchFS.",
    mixinStandardHelpOptions = true,
    subcommands = {ProfileQueryCli.class})
public class ProfileCli implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(new ProfileCli(), System.out);
        return;
    }
}
