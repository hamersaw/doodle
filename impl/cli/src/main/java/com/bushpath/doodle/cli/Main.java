package com.bushpath.doodle.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "doodle",
    version = "0.1",
    description = "cli application to interface with doodle",
    mixinStandardHelpOptions = true,
    subcommands = {ControlCli.class, DataCli.class,
        FileSystemCli.class, NodeCli.class, PluginCli.class,
        ProfileCli.class, SketchCli.class})
public class Main implements Runnable {
    @Option(names={"-i", "--ip-address"},
        description="IP address of doodle node [default=\"127.0.0.1\"].")
    public static String ipAddress = "127.0.0.1";

    @Option(names={"-p", "--port"},
        description="Port of doodle node [default=\"5450\"].")
    public static short port = 5450;

    public static void main(String[] args) {
        CommandLine.run(new Main(), System.err, args);
    }

    @Override
    public void run() {
        CommandLine.usage(new Main(), System.out);
        return;
    }
}
