package com.bushpath.doodle.node;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;

import com.moandjiezana.toml.Toml;

import org.reflections.Reflections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.plugin.PluginManager;
import com.bushpath.doodle.node.plugin.PluginService;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    protected static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // check arguments
        if (args.length != 1) {
            System.err.println("Usage: <config-file>");
            System.exit(1);
        }

        // parse configuration file
        Toml toml = new Toml();
        try {
            toml.read(new File(args[0]));
        } catch (Exception e) {
            log.error("Failed to parse configuration file", e);
            System.exit(2);
        }

        // initialize Server
        Server server = new Server(
                toml.getLong("control.port").shortValue(),
                toml.getLong("control.threadCount").shortValue()
            );

        // initialize PluginManager
        PluginManager pluginManager = new PluginManager();
        try {
            // find plugin jars
            Object[] paths =
                Files.walk(Paths.get(toml.getString("plugins.directory")))
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.endsWith("jar"))
                    .toArray();

            URL[] urls = new URL[paths.length];
            for (int i=0; i<urls.length; i++) {
                String absoluteName = ((Path) paths[i]).toUri().toString();

                log.info("Loading JAR file '{}'", absoluteName);
                urls[i] = new URL(absoluteName);
            }

            // initialize new class loader as child
            URLClassLoader urlClassLoader =
                new URLClassLoader(urls, Main.class.getClassLoader());

            // must set ContextClassLoader of current thread
            // it is adopted by all threads created by this (ex. services)
            Thread.currentThread().setContextClassLoader(urlClassLoader);

            Reflections reflections = new Reflections();

            // register gossip plugins
            for (Class<? extends ControlPlugin> clazz :
                    reflections.getSubTypesOf(ControlPlugin.class)) {
                pluginManager.registerControlPlugin(clazz);
            }

            // reguster sketch plugins
            for (Class<? extends SketchPlugin> clazz :
                    reflections.getSubTypesOf(SketchPlugin.class)) {
                // TODO - pluginManager.registerSketchPlugin(clazz);
            }
        } catch (IOException e) {
            log.error("Unknown plugin loading failure", e);
            System.exit(3);
        }

        // register Services
        try {
            PluginService pluginService = new PluginService(pluginManager);
            server.registerService(pluginService);
        } catch (Exception e) {
            log.error("Unknwon Service registration failure", e);
            System.exit(4);
        }

        // start Server
        try {
            server.start();

            server.join();
        } catch (InterruptedException e) {
            log.error("Unknown failure", e);
        }
    }
}
