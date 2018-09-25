package com.bushpath.doodle.node.plugin;

import com.bushpath.doodle.ControlPlugin;
import com.bushpath.doodle.SketchPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PluginManager {
    protected static final Logger log =
        LoggerFactory.getLogger(PluginManager.class);

    protected Map<String, Class<? extends ControlPlugin>> controlPlugins;
    protected Map<String, Class<? extends SketchPlugin>> sketchPlugins;

    public PluginManager() {
        this.controlPlugins = new HashMap();
        this.sketchPlugins = new HashMap();
    }

    public Set<String> getControlPlugins() {
        return this.controlPlugins.keySet();
    }

    public Set<String> getSketchPlugins() {
        return this.sketchPlugins.keySet();
    }

    public void registerControlPlugin(Class<? extends ControlPlugin> clazz)
            throws Exception {
        // check if already registered
        if (this.controlPlugins.containsKey(clazz.getName())) {
            throw new RuntimeException("ControlPlugin '"
                + clazz.getName() + "' has already been registered");
        }

        // register ControlPlugin
        this.controlPlugins.put(clazz.getName(), clazz);
        log.info("Registered ControlPlugin '" + clazz.getName() + "'");
    }

    public void registerSketchPlugin(Class<? extends SketchPlugin> clazz)
            throws Exception {
        // check if already registered
        if (this.sketchPlugins.containsKey(clazz.getName())) {
            throw new RuntimeException("SketchPlugin '"
                + clazz.getName() + "' has already been registered");
        }

        // register SketchPlugin
        this.sketchPlugins.put(clazz.getName(), clazz);
        log.info("Registered SketchPlugin '" + clazz.getName() + "'");
    }
}
