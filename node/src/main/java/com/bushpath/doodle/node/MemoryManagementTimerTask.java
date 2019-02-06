package com.bushpath.doodle.node;

import com.bushpath.doodle.SketchPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.doodle.node.control.NodeManager;
import com.bushpath.doodle.node.sketch.SketchManager;

import java.util.Collection;
import java.util.Map;
import java.util.TimerTask;

public class MemoryManagementTimerTask extends TimerTask {
    protected static final Logger log =
        LoggerFactory.getLogger(MemoryManagementTimerTask.class);

    protected NodeManager nodeManager;
    protected SketchManager sketchManager;
    protected long writeDiffMilliSeconds;

    public MemoryManagementTimerTask(NodeManager nodeManager,
            SketchManager sketchManager,
            long writeDiffMilliSeconds) {
        this.nodeManager = nodeManager;
        this.sketchManager = sketchManager;
        this.writeDiffMilliSeconds = writeDiffMilliSeconds;
    }

    @Override
    public void run() {
        int thisNodeId = this.nodeManager.getThisNodeId();

        // TODO - check if memory is over a specified threshold

        // flush sketches based on diff since last write
        for (Map.Entry<String, SketchPlugin> entry :
                this.sketchManager.getEntrySet()) {
            SketchPlugin sketchPlugin = entry.getValue();

            // get node IDs for all replicas
            Collection<Integer> nodeIds =
                sketchPlugin.getPrimaryReplicas(thisNodeId);
            nodeIds.add(thisNodeId);

            // check duration since last checkpoint
            for (Integer nodeId : nodeIds) {
                long writeTimestamp =
                    sketchPlugin.getWriteTimestamp(nodeId);
                long flushTimestamp =
                    sketchPlugin.getFlushTimestamp(nodeId);

                // no new writes -> no need to flush
                if (writeTimestamp == flushTimestamp) {
                    continue;
                }

                if (System.nanoTime() - writeTimestamp
                        > writeDiffMilliSeconds * 1000000) {
                    try {
                        // flush sketch
                        this.sketchManager
                            .flush(sketchPlugin.getId(), nodeId);

                        log.info("flushed sketch {}:{}",
                            sketchPlugin.getId(), nodeId);
                    } catch (Exception e) {
                        log.warn("failed to flush sketch {};{}",
                            sketchPlugin.getId(), nodeId, e);
                    }
                }
            }
        }

        // TODO - remove old file system blocks
    }
}
