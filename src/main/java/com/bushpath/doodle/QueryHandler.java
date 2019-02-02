package com.bushpath.doodle;

import com.bushpath.rutils.query.Query;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

public class QueryHandler extends Thread {
    protected int nodeId;
    protected Query query;
    protected SketchPlugin sketchPlugin;
    protected BlockingQueue<Serializable> queue;

    public QueryHandler(int nodeId, SketchPlugin sketchPlugin,
            Query query, BlockingQueue<Serializable> queue) {
        this.nodeId = nodeId;
        this.query = query;
        this.sketchPlugin = sketchPlugin;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            this.sketchPlugin.query(this.nodeId,
                this.query, this.queue);

            while(!this.queue.offer(new Poison())) {}
        } catch (Exception e) {
            while(!this.queue.offer(e)) {}
        }
    }
}
