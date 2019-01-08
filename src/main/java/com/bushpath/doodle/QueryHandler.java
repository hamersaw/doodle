package com.bushpath.doodle;

import com.bushpath.rutils.query.Query;

import java.io.DataInputStream;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

public class QueryHandler extends Thread {
    protected DataInputStream in;
    protected Query query;
    protected BlockingQueue<Serializable> queue;
    protected SketchPlugin sketchPlugin;

    public QueryHandler(DataInputStream in, Query query,
            BlockingQueue<Serializable> queue, SketchPlugin sketchPlugin) {
        this.in = in;
        this.query = query;
        this.queue = queue;
        this.sketchPlugin = sketchPlugin;
    }

    @Override
    public void run() {
        try {
            if (this.in == null) {
                this.sketchPlugin.query(this.query, this.queue);
            } else {
                this.sketchPlugin.query(this.query, this.in, this.queue);
            }

            while(!this.queue.offer(new Poison())) {}
        } catch (Exception e) {
            while(!this.queue.offer(e)) {}
        }
    }
}
