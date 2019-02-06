package com.bushpath.doodle.node.sketch;

import com.bushpath.doodle.Poison;
import com.bushpath.doodle.SketchPlugin;

import com.bushpath.rutils.query.Query;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;

public class QueryHandler extends Thread {
    protected int nodeId;
    protected Query query;
    protected File file;
    protected SketchPlugin sketchPlugin;
    protected BlockingQueue<Serializable> queue;

    public QueryHandler(int nodeId, SketchPlugin sketchPlugin,
            Query query, File file, BlockingQueue<Serializable> queue) {
        this.nodeId = nodeId;
        this.query = query;
        this.file = file;
        this.sketchPlugin = sketchPlugin;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            // open streams if file exists
            FileInputStream fileIn = null;
            ObjectInputStream in = null;
            if (file.exists()) {
                fileIn = new FileInputStream(file);
                in = new ObjectInputStream(fileIn);
            }

            // execute query
            this.sketchPlugin.query(this.nodeId,
                this.query, in, this.queue);

            // close streams
            if (in != null) {
                in.close();
                fileIn.close();
            }

            // send poison through queue
            while(!this.queue.offer(new Poison())) {}
        } catch (Exception e) {
            while(!this.queue.offer(e)) {}
        }
    }
}
