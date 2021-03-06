package com.bushpath.doodle.dfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.TreeMap;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NodeManager {
    protected static final Logger log =
        LoggerFactory.getLogger(NodeManager.class);

    protected int nodeId;
    protected TreeMap<Integer, NodeMetadata> nodes;
    protected Random random;
    protected ReadWriteLock lock;

    public NodeManager(int nodeId,
            TreeMap<Integer, NodeMetadata> nodes) {
        this.nodeId = nodeId;
        this.nodes = nodes;
        this.random = new Random(System.nanoTime());
        this.lock = new ReentrantReadWriteLock();
    }

    public void checkExists(int id) {
        this.lock.readLock().lock();
        try {
            if (!this.nodes.containsKey(id)) {
                throw new RuntimeException("Node '"
                    + id + "' does not exist");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public void checkNotExists(int id) {
        this.lock.readLock().lock();
        try {
            if (this.nodes.containsKey(id)) {
                throw new RuntimeException("Node '"
                    + id + "' already exists");
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public boolean contains(int id) {
        this.lock.readLock().lock();
        try {
            return this.nodes.containsKey(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public NodeMetadata get(int id) throws Exception {
        this.lock.readLock().lock();
        try {
            // check if node exists
            if (!this.nodes.containsKey(id)) {
                throw new RuntimeException("Node '" + id + "' does not exist");
            }

            return this.nodes.get(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Collection<NodeMetadata> getValues() {
        this.lock.readLock().lock();
        try {
            return this.nodes.values();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public int getThisNodeId() {
        return this.nodeId;
    }

    public NodeMetadata getRandomNode(int ... excludeNodeIds) {
        this.lock.readLock().lock();
        try {
            // find number of excludeNodeIds in ids
            int invalidIdCount = 0;
            for (int excludeNodeId : excludeNodeIds) {
                if (this.nodes.containsKey(excludeNodeId)) {
                    invalidIdCount += 1;
                }
            }

            if (invalidIdCount == this.nodes.size()) {
                // all nodes are invalid
                return null;
            }

            // find random node
            NodeMetadata nodeMetadata = null;
            while (true) {
                // choose random node in 'nodes'
                int randomIndex = this.random.nextInt(this.nodes.size());
                int randomNodeId = this.nodes.firstKey();
                for (int i=0; i<randomIndex; i++) {
                    randomNodeId = this.nodes.higherKey(randomNodeId);
                }

                nodeMetadata = this.nodes.get(randomNodeId);

                // check if node is valid
                boolean valid = true;
                for (int excludeNodeId : excludeNodeIds) {
                    if (excludeNodeId == nodeMetadata.getId()) {
                        valid = false;
                        break;
                    }
                }

                if (valid) {
                    return nodeMetadata;
                }
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
