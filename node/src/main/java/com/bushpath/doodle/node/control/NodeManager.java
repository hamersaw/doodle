package com.bushpath.doodle.node.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

public class NodeManager {
    protected static final Logger log =
        LoggerFactory.getLogger(NodeManager.class);

    protected int nodeId;
    protected List<NodeMetadata> seedNodes;
    protected TreeMap<Integer, NodeMetadata> nodes;
    protected Random random;
    protected ReadWriteLock lock;

    public NodeManager(int nodeId, List<NodeMetadata> seedNodes) {
        this.nodeId = nodeId;
        this.seedNodes = seedNodes;
        this.nodes = new TreeMap();
        this.random = new Random(System.currentTimeMillis());
        this.lock = new ReentrantReadWriteLock();
    }

    public void addNode(NodeMetadata node) throws Exception {
        // check if node already exists
        this.lock.readLock().lock();
        try {
            if (this.nodes.containsKey(node.getId())) {
                throw new RuntimeException("node '"
                    + node.getId() + "' already exists");
            }
        } finally {
            this.lock.readLock().unlock();
        }

        // add node
        this.lock.writeLock().lock();
        try {
            this.nodes.put(node.getId(), node);
            log.info("Added node {}", node);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public boolean containsNode(int id) {
        this.lock.readLock().lock();
        try {
            return this.nodes.containsKey(id);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public NodeMetadata getNode(int id) throws Exception {
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

    public int getNodesHash() {
        CRC32 crc32 = new CRC32();

        this.lock.readLock().lock();
        try {
            // update crc32 with toString() of each node
            for (NodeMetadata nodeMetadata : this.nodes.values()) {
                crc32.update(nodeMetadata.toString().getBytes());
            }
        } finally {
            this.lock.readLock().unlock();
        }

        return (int) crc32.getValue();
    }

    public Collection<NodeMetadata> getNodeValues() {
        this.lock.readLock().lock();
        try {
            return this.nodes.values();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public NodeMetadata getRandomNode() {
        NodeMetadata nodeMetadata = null;

        this.lock.readLock().lock();
        try {
            do {
                if (this.nodes.size() == 1) {
                    // if only node in 'nodes' is self return a seed node
                    nodeMetadata = this.seedNodes.get(
                            this.random.nextInt(this.seedNodes.size())
                        );
                } else {
                    // choose random node in 'nodes'
                    int randomIndex = this.random.nextInt(this.nodes.size());
                    int randomNodeId = this.nodes.firstKey();
                    for (int i=0; i<randomIndex; i++) {
                        randomNodeId = this.nodes.higherKey(randomNodeId);
                    }

                    nodeMetadata = this.nodes.get(randomNodeId);
                }
            } while (nodeMetadata.getId() == this.nodeId);
        } finally {
            this.lock.readLock().unlock();
        }

        return nodeMetadata;
    }
}
