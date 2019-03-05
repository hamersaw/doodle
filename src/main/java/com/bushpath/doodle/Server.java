package com.bushpath.doodle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Server extends Thread {
    protected static final Logger log = LoggerFactory.getLogger(Server.class);

    protected short port;
    protected ServerSocket serverSocket;

    protected Worker[] workers;
    protected BlockingQueue<Socket> queue;
    protected Map<Integer, Service> services;

    protected boolean shutdown;

    public Server(short port, short threadCount) {
        this.port = port;
        this.workers = new Worker[threadCount];
        this.queue = new ArrayBlockingQueue(128);
        this.services = new HashMap();
        this.shutdown = true;
    }

    public void registerService(Service service) throws Exception {
        // ensure server is not running
        if (!this.shutdown) {
            throw new RuntimeException("Unable to register"
                + " service to a running server");
        }

        // register message types
        for (int messageType : service.getMessageTypes()) {
            if (this.services.containsKey(messageType)) {
                throw new RuntimeException("Message type '"
                    + messageType + "' already registered with server");
            }

            this.services.put(messageType, service);
        }

        log.debug("Registered Service '{}'", service.getClass().getName());
    }

    @Override
    public void run() {
        log.debug("Starting");

        // open server socket
        try {
            this.serverSocket = new ServerSocket(this.port);
        } catch (IOException e) {
            log.error("Failed to open ServerSocket", e);
        }

        log.debug("ServerSocket listening on port {}", this.port);
 
        // start workers
        for (int i=0; i<this.workers.length; i++) {
            this.workers[i] = new Worker();
            this.workers[i].start();
        }

        // accept Socket connections
        this.shutdown = false;
        while (!this.shutdown) {
            try {
                Socket socket = this.serverSocket.accept();
                while (!this.queue.offer(socket)) {}
            } catch (IOException e) {
                log.error("Failed to accept ServerSocket connection", e);
            }
        }
    }

    public void shutdown() throws IOException {
        this.shutdown = true;
        this.serverSocket.close();
    }

    protected class Worker extends Thread {
        protected boolean shutdown;

        public Worker() {
            this.shutdown = true;
        }

        @Override
        public void run() {
            this.shutdown = false;

            Socket socket = null;
            while (!this.shutdown) {
                // recv Socket
                try {
                    socket = queue.poll(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("Failed to recv socket", e);
                }

                // ensure we recv a Socket
                if (socket == null) {
                    continue;
                }

                try {
                    // open data streams
                    DataInputStream in =
                        new DataInputStream(socket.getInputStream());
                    DataOutputStream out =
                        new DataOutputStream(socket.getOutputStream());

                    // recv message type
                    int messageType = in.readInt();

                    if (!services.containsKey(messageType)) {
                        // warn if messageType is unregistered
                        log.warn("Recv unregistered messageType '"
                            + messageType + "'");
                    } else {
                        // send message to Service
                        services.get(messageType)
                            .handleMessage(messageType, in, out);
                    }
                } catch (Exception e) {
                    log.error("Unknown communication failure", e);
                }

                // close socket
                try {
                    socket.close();
                } catch (IOException e) {
                    log.error("Failed to close socket", e);
                }
            }
        }
    }
}
