package com.nkcoding.communication;

import com.nkcoding.communication.transmissions.IntTransmission;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class SocketCommunication extends Communication {

    private final ConcurrentLinkedQueue<Transmission> receivedTransmissions;

    private final ConcurrentMap<Integer, Connection> connections;

    private CopyOnWriteArraySet<Integer> peerSet;

    private ServerSocket serverSocket;

    private Thread serverAcceptingThread;

    /**
     * the id for this SocketCommunication
     * is -1 if it is not set yet
     * is 0 if this is the server
     */
    private int id = -1;

    /**
     * the next id for a client
     * is increased, if it is used
     * has only effect if isServer
     */
    private int idCounter = 1;

    /**
     * create a new communication instance
     *
     * @param isServer should it be the server?
     * @param port     the port to use
     */
    public SocketCommunication(boolean isServer, int port) {
        super(isServer, port);
        receivedTransmissions = new ConcurrentLinkedQueue<>();
        connections = new ConcurrentHashMap<>();
        peerSet = new CopyOnWriteArraySet<>();
        if (isServer) {
            this.id = 0;
        }
        try {
            //create the ServerSocket with  a thread for it and start it
            serverSocket = new ServerSocket(port);
            serverAcceptingThread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket socket = serverSocket.accept();
                        System.out.println("accepted socket");
                        System.out.println(socket.getRemoteSocketAddress().toString());
                        //if this is a server, then a new id is generated
                        //otherwise the id must be known by the peer already
                        //also start the tread
                        if (isServer()) {
                            Connection connection = new Connection(socket, idCounter);
                            connection.start();
                            idCounter++;
                            //tell the connection who he is
                            connection.send(new IntTransmission(Transmission.SET_ID, connection.peerID));
                            for (Connection other : connections.values()) {
                                if (other != connection && other.remotePortAvailable()) {
                                    connection.send(new PeerInfoTransmission(Transmission.ADD_ID, other.getRemoteIP(), other.remotePort, other.peerID));
                                }
                            }

                        } else {
                            new Connection(socket).start();
                        }
                    } catch (IOException e) {
                        System.err.println("errow while opening socket");
                        e.printStackTrace();
                    }
                }
            });
            serverAcceptingThread.start();
        } catch (IOException e) {
            throw new IllegalArgumentException("could not create ServerSocket", e);
        }
    }

    public static int getEphemeralPort() {
        try {
            ServerSocket serverSocket = new ServerSocket(0);
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("no port available");
        }
    }

    /**
     * start the communication with a peer
     * This peer MUST NOT be the server to use this method
     *
     * @param ip the ip address of the peer
     */
    @Override
    public void openCommunication(String ip, int port) {
        //check that this is not a server, to prevent serious errors
        if (isServer()) {
            throw new IllegalStateException("must not be the server");
        } else {
            Connection connection = openConnection(ip, port, 0);
            if (connection != null && id == -1) {
                System.out.println("try to get own id");
                connection.send(new Transmission(Transmission.GET_PEER_ID));
            }
        }
    }

    /**
     * send data to a specified peer with a specific id
     *
     * @param peer         the id got from openCommunication
     * @param transmission the transmission to send
     */
    @Override
    public void sendTo(int peer, Transmission transmission) {
        System.out.println("send transmission " + transmission + " to " + peer);
        Connection connection = connections.get(peer);
        if (connection == null) {
            throw new IllegalArgumentException("peer does not exist");
        } else {
            connection.send(transmission);
        }
    }

    /**
     * sends some data to all peers
     *
     * @param transmission the transmission to send
     */
    @Override
    public void sendToAll(Transmission transmission) {
        for (Connection connection : connections.values()) {
            connection.send(transmission);
        }
    }

    /**
     * checks if there are any received transmissions
     *
     * @return true if there are any transmissions to receive
     */
    @Override
    public boolean hasTransmissions() {
        return !receivedTransmissions.isEmpty();
    }

    /**
     * gets the oldest received transmission, if it was not internal
     *
     * @return the Transmission or null if none was available
     */
    @Override
    public Transmission getTransmission() {
        return receivedTransmissions.poll();
    }

    /**
     * get a list of all peers
     *
     * @return a list with all peers
     */
    @Override
    public Set<Integer> getPeers() {
        return Collections.unmodifiableSet(peerSet);
    }

    /**
     * get the id of this peer
     *
     * @return the id
     */
    @Override
    public int getId() {
        return this.id;
    }

    /**
     * closes all sockets and tries to stop al threads
     */
    @Override
    public void close() {
        serverAcceptingThread.interrupt();
        try {
            serverAcceptingThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Connection connection : connections.values()) {
            connection.interrupt();
            try {
                connection.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void setID(int newID) {
        this.id = newID;
    }

    /**
     * internal version of openConnection
     * used to ad new peers on other clients
     *
     * @param ip the new peer ip
     * @param id the id for the new peer
     */
    private void addPeer(String ip, int port, int id) {
        Connection connection = openConnection(ip, port, id);
//        if (connection != null) {
//            //tell the new peer who I am
//            System.out.println("I AM " + this.id);
//            connection.send(new IntTransmission(Transmission.SET_PEER_ID, this.id));
//        }
    }


    /**
     * internally used to open a connection
     *
     * @param ip the address for remote
     * @return the Connection
     */
    private Connection openConnection(String ip, int port, int id) {
        try {
            Socket peerSocket = new Socket(ip, port);
            Connection connection = new Connection(peerSocket, port, id);
            connection.start();
            return connection;
        } catch (IOException e) {
            System.err.println("could not open socket");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * internally used to open a connection with no known id for the peer
     *
     * @param ip the address for remote
     * @return the Connection
     */
    private Connection openConnection(String ip, int port) {
        return openConnection(ip, port, -1);
    }

    private class Connection extends Thread implements Closeable {
        public final Socket socket;

        private int peerID = -1;

        public final ObjectInputStream inputStream;

        public final ObjectOutputStream outputStream;

        public int remotePort = -1;

        public Connection(Socket socket, int remotePort, int peerID) throws IOException {
            setPeerID(peerID);
            this.socket = socket;
            this.remotePort = remotePort;
            outputStream = new ObjectOutputStream(socket.getOutputStream());
            outputStream.flush();
            inputStream = new ObjectInputStream(socket.getInputStream());
            if (peerID == -1) {
                //try to get the id
                System.out.println("send peerID request");
                send(new Transmission(Transmission.GET_ID));
            }
            if (remotePort == -1) {
                //find out remote port
                System.out.println("request remote port");
                send(new Transmission(Transmission.GET_PORT));
            }
        }

        /**
         * wrapper for Connection(socket, -1, peerID)
         */
        public Connection(Socket socket, int peerID) throws IOException {
            this(socket, -1, peerID);
        }

        /**
         * wrapper for Connection(socket, -1, -1)
         */
        public Connection(Socket socket) throws IOException {
            this(socket, -1, -1);
        }

        @Override
        public void run() {
            super.run();
            while (true) {
                try {
                    Transmission transmission = (Transmission) inputStream.readObject();
                    System.out.println("read new transmission: " + transmission);
                    switch (transmission.getId()) {
                        case Transmission.SET_PEER_ID:
                            //change the id
                            int newID = ((IntTransmission) transmission).value;
                            setPeerID(newID);
                            break;
                        case Transmission.SET_ID:
                            int nID = ((IntTransmission) transmission).value;
                            if (SocketCommunication.this.id != nID) {
                                setID(nID);
                            }
                            break;
                        case Transmission.GET_ID:
                            //if the id is requested, the peer wants to know my id, so a
                            //SET_PEER_ID is sent back
                            System.out.println("(requested) I AM " + SocketCommunication.this.id);
                            send(new IntTransmission(Transmission.SET_PEER_ID, SocketCommunication.this.id));
                            break;
                        case Transmission.GET_PEER_ID:
                            //if the peer id is requested, the peer wants to know its own id, so
                            //a SET_ID is sent back
                            if (this.peerID == -1) {
                                System.err.println("tries to get unknown id: " + peerID);
                            } else {
                                System.out.println("send peer id: " + peerID);
                                send(new IntTransmission(Transmission.SET_ID, this.peerID));
                            }
                            break;
                        case Transmission.ADD_ID:
                            //add a new peer
                            PeerInfoTransmission pit = (PeerInfoTransmission) transmission;
                            addPeer(pit.ip, pit.port, pit.peerID);
                            break;
                        case Transmission.GET_PORT:
                            send(new IntTransmission(Transmission.SET_PORT, SocketCommunication.this.port));
                            break;
                        case Transmission.SET_PORT:
                            this.remotePort = ((IntTransmission)transmission).value;
                            break;
                        default:
                            //redirect to the other transmissions
                            receivedTransmissions.add(transmission);
                    }
                } catch (IOException e) {
                    System.err.println("IOException occurred while reading on " + peerID);
                    e.printStackTrace();
                } catch (Exception e) {
                    System.err.println("probably different project versions " + peerID);
                    e.printStackTrace();
                }
            }
        }

        public synchronized void send(Transmission transmission) {
            try {
                outputStream.writeObject(transmission);
            } catch (IOException e) {
                System.err.println("could not send data: " + this.peerID);
            }
        }

        public String getRemoteIP() {
            return ((InetSocketAddress) socket.getRemoteSocketAddress()).getAddress().getHostAddress();
        }

        public boolean remotePortAvailable() {
            return remotePort != -1;
        }

        private void setPeerID(int peerID) {
            if (this.peerID != peerID) {
                if (connections.containsKey(this.peerID)) {
                    System.out.println("previous peer id: " + this.peerID);
                    System.out.println("HAVE TO REMOVE CONNECTION: NEW ID");
                    connections.remove(this.peerID);
                }
                peerSet.remove(this.peerID);

                this.peerID = peerID;

                peerSet.add(peerID);
                connections.put(peerID, this);
            }
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
            outputStream.close();
            socket.close();
        }
    }

    private static class PeerInfoTransmission extends Transmission {
        public String ip;
        public int peerID;
        public int port;

        public PeerInfoTransmission(int id, String ip, int port, int peerID) {
            super(id);
            this.ip = ip;
            this.peerID = peerID;
            this.port = port;
        }

        @Override
        public String toString() {
            return String.format("%d: address=%s:%d, peerID=%d", getId(), ip, port, peerID);
        }
    }
}
