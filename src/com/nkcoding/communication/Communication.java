package com.nkcoding.communication;

import java.io.Closeable;
import java.util.Set;

public abstract class Communication implements Closeable {
    private boolean isServer;

    protected final int port;

    /**
     * create a new communication instance
     * @param isServer should it be the server?
     * @param port the port to use
     */
    protected Communication(boolean isServer, int port) {
        this.isServer = isServer;
        this.port = port;
    }

    /**
     * start the communication with a peer
     * @param ip the ip address of the peer
     */
    public abstract void openCommunication(String ip, int port);

    /**
     * send data to a specified peer with a specific id
     * @param peer the id got from openCommunication
     * @param transmission the transmission to send
     */
    public abstract void sendTo(int peer, Transmission transmission);

    /**
     * sends some data to all peers
     * @param transmission the transmission to send
     */
    public abstract void sendToAll(Transmission transmission);

    /**
     * checks if there are any received transmissions
     * @return true if there are any transmissions to receive
     */
    public abstract boolean hasTransmissions();

    /**
     * gets the oldest received transmission, if it was not internal
     * @return the Transmission or null if none was available
     */
    public abstract Transmission getTransmission();

    /**
     * get a list of all peers
     * @return a list with all peers
     */
    public abstract Set<Integer> getPeers();

    public boolean isServer() {
        return isServer;
    }

    /**
     * get the id of this peer
     * @return the id
     */
    public abstract int getId();
}
