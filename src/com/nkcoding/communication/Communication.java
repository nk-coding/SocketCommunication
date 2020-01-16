package com.nkcoding.communication;

import java.io.Closeable;
import java.io.DataInputStream;
import java.util.Set;

public abstract class Communication implements Closeable {
    public static int MAX_SIZE = 1280;

    protected final int port;
    private boolean isServer;

    /**
     * create a new communication instance
     *
     * @param isServer should it be the server?
     * @param port     the port to use
     */
    protected Communication(boolean isServer, int port) {
        this.isServer = isServer;
        this.port = port;
    }

    /**
     * start the communication with a peer
     *
     * @param ip the ip address of the peer
     */
    public abstract void openCommunication(String ip, int port);

    public abstract ResetDataOutputStream getOutputStream(boolean reliable);

    /**
     * send data to a specified peer with a specific id
     *
     * @param peer         the id got from openCommunication
     * @param transmission the transmission to send
     */
    public abstract void sendTo(int peer, ResetDataOutputStream transmission);

    /**
     * sends some data to all peers
     *
     * @param transmission the transmission to send
     */
    public abstract void sendToAll(ResetDataOutputStream transmission);

    /**
     * checks if there are any received transmissions
     *
     * @return true if there are any transmissions to receive
     */
    public abstract boolean hasTransmissions();

    /**
     * gets the oldest received transmission, if it was not internal
     *
     * @return the Transmission or null if none was available
     */
    public abstract DataInputStream getTransmission();

    /**
     * get a list of all peers
     *
     * @return a list with all peers
     */
    public abstract Set<Integer> getPeers();

    public boolean isServer() {
        return isServer;
    }

    /**
     * get the id of this peer
     *
     * @return the id
     */
    public abstract int getId();
}
