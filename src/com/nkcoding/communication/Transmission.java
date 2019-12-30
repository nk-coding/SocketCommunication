package com.nkcoding.communication;

import java.io.Serializable;

public class Transmission implements Serializable {

    public static final int SET_PEER_ID = -1;
    public static final int ADD_ID = -2;
    public static final int GET_ID = -3;
    public static final int SET_ID = -4;
    public static final int GET_PEER_ID = -5;
    public static final int GET_PORT = -6;
    public static final int SET_PORT = -7;
    public static final int REDIRECT_TRANSMISSION = -8;
    public static final int ADD_ID_REDIRECTION = -9;

    /**
     * the transmission id
     * must be >= 0
     */
    private int id;

    public Transmission(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return id + ": ";
    }
}
