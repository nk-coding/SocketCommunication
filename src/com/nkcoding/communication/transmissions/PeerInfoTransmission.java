package com.nkcoding.communication.transmissions;

import com.nkcoding.communication.Transmission;

public class PeerInfoTransmission extends Transmission {
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
