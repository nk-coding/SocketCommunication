package com.nkcoding.communication.transmissions;

import com.nkcoding.communication.Transmission;

import java.io.Serializable;
import java.util.Arrays;

public class PeerInfoTransmission extends Transmission {
    public PeerInfo[] peerInfos;

    public PeerInfoTransmission(int id, PeerInfo[] peerInfos) {
        super(id);
        this.peerInfos = peerInfos;
    }

    @Override
    public String toString() {
        return getId() + ": " + Arrays.toString(peerInfos);
    }

    public static class PeerInfo implements Serializable {
        public String ip;
        public int peerID;
        public int port;

        public PeerInfo(String ip, int port, int peerID) {
            this.ip = ip;
            this.peerID = peerID;
            this.port = port;
        }


        @Override
        public String toString() {
            return String.format("address=%s:%d, peerID=%d", ip, port, peerID);
        }
    }
}
