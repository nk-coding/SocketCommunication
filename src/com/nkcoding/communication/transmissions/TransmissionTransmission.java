package com.nkcoding.communication.transmissions;

import com.nkcoding.communication.Transmission;

public class TransmissionTransmission extends Transmission {
    public int from;

    public int to;

    public Transmission transmission;

    public TransmissionTransmission(int id, int from, int to, Transmission transmission) {
        super(id);
        this.from = from;
        this.to = to;
        this.transmission = transmission;
    }

    @Override
    public String toString() {
        return String.format("%d: %d -> %d : %s", getId(), from, to, transmission);
    }
}
