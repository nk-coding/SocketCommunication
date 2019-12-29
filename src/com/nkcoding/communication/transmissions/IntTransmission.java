package com.nkcoding.communication.transmissions;

import com.nkcoding.communication.Transmission;

public class IntTransmission extends Transmission {
    public int value;

    public IntTransmission(int id, int value) {
        super(id);
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("%d: %d", getId(), value);
    }
}
