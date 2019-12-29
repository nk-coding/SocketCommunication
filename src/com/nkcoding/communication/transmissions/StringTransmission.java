package com.nkcoding.communication.transmissions;

import com.nkcoding.communication.Transmission;

public class StringTransmission extends Transmission {
    public String value;

    public StringTransmission(int id, String data) {
        super(id);
        this.value = data;
    }

    @Override
    public String toString() {
        return String.format("%d: %s", this.getId(), value);
    }
}
