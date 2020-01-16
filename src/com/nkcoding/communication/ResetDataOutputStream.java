package com.nkcoding.communication;

import java.io.*;

public class ResetDataOutputStream extends DataOutputStream {
    private ByteArrayOutputStream byteArrayOutputStream;

    /**
     * Creates a new data output stream to write data in an underlaying
     * ByteArrayOutputStream. The counter <code>written</code> is
     * set to zero.
     * Supports reset via reset
     *
     * @see FilterOutputStream#out
     */
    public ResetDataOutputStream() {
        this(new ByteArrayOutputStream());
    }

    private ResetDataOutputStream(ByteArrayOutputStream byteArrayOutputStream) {
        super(byteArrayOutputStream);
        this.byteArrayOutputStream = byteArrayOutputStream;
    }

    /**
     * flushes this stream and resets the underlying ByteArrayOutputStream
     */
    public void reset() throws IOException{
        flush();
        byteArrayOutputStream.reset();
        this.written = 0;
    }

    /**
     * creates a newly allocated byte array with the buffer written to it
     */
    public byte[] toByteArray() {
        return byteArrayOutputStream.toByteArray();
    }

}
