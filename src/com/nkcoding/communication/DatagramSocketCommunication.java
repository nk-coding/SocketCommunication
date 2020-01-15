package com.nkcoding.communication;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.*;

public class DatagramSocketCommunication extends Communication {

    private static final int RECEIVE_TIMEOUT = 500;
    private static final int RESEND_TIMEOUT = 500;

    static final byte IS_SYSTEM = 0x01;
    static final byte IS_RELIABLE = 0x02;
    static final byte IS_PARTIAL = 0x04;
    static final byte IS_PARTIAL_START = 0x08;
    static final byte IS_PARTIAL_END = 0x10;
    static final byte IS_INDIRECT = 0x20;
    static final byte IS_OPEN_CONNECTION = 0x40;

    private static final short PROTOCOL_ID = 8001;
    private static final int HEADER_SIZE = 21;

    //the id for this client
    private short clientID;

    private final ConcurrentLinkedQueue<DataInputStream> receivedTransmissions;

    private final ConcurrentMap<Integer, Connection> connections;

    private final CopyOnWriteArraySet<Integer> peerSet;

    private Thread serverAcceptingThread;

    /**
     * create a new communication instance
     *
     * @param isServer should it be the server?
     * @param port     the port to use
     */
    public DatagramSocketCommunication(boolean isServer, int port) {
        super(isServer, port);
        //initialize data structures
        receivedTransmissions = new ConcurrentLinkedQueue<>();
        connections = new ConcurrentHashMap<>();
        peerSet = new CopyOnWriteArraySet<>();
    }

    @Override
    public void openCommunication(String ip, int port) {

    }

    @Override
    public DataOutputStream getOutputStream(boolean reliable) {
        return null;
    }

    @Override
    public void sendTo(int peer, DataOutputStream transmission) {

    }

    @Override
    public void sendToAll(DataOutputStream transmission) {

    }

    @Override
    public boolean hasTransmissions() {
        return false;
    }

    @Override
    public DataInputStream getTransmission() {
        return null;
    }

    @Override
    public Set<Integer> getPeers() {
        return null;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    public static void writeInt(byte[] msg, int offset, int val) {
        msg[offset] = (byte)(val >>> 24);
        msg[offset + 1] = (byte)(val >>> 16);
        msg[offset + 2] = (byte)(val >>> 8);
        msg[offset + 3] = (byte)(val);
    }

    public static void writeShort(byte[] msg, int offset, short val) {
        msg[offset] = (byte)(val >>> 8);
        msg[offset + 1] = (byte)(val);
    }

    public static int readInt(byte[] msg, int offset) {
        return ((msg[offset] & 0xFF) << 24)
                + ((msg[offset + 1] & 0xFF) << 16)
                + ((msg[offset + 2] & 0xFF) << 8)
                + (msg[offset + 3] & 0xFF);
    }

    static short readShort(byte[] msg, int offset) {
        return (short)(((msg[offset] & 0xFF) << 8)
                + (msg[offset + 1] & 0xFF));
    }

    static void setFlag(byte[] msg, int offset, byte flag, boolean value) {
        if (value) {
            msg[offset] |= flag;
        } else {
            msg[offset] &= ~flag;
        }
    }

    static void setFlag(byte[] msg, byte flag, boolean value) {
        setFlag(msg, 3, flag, value);
    }

    static boolean readFlag(byte[] msg, byte flag) {
        return ((msg[2] & flag) & 0xFF) != 0;
    }

    public short getClientID() {
        return clientID;
    }

    private class Connection extends Thread {
        private short remoteID;

        private String remoteIP;
        private int remotePort;

        private boolean connected = false;
        private boolean isIndirect = false;

        /**
         * represents the next EXPECTED message, NOT the last received one
         */
        private int ack;
        /**
         * represents the status of further messages, INCLUDING the expected one from ack
         */
        private int ackField = 0;

        /**
         * the sequence number for the next send operation
         */
        private int sequence = 0;

        /**
         * the sequence Number that LAST was acknowledged
         */
        private int sequenceAcknowledged = -1;

        private int sequenceAcknowledgedField = 0xFFFFFFFF;

        private volatile boolean shutdown = false;

        /**
         * queue for synchronization and timeout, does not save elements
         */
        private SynchronousQueue<byte[]> receiveQueue = new SynchronousQueue<>();

        /**
         * deque which works as a buffer for reliable messages
         */
        private LinkedList<byte[]> reliableMessageBuffer = new LinkedList<>();

        /**
         * the offset for the reliableMessageBuffer compared to the message's sequence
         * for example if the msg at position 0 in the buffer came with the sequenceNumber 10, then offset is 10
         */
        private int messageBufferOffset = 0;
        /**
         * the amount of partial messages necessary to compose the current message
         */
        private int partialMessageLength = 1;

        public Connection(String remoteIP, int remotePort) {
            this.remoteIP = remoteIP;
            this.remotePort = remotePort;
        }

        private void connect() {

        }

        /**
         * sends a message
         * sets isIndirect and indirectTarget if indirect
         * sets remoteId, sequence, ack and ackField if NOT indirect
         * @param msg the message to send
         */
        private void sendInternal(byte[] msg) {
            //set indirect flag
            if (isIndirect) {
                setFlag(msg, IS_INDIRECT, true);
                writeShort(msg, 5, remoteID);
            } else {
                writeShort(msg, 3, remoteID);
                writeInt(msg, 9, sequence);
                writeInt(msg, 13, ack);
                writeInt(msg, 17, ackField);
            }
            //TODO
        }

        /**
         * handles a raw received message byte array
         */
        private void receiveInternal(byte[] msg) {
            int acknowledged = readInt(msg, 13);
            if (acknowledged > sequenceAcknowledged) {
                sequenceAcknowledgedField >>>= (acknowledged - sequenceAcknowledged);
                sequenceAcknowledgedField |= readInt(msg, 17);
            }
            if (readFlag(msg, IS_RELIABLE)) {
                handleReliableMessage(msg);
            } else {
                handleReceivedMessage(msg, HEADER_SIZE);
            }
        }

        private void handleReliableMessage(byte[] msg) {
            //get the message sequence
            int msgSequence = readInt(msg, 9);
            if (msgSequence < ack) {
                System.out.println("received delayed message, discard: " + msgSequence + ", " + ack);
                return;
            }
            //ensure the capacity
            if (msgSequence - messageBufferOffset > reliableMessageBuffer.size()) {
                for (int i = 0; i < reliableMessageBuffer.size() - msgSequence + messageBufferOffset; i++) {
                    reliableMessageBuffer.add(null);
                }
            }
            //add it to the buffer
            reliableMessageBuffer.set(msgSequence - messageBufferOffset, msg);
            //update partialMessageLength if necessary
            if (msgSequence == ack) {
                partialMessageLength = (readFlag(msg, IS_PARTIAL)) ? readShort(msg, 7) : 1;
            }
            //check if it has to be handled by updateAcknowledged
            if ((msgSequence - messageBufferOffset) < 32) {
                ackField |= (1 << (msgSequence - messageBufferOffset));
                tryReceiveReliableMessage();
            }
        }

        /**
         * tries to receive a reliable message from the reliableMessagesBuffer, and updates the ack accordingly
         * should be called after an update of one of the first 32 elements in the reliableMessagesBuffer
         */
        private void tryReceiveReliableMessage() {
            boolean receivedAll = false;
            while (!receivedAll) {
                Iterator<byte[]> iter = reliableMessageBuffer.iterator();
                int amount = 0;
                int totalSize = 0;
                while (amount < messageBufferOffset && iter.hasNext()) {
                    byte[] bytes = iter.next();
                    if (bytes == null) {
                        receivedAll = true;
                        break;
                    }
                    amount++;
                    totalSize += bytes.length;
                }
                //update ack and ackfield
                if (amount > 0) {
                    ack += amount;
                    ackField >>>= amount;
                    int index = 32 - amount;
                    if (index < reliableMessageBuffer.size()) {
                        ListIterator<byte[]> listIterator = reliableMessageBuffer.listIterator(32);
                        while (index < 32 && listIterator.hasNext()) {
                            byte[] bytes = listIterator.next();
                            if (index >= 0 && bytes != null) {
                                ackField |= (1 << index);
                            }
                            index++;
                        }
                    }
                }
                if (!receivedAll) {
                    //handle the message based on whether it is partial or not
                    if (amount == 1) {
                        byte[] msg = reliableMessageBuffer.removeFirst();
                        handleReceivedMessage(msg, HEADER_SIZE);
                        reliableMessageBuffer.addLast(null);
                    } else {
                        byte[] msg = new byte[totalSize - amount * HEADER_SIZE];
                        int newPos = 0;
                        for (int i = 0; i < amount; i++) {
                            byte[] part = reliableMessageBuffer.removeFirst();
                            int length = part.length - HEADER_SIZE;
                            System.arraycopy(part, HEADER_SIZE, msg, newPos, length);
                            newPos += length;
                            reliableMessageBuffer.addLast(null);
                        }
                        handleReceivedMessage(msg, 0);
                    }
                    //update offset
                    messageBufferOffset += amount;
                }
            }
        }

        /**
         * handles a complete received message, and adds it to receivedTransmissions or handles it internal
         */
        private void handleReceivedMessage(byte[] msg, int offset) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(msg);
            long skipped = inputStream.skip(offset);
            inputStream.mark(0);
            if (skipped != offset) System.out.println("error: could not skip enough: " + skipped);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            if (readFlag(msg, IS_SYSTEM)) {
                handleSystemMessage(dataInputStream);
            } else {
                //add it directly to the output queue
                receivedTransmissions.add(dataInputStream);
            }
        }

        /**
         * handles a system message
         */
        private void handleSystemMessage(DataInputStream inputStream) {
            //TODO
        }

        @Override
        public void run() {
            super.run();
            while (!shutdown) {
                try {
                    byte[] msg = receiveQueue.poll(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS);
                    receiveInternal(msg);
                } catch (InterruptedException e) {
                    System.out.println("did not receive anything: " + remoteID);
                }
            }
        }

        public int getRemoteID() {
            return remoteID;
        }

    }
}
