package com.nkcoding.communication;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DatagramSocketCommunication extends Communication {

    private static final int RECEIVE_TIMEOUT = 500;
    private static final int RESEND_TIMEOUT = 500;

    static final byte IS_SYSTEM = 0x01;
    static final byte IS_RELIABLE = 0x02;
    static final byte IS_PARTIAL = 0x04;
//    static final byte IS_PARTIAL_START = 0x08;
//    static final byte IS_PARTIAL_END = 0x10;
    static final byte IS_INDIRECT = 0x20;
    static final byte IS_OPEN_CONNECTION = 0x40;
    static final byte REQUEST_RESEND = (byte)0x80;

    private static final short PROTOCOL_ID = 8001;
    private static final int HEADER_SIZE = 21;
    private static final int MIN_RESEND_TIMEOUT = 100;
    private static final int MAX_SIZE = 1310;


    /**
     * the id for this client
     */
    private short clientID;

    /**
     * list with all received transmissions, ready for the user
     */
    private final ConcurrentLinkedQueue<DataInputStream> receivedTransmissions;

    /**
     * map with all connections
     */
    private final ConcurrentMap<Integer, Connection> connections;

    /**
     * set with all indexes of connections, used for faster return
     */
    private final CopyOnWriteArraySet<Integer> peerSet;

    private final Deque<ResetDataOutputStream> outputStreamPool;

    /**
     * the Thread that handles all read on the DatagramSocket
     */
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
        outputStreamPool = new ArrayDeque<>();
    }

    @Override
    public void openCommunication(String ip, int port) {

    }

    @Override
    public synchronized ResetDataOutputStream getOutputStream(boolean reliable) {
        ResetDataOutputStream dataOutputStream;
        if (outputStreamPool.isEmpty()) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            dataOutputStream = new ResetDataOutputStream();
        } else {
            dataOutputStream = outputStreamPool.poll();
        }
        //TODO set header and position
        return dataOutputStream;
    }

    @Override
    public synchronized void sendTo(int peer, ResetDataOutputStream transmission) {
        try {
            transmission.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        sendTo(peer, transmission.toByteArray());
        try {
            transmission.reset();
            outputStreamPool.add(transmission);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                transmission.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void sendToAll(ResetDataOutputStream transmission) {
        try {
            transmission.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int peer : peerSet) {
            sendTo(peer, transmission.toByteArray());
        }
        try {
            transmission.reset();
            outputStreamPool.add(transmission);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                transmission.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void sendTo(int peer, byte[] msg) {
        //TODO implementation
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

    private void sendMsgInternal(byte[] msg) {
        //TODO implementation
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

        //calculates the amount of newer packages and requests a resend if this exceeded 5
        private int resendRequestCounter = 0;

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
        private int sequenceAck = -1;

        private int sequenceAckField = 0xFFFFFFFF;

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
         * a queue with all the messages send but not acknowledged
         */
        private LinkedList<byte[]> sentMessagesBuffer = new LinkedList<>();

        /**
         * the offset for the reliableMessageBuffer compared to the message's sequence
         * for example if the msg at position 0 in the buffer came with the sequenceNumber 10, then offset is 10
         */
        private int messageBufferOffset = 0;
        /**
         * the amount of partial messages necessary to compose the current message
         */
        private int partialMessageLength = 1;

        private long lastResendTimestamp;

        public Connection(String remoteIP, int remotePort) {
            this.remoteIP = remoteIP;
            this.remotePort = remotePort;
            this.lastResendTimestamp = System.currentTimeMillis();
        }

        private void connect() {

        }

        /**
         * handles all partial stuff, rest is done by sendInternal
         * @param msg the message to send
         */
        private synchronized void send(byte[] msg) {
            if (readFlag(msg, IS_RELIABLE) && msg.length > MAX_SIZE) {
                int parts = (msg.length - HEADER_SIZE) / (Communication.MAX_SIZE);
                for (int i = 0; i < parts - 1; i++) {
                    byte[] msgPart = new byte[HEADER_SIZE + Communication.MAX_SIZE];
                    System.arraycopy(msg, 0, msgPart, 0, HEADER_SIZE);
                    System.arraycopy(msg, Communication.MAX_SIZE * i + HEADER_SIZE, msgPart, HEADER_SIZE, Communication.MAX_SIZE);
                    writeShort(msgPart, 7, (short)parts);
                    setFlag(msg, IS_PARTIAL, true);
                    sendInternal(msgPart);
                }
                //send the last part which has a special length
                byte[] msgLastPart = new byte[msg.length - Communication.MAX_SIZE * (parts - 1)];
                System.arraycopy(msg, 0, msgLastPart, 0, HEADER_SIZE);
                System.arraycopy(msg, Communication.MAX_SIZE * (parts - 1) + HEADER_SIZE, msgLastPart, HEADER_SIZE, msgLastPart.length - HEADER_SIZE);
                writeShort(msgLastPart, 7, (short)parts);
                setFlag(msg, IS_PARTIAL, true);
                sendInternal(msgLastPart);
            } else {
                //nothing to do here, if it is to long than there's nothing I can do
                sendInternal(msg);
            }
        }

        /**
         * sends a message
         * sets isIndirect and indirectTarget if indirect
         * sets remoteId, sequence, ack and ackField if NOT indirect
         * requires the following fields to be set:
         * <ul>
         *     <li>protocolID</li>
         *     <li>clientID</li>
         *     <li>isSystem, isReliable, </li>
         *     <li>all partial stuff</li>
         * </ul>
         * @param msg the message to send
         */
        private synchronized void sendInternal(byte[] msg) {
            if (isIndirect) {
                //set indirect flag
                setFlag(msg, IS_INDIRECT, true);
                writeShort(msg, 5, remoteID);
                //send via server
                connections.get(0).sendInternal(msg);
            } else {
                setFlag(msg, REQUEST_RESEND, resendRequestCounter > 3);
                writeShort(msg, 3, remoteID);
                writeInt(msg, 9, sequence);
                writeInt(msg, 13, ack);
                writeInt(msg, 17, ackField);
                sequence++;
                sentMessagesBuffer.addLast(msg);
                sendMsgInternal(msg);
            }
        }

        /**
         * handles a raw received message byte array
         */
        private void receiveInternal(byte[] msg) {
            handleSequenceAckAndResend(msg);
            if (readFlag(msg, IS_RELIABLE)) {
                handleReliableMessage(msg);
            } else {
                //special handling to use unreliable messages to request a resend
                int seq = readInt(msg, 9);
                if (seq > ack) {
                    resendRequestCounter++;
                }
                handleReceivedMessage(msg, HEADER_SIZE);
            }
        }

        private void handleSequenceAckAndResend(byte[] msg) {
            int acknowledged = readInt(msg, 13);
            int seq = readInt(msg, 9);
            boolean resend = false;
            //check if the packet is new enough to do all this stuff
            if (acknowledged >= sequenceAck) {
                //reset the counter, because a new reliable msg was registered, so a request would happen on its own
                if (seq > ack && readFlag(msg, IS_RELIABLE)) {
                    resendRequestCounter = 0;
                }
                //update sequenceAck, sequenceAckField and the sentMessagesBuffer
                sequenceAckField >>>= (acknowledged - sequenceAck);
                for (int i = 0; i < acknowledged - sequenceAck; i++) {
                    //these messages are proven sent, so remove these
                    sentMessagesBuffer.removeFirst();
                }
                sequenceAckField |= readInt(msg, 17);
                sequenceAck = acknowledged;
                //find the newest acknowledged and resend all older
                for (int i = 31; i > 0; i--) {
                    //check if the bit is set
                    if ((sequenceAckField & (1 << i)) != 0) {
                        //check if all previous were send, if not resend
                        int mask = ((1 << i) - 1);
                        if ((mask & sequenceAckField) != mask) {
                            System.out.println("resend from mask");
                            resend = true;
                        }
                        break;
                    }
                }
            }
            //resend if it is requested
            if (readFlag(msg, REQUEST_RESEND)) {
                System.out.println("resend because requested");
                resend = true;
            }

            //resend if necessary
            if (resend) {
                resend();
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
                    reliableMessageBuffer.addLast(null);
                }
            }
            //add it to the buffer
            reliableMessageBuffer.set(msgSequence - messageBufferOffset, msg);
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
                //update partialMessagesLength
                byte[] firstMsg = reliableMessageBuffer.getFirst();
                if (firstMsg != null) {
                    partialMessageLength = readFlag(firstMsg, IS_PARTIAL) ?  readShort(firstMsg, 7) : 1;
                }

                Iterator<byte[]> iter = reliableMessageBuffer.iterator();
                int amount = 0;
                int totalSize = 0;
                while (amount < partialMessageLength && iter.hasNext()) {
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
         * resend the messages that are not acknowledged
         */
        private void resend() {
            if (System.currentTimeMillis() - lastResendTimestamp > MIN_RESEND_TIMEOUT) {
                int index = 0;
                Iterator<byte[]> iter = sentMessagesBuffer.iterator();
                while (index < 32 && iter.hasNext()) {
                    byte[] msg = iter.next();
                    if ((sequenceAckField & (1 << index)) != 0) {
                        sendMsgInternal(msg);
                    }
                    index++;
                }
                lastResendTimestamp = System.currentTimeMillis();
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
                    System.out.println("resend from timeout");
                    resend();
                }
            }
        }

        public int getRemoteID() {
            return remoteID;
        }

    }
}
