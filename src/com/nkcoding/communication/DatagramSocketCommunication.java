package com.nkcoding.communication;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DatagramSocketCommunication extends Communication {

    private static final int RECEIVE_TIMEOUT = 800;
    private static final int RESEND_TIMEOUT = 100;

    static final byte IS_SYSTEM = 0x01;
    static final byte IS_RELIABLE = 0x02;
    static final byte IS_PARTIAL = 0x04;
    static final byte IS_INDIRECT = 0x20;
    static final byte IS_OPEN_CONNECTION = 0x40;
    static final byte REQUEST_RESEND = (byte)0x80;

    private static final short PROTOCOL_ID = 8001;
    private static final int HEADER_SIZE = 21;
    private static final int MAX_SIZE = 1310;

    /**
     * requests to open a connection
     * 0 arguments (right now)
     */
    private static final short OPEN_CONNECTION = 1;
    /**
     * acks the open connection
     * short: new id (or -1 if not server)
     */
    private static final short ACK_OPEN_CONNECTION = 2;
    /**
     * sends a timeout warning, also used to update
     * short: clientID
     */
    private static final short TIMEOUT_WARNING = 3;
    /**
     * sends an acl fpr tje timeout warning, used to update
     * requests a resend (normally)
     * 0 arguments
     */
    private static final short ACK_TIMEOUT_WARNING = 8;
    /**
     * sets a connection to indirect if it was set by one side to indirect
     * 0 arguments
     */
    private static final short SET_INDIRECT = 4;
    /**
     * sets the peers
     * short amount
     * (short id, short addressLength, byte[addressLength] address, int port)[length]
     */
    private static final short ADD_PEERS = 7;
    /**
     * sends all (possibly) unsent messages via redirection
     * these than are handled like normal messages, because the complete ack / system is not touched after redirect
     * is initialized
     * short amount
     * (short msgLength, msg)[amount]
     */
    private static final short REDIRECT_UNSENT_MSG = 9;



    /**
     * the id for this client
     */
    private short clientID = -1;

    /**
     * is this the server?
     */
    private boolean isServer;

    private int port;

    /**
     * the next id for a client
     * is increased, if it is used
     * has only effect if isServer
     */
    private short idCounter = 1;

    /**
     * list with all received transmissions, ready for the user
     */
    private final ConcurrentLinkedQueue<DataInputStream> receivedTransmissions;

    /**
     * map with all connections
     */
    private final ConcurrentMap<Short, Connection> connections;

    /**
     * set with all indexes of connections, used for faster return
     */
    private final CopyOnWriteArraySet<Short> peerSet;

    private final Deque<ResetDataOutputStream> outputStreamPool;

    /**
     * the Thread that handles all read on the DatagramSocket
     */
    private Thread readingThread;

    private DatagramSocket datagramSocket;
    private final DatagramPacket defaultReceivePacket;

    //arrays that represent the standard headers for new transmissions
    private byte[] reliableHeader = new byte[21];
    private byte[] unreliableHeader = new byte[21];

    /**
     * create a new communication instance
     *
     * @param isServer should it be the server?
     * @param port     the port to use
     */
    public DatagramSocketCommunication(boolean isServer, int port) {
        super(isServer, port);
        //initialize data structures
        this.isServer = isServer;
        if (isServer) {
            setClientID((short)0);
        }
        this.port = port;
        receivedTransmissions = new ConcurrentLinkedQueue<>();
        connections = new ConcurrentHashMap<>();
        peerSet = new CopyOnWriteArraySet<>();
        outputStreamPool = new ArrayDeque<>();
        setFlag(reliableHeader, IS_RELIABLE, true);
        setFlag(unreliableHeader, IS_RELIABLE, false);
        try {
            datagramSocket = new DatagramSocket(port);
        } catch (SocketException e) {
            System.out.println("exception creating DatagramSocket");
            e.printStackTrace();
        }
        defaultReceivePacket = new DatagramPacket(new byte[MAX_SIZE + 10], MAX_SIZE + 10);
        readingThread = new Thread() {
            @Override
            public void run() {
                super.run();
                while (!datagramSocket.isClosed()) {
                    try {
                        datagramSocket.receive(defaultReceivePacket);
                        handleMsg(Arrays.copyOf(defaultReceivePacket.getData(), defaultReceivePacket.getLength()), defaultReceivePacket);
                    } catch (IOException e) {
                        System.err.println("error while reading packet");
                        e.printStackTrace();
                    }
                }
            }
        };
        readingThread.start();
    }

    @Override
    public void openCommunication(String ip, int port) {
        InetSocketAddress address = new InetSocketAddress(ip, port);
        openConnection(address, 0);
    }

    private void openConnection(InetSocketAddress address, int remoteID) {
        Connection connection = new Connection(address, (short)remoteID);
        connection.startAndConnect();
    }

    @Override
    public synchronized ResetDataOutputStream getOutputStream(boolean reliable) {
        ResetDataOutputStream dataOutputStream;
        if (outputStreamPool.isEmpty()) {
            dataOutputStream = new ResetDataOutputStream();
        } else {
            dataOutputStream = outputStreamPool.poll();
        }
        try {
            dataOutputStream.write(reliable ? reliableHeader : unreliableHeader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataOutputStream;
    }

    @Override
    public synchronized void sendTo(short peer, ResetDataOutputStream transmission) {
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
        for (short peer : peerSet) {
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

    /**
     * internal helper method to send a msg to a specific peer
     */
    private void sendTo(short peer, byte[] msg) {
        writeShort(msg, 3, clientID);
        writeShort(msg, 0, PROTOCOL_ID);
        Connection connection = connections.get(peer);
        if (connection != null) {
            connection.send(msg);
        } else {
            System.out.println("cannot send to " + peer);
        }
    }

    @Override
    public boolean hasTransmissions() {
        return !receivedTransmissions.isEmpty();
    }

    @Override
    public DataInputStream getTransmission() {
        return receivedTransmissions.poll();
    }

    @Override
    public Set<Short> getPeers() {
        return Collections.unmodifiableSet(peerSet);
    }

    @Override
    public short getId() {
        return clientID;
    }

    @Override
    public void close() {
        this.readingThread.interrupt();
        datagramSocket.close();
        for (Connection connection : connections.values()) {
            connection.close();
        }
    }

    /**
     * handle a message
     * DOES NOT MODIFY source
     */
    private void handleMsg(byte[] msg, DatagramPacket source) {
        System.out.println("---------------MESSAGE-----------------");
        printMsg(msg);

        //redirect if necessary
        boolean isIndirect = readFlag(msg, IS_INDIRECT);
        if (isIndirect && isServer) {
            short target = readShort(msg, 5);
            short origin = readShort(msg, 3);
            writeShort(msg, 5, origin);
            writeShort(msg, 3, target);
            sendTo(target, msg);
        } else if (readFlag(msg, IS_OPEN_CONNECTION)) {
            InetSocketAddress socketAddress = (InetSocketAddress)source.getSocketAddress();
            String ip = socketAddress.getAddress().getHostAddress();
            int port = socketAddress.getPort();
            System.out.printf("incoming connection: ip=%s, port=%d%n", ip, port);

            if (isServer) {
                try {
                    idCounter++;
                    Connection connection = new Connection(socketAddress, idCounter);
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                    byte[] headerAndID = getSystemMsg(ADD_PEERS, 2);
                    dataOutputStream.write(headerAndID);
                    short connectionCount = 0;
                    for (Connection connectTo : connections.values()) {
                        connectionCount++;
                        dataOutputStream.writeShort(connectTo.getRemoteID());
                        byte[] address = connectTo.socketAdress.getAddress().getAddress();
                        dataOutputStream.writeShort(address.length);
                        dataOutputStream.write(address);
                        dataOutputStream.writeInt(connectTo.socketAdress.getPort());
                    }
                    dataOutputStream.flush();
                    byte[] peersMsg = outputStream.toByteArray();
                    writeShort(peersMsg, HEADER_SIZE + 2, connectionCount);
                    dataOutputStream.close();
                    //send with the additional init message
                    connection.startAndAcknowledge(false, peersMsg);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }

            } else {
                Connection connection = new Connection(socketAddress, readShort(msg, HEADER_SIZE + 2));
                connection.startAndAcknowledge(false);
            }
        } else {
            short remoteID = readShort(msg, 3);
            Connection connection = connections.get(remoteID);
            if (connection != null) {
                connection.receive(msg);
            } else {
                if (isIndirect) {
                    System.out.println("create new client via indirect");
                    InetSocketAddress socketAddress = (InetSocketAddress)source.getSocketAddress();
                    Connection newIndirectConnection = new Connection(socketAddress, remoteID);
                    newIndirectConnection.startAndAcknowledge(true);
                } else {
                    System.err.println("CANNOT RECEIVE MSG");
                    printMsg(msg);
                }
            }
        }
    }

    private synchronized void sendMsgInternal(byte[] msg, InetSocketAddress socketAddress) {
        DatagramPacket packet = new DatagramPacket(msg, msg.length,socketAddress);
        try {
            datagramSocket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setClientID(short clientID) {
        this.clientID = clientID;
        writeShort(reliableHeader, 3, clientID);
        writeShort(unreliableHeader, 3, clientID);
    }

    public short getClientID() {
        return clientID;
    }

    /**
     * remote a connection
     */
    private void removeConnection(short remoteID) {
        Connection connection = connections.remove(remoteID);
        if (connection != null) {
            connection.close();
        }
        peerSet.remove(remoteID);
        //TODO notify program?
        //TODO remove from other peers?
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

    /**
     * create a system message which is reliable, has set the id as first short and has set protocol and client id
     */
    private byte[] getSystemMsg(short id, int dataLength) {
        byte[] msg = new byte[HEADER_SIZE + 4 + dataLength];
        setFlag(msg, IS_RELIABLE, true);
        setFlag(msg, IS_SYSTEM, true);
        writeShort(msg, 0, PROTOCOL_ID);
        writeShort(msg, 3, clientID);
        writeShort(msg, HEADER_SIZE, id);
        return msg;
    }

    static void printMsg(byte[] msg) {
        System.out.printf("protocol id: %d%n", readShort(msg, 0));
        System.out.printf("client id: %d%n", readShort(msg, 3));
        System.out.printf("sys: %b reliable: %b partial: %b indirect: %b open: %b%n",
                readFlag(msg, IS_SYSTEM), readFlag(msg, IS_RELIABLE), readFlag(msg, IS_PARTIAL),
                readFlag(msg, IS_INDIRECT), readFlag(msg, IS_OPEN_CONNECTION));
        System.out.printf("indirect target: %d%n", readShort(msg, 5));
        System.out.printf("amount parts: %d%n", readShort(msg, 7));
        System.out.printf("sequence: %d%n", readInt(msg, 9));
        System.out.printf("ack: %d%n", readInt(msg, 13));
        System.out.printf("ack field: %s%n", String.format("%16s",
                Integer.toBinaryString(readInt(msg, 17))).replace(' ', '0'));
    }

    private class Connection extends Thread implements Closeable {
        private short remoteID = -1;

        private InetSocketAddress socketAdress;

        private volatile boolean connected = false;

        /**
         * shows if a connection handles messages on its own or uses a client server model
         * if it is indirect, the Connection loses basically oll its important features, like ack, sequence and sequenceAck
         */
        private volatile boolean isIndirect = false;

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

        private int timeoutCounter = 0;

        /**
         * default constructor with well known id
         */
        public Connection(InetSocketAddress socketAddress, short remoteID) {
            this(socketAddress);
            setRemoteID(remoteID);
        }

        /**
         * Constructor with no id
         */
        public Connection(InetSocketAddress socketAddress) {
            this.socketAdress = socketAddress;
            this.lastResendTimestamp = System.currentTimeMillis();
        }

        /**
         * starts the thread, and connects
         */
        public void startAndConnect() {
            start();
            connect();
        }

        /**
         * starts the thread and acks the connection
         * @param msgs should only be reliable messages
         */
        public void startAndAcknowledge(boolean indirect, byte[]... msgs) {
            this.isIndirect = indirect;
            this.connected = true;
            start();
            byte[] ackConnectionMsg = getSystemMsg(ACK_OPEN_CONNECTION, 2);
            writeShort(ackConnectionMsg, HEADER_SIZE + 2, remoteID);
            send(ackConnectionMsg);
            for (byte[] msg : msgs) {
                send(msg);
            }
        }


        /**
         * start the connecting process
         */
        private void connect() {
            Thread connectThread = new Thread() {
                @Override
                public void run() {
                    super.run();
                    for (int i = 0; i < 5; i++) {
                        if (!connected) {
                            connectInternal();
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    //set to indirect if it was not possible to connect
                    if (!connected) {
                        if (remoteID == 0) {
                            System.out.println("failed to connect to server");
                            throw new IllegalStateException();
                        } else {
                            System.out.println("failed to connect, set to indirect");
                            activateIndirect(true);
                        }
                    }
                }
            };
            connectThread.start();
        }

        /**
         * called internally
         */
        private void connectInternal() {
            if (!connected) {
                byte[] msg = getSystemMsg(OPEN_CONNECTION, 0);
                setFlag(msg, IS_RELIABLE, false);
                sendInternal(msg);
            }
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
                if (readFlag(msg, IS_RELIABLE)) sequence++;
                sentMessagesBuffer.addLast(msg);
                sendMsgInternal(msg, socketAdress);
            }
        }

        /**
         * receive a message
         * @param msg
         */
        public void receive(byte[] msg) {
            try {
                this.receiveQueue.put(msg);
            } catch (InterruptedException e) {
                System.err.println("unable to put message");
                e.printStackTrace();
            }
        }

        /**
         * handles a raw received message byte array
         */
        private void receiveInternal(byte[] msg) {
            timeoutCounter = 0;
            //only check for correct indirect message if not received from server
            if (remoteID != 0 && readFlag(msg, IS_INDIRECT)) {
                Connection indirectConnection = connections.get(readShort(msg, 5));
                if (indirectConnection != null) {
                    indirectConnection.handleMessage(msg, readFlag(msg, IS_SYSTEM));
                } else {
                    System.err.println("could not transmit to correct indirect connection");
                    printMsg(msg);
                }
            }
            handleSequenceAckAndResend(msg);
            if (readFlag(msg, IS_RELIABLE)) {
                handleReliableMessage(msg);
            } else {
                //special handling to use unreliable messages to request a resend
                int seq = readInt(msg, 9);
                if (seq > ack) {
                    resendRequestCounter++;
                }
                handleReceivedMessage(msg);
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
                        handleReceivedMessage(msg);
                        reliableMessageBuffer.addLast(null);
                    } else {
                        byte[] msg = new byte[HEADER_SIZE + totalSize - amount * HEADER_SIZE];
                        int newPos = HEADER_SIZE;
                        System.arraycopy(reliableMessageBuffer.peekFirst(), 0, msg, 0, HEADER_SIZE);
                        for (int i = 0; i < amount; i++) {
                            byte[] part = reliableMessageBuffer.removeFirst();
                            int length = part.length - HEADER_SIZE;
                            System.arraycopy(part, HEADER_SIZE, msg, newPos, length);
                            newPos += length;
                            reliableMessageBuffer.addLast(null);
                        }
                        handleReceivedMessage(msg);
                    }
                    //update offset
                    messageBufferOffset += amount;
                }
            }
        }

        /**
         * handles a complete received message
         * redirects message if necessary
         */
        private void handleReceivedMessage(byte[] msg) {
            if (readFlag(msg, IS_INDIRECT)) {
                Connection indirectConnection = connections.get(readShort(msg, 5));
                if (indirectConnection != null) {
                    indirectConnection.receive(msg);
                } else {
                    System.err.println("could not transmit to correct indirect connection");
                    printMsg(msg);
                }
            }

            //send it if necessary to the correct connection
            boolean isSystem = readFlag(msg, IS_SYSTEM);
            handleMessage(msg, isSystem);
        }

        /**
         * adds a message to receivedTransmissions or handles it internal
         */
        void handleMessage(byte[] msg, boolean isSystem) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(msg);
            long skipped = inputStream.skip(HEADER_SIZE);
            inputStream.mark(0);
            if (skipped != HEADER_SIZE) System.out.println("error: could not skip enough: " + skipped);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            if (isSystem) {
                handleSystemMessage(dataInputStream);
            } else {
                //add it directly to the output queue
                receivedTransmissions.add(dataInputStream);
            }
        }

        /**
         * a timeout has occured, requests a resend if necessary and goes indirect if possible,
         * else it shuts down
         */
        private void timeout() {
            System.out.println("TIMEOUT WARNING");
            timeoutCounter++;
            if (timeoutCounter > 8) {
                if (isIndirect) {
                    System.err.println("too many timeouts even if indirect, shutdown: " + remoteID);
                    removeConnection(remoteID);
                } else {
                    //set to indirect as a last try
                    timeoutCounter = 0;
                    activateIndirect(true);
                }
            }
            if (connected) {
                byte[] timeoutMsg = getSystemMsg(TIMEOUT_WARNING, 0);
                send(timeoutMsg);
                resend();
            }
        }

        private void activateIndirect(boolean sendToPeer) {
            isIndirect = true;
            if (sendToPeer) {
                byte[] indirectMsg = getSystemMsg(SET_INDIRECT, 0);
                send(indirectMsg);
            }
            //send all the unsent messages
            try {
                byte[] headerAndID = getSystemMsg(REDIRECT_UNSENT_MSG, 0);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                dataOutputStream.write(headerAndID);
                dataOutputStream.writeShort(sentMessagesBuffer.size());
                for (byte[] msg : sentMessagesBuffer) {
                    dataOutputStream.writeShort(msg.length);
                    dataOutputStream.write(msg);
                }
                dataOutputStream.flush();
                byte[] unsentMsgMsg = outputStream.toByteArray();
                send(unsentMsgMsg);
                dataOutputStream.close();
            } catch (IOException e) {
                System.err.println("cannot write sentMessagesBuffer");
            }
        }

        /**
         * resend the messages that are not acknowledged
         */
        private void resend() {
            if (System.currentTimeMillis() - lastResendTimestamp > RESEND_TIMEOUT && !isIndirect) {
                System.out.println("RESEND");
                int index = 0;
                Iterator<byte[]> iter = sentMessagesBuffer.iterator();
                while (index < 32 && iter.hasNext()) {
                    byte[] msg = iter.next();
                    if ((sequenceAckField & (1 << index)) != 0) {
                        sendMsgInternal(msg, socketAdress);
                    }
                    index++;
                }
                lastResendTimestamp = System.currentTimeMillis();
            }
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
                    timeout();
                }
            }
        }

        public int getRemoteID() {
            return remoteID;
        }

        private void setRemoteID(short remoteID) {
            if (this.remoteID != remoteID) {
                if (connections.containsKey(this.remoteID)) {
                    System.out.println("previous peer id: " + this.remoteID);
                    System.out.println("HAVE TO REMOVE CONNECTION: NEW ID");
                    connections.remove(this.remoteID);
                }
                peerSet.remove(this.remoteID);

                this.remoteID = remoteID;

                peerSet.add(remoteID);
                connections.put(remoteID, this);
            }
        }

        @Override
        public void close() {
            shutdown = true;
        }

        /**
         * handles a system message
         */
        private void handleSystemMessage(DataInputStream inputStream) {
            try {
                short msgID = inputStream.readShort();
                switch (msgID) {
                    case OPEN_CONNECTION:
                        //if this message comes in, just ignore for this time
                        System.err.println("received open message, ignore");
                        break;
                    case ACK_OPEN_CONNECTION:
                        short newClientID = inputStream.readShort();
                        if (newClientID != -1) {
                            if (newClientID == clientID) {
                                System.out.println("received same clientID, do nothing");
                            } else {
                                if (clientID == -1) {
                                    System.out.println("updated clientID");
                                    setClientID(newClientID);
                                } else {
                                    System.err.println("try to overwrite clientID");
                                }
                            }
                        }
                        break;
                    case TIMEOUT_WARNING:
                        System.out.println("TIMEOUT WARNING");
                        byte[] timeoutWarningAck = getSystemMsg(ACK_TIMEOUT_WARNING, 0);
                        send(timeoutWarningAck);
                        break;
                    case ACK_TIMEOUT_WARNING:
                        System.out.println("Acked timout warning");
                        //do nothing, it fullfilled already its reason
                        break;
                    case SET_INDIRECT:
                        System.out.println("SET INDIRECT");
                        //this also sends the msg
                        activateIndirect(false);
                        break;
                    case ADD_PEERS:
                        System.out.println("add peers");
                        short amountPeers = inputStream.readShort();
                        for (int i = 0; i < amountPeers; i++) {
                            short id = inputStream.readShort();
                            short addressLength = inputStream.readShort();
                            byte[] address = inputStream.readNBytes(addressLength);
                            int port = inputStream.readInt();
                            InetSocketAddress newSocketAddress = new InetSocketAddress(InetAddress.getByAddress(address), port);
                            openConnection(newSocketAddress, id);
                        }
                        break;
                    case REDIRECT_UNSENT_MSG:
                        System.out.println();
                        short amountMsgs = inputStream.readShort();
                        for (int i = 0; i < amountMsgs; i++) {
                            short msgLength = inputStream.readShort();
                            byte[] msg = inputStream.readNBytes(msgLength);
                            //receive this message the normal way
                            receiveInternal(msg);
                        }
                        break;
                    default:
                        System.err.println("unrecognized system msg: " + msgID);
                        throw new IllegalStateException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
