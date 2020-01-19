package com.nkcoding.communication;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DatagramSocketCommunication extends Communication {

    static final byte IS_SYSTEM = 0x01;
    static final byte IS_RELIABLE = 0x02;
    static final byte IS_PARTIAL = 0x04;
    static final byte IS_INDIRECT = 0x20;
    static final byte IS_OPEN_CONNECTION = 0x40;
    static final byte REQUEST_RESEND = (byte) 0x80;
    private static final int RECEIVE_TIMEOUT = 800;
    private static final int RESEND_TIMEOUT = 100;
    private static final short PROTOCOL_ID = 8001;
    private static final int HEADER_SIZE = 21;
    private static final int MAX_SIZE = 29;//Communication.MAX_SIZE + 25;
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
     * acks the ack for the open connection
     * 0 arguments (right now)
     */
    private static final short ACK_ACK_OPEN_CONNECTION = 10;
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
     * requests a list of peers to connect to
     * 0 arguments
     */
    private static final short REQUEST_PEERS = 11;
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
    private static boolean doDebug = false;
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
    private final DatagramPacket defaultReceivePacket;
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
    private short idCounter = 0;
    /**
     * the Thread that handles all read on the DatagramSocket
     */
    private Thread readingThread;
    private DatagramSocket datagramSocket;
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
            setClientID((short) 0);
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
            if (doDebug) System.out.println("exception creating DatagramSocket");
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
                        if (doDebug) System.out.println("error while reading packet");
                        e.printStackTrace();
                    }
                }
            }
        };
        readingThread.start();
    }

    public static void writeInt(byte[] msg, int offset, int val) {
        msg[offset] = (byte) (val >>> 24);
        msg[offset + 1] = (byte) (val >>> 16);
        msg[offset + 2] = (byte) (val >>> 8);
        msg[offset + 3] = (byte) (val);
    }

    public static void writeShort(byte[] msg, int offset, short val) {
        msg[offset] = (byte) (val >>> 8);
        msg[offset + 1] = (byte) (val);
    }

    public static int readInt(byte[] msg, int offset) {
        return ((msg[offset] & 0xFF) << 24)
                + ((msg[offset + 1] & 0xFF) << 16)
                + ((msg[offset + 2] & 0xFF) << 8)
                + (msg[offset + 3] & 0xFF);
    }

    static short readShort(byte[] msg, int offset) {
        return (short) (((msg[offset] & 0xFF) << 8)
                + (msg[offset + 1] & 0xFF));
    }

    static void setFlag(byte[] msg, byte flag, boolean value) {
        if (value) {
            msg[2] |= flag;
        } else {
            msg[2] &= ~flag;
        }
    }

    static boolean readFlag(byte[] msg, byte flag) {
        return ((msg[2] & flag) & 0xFF) != 0;
    }

    static void printMsg(byte[] msg) {
        System.out.println("message:");
        System.out.printf("protocol id: %d%n", readShort(msg, 0));
        System.out.printf("client id: %d%n", readShort(msg, 3));
        System.out.printf("sys=%b reliable=%b partial=%b indirect=%b open=%b%n",
                readFlag(msg, IS_SYSTEM), readFlag(msg, IS_RELIABLE), readFlag(msg, IS_PARTIAL),
                readFlag(msg, IS_INDIRECT), readFlag(msg, IS_OPEN_CONNECTION));
        System.out.printf("indirect target: %d%n", readShort(msg, 5));
        System.out.printf("amount parts: %d%n", readShort(msg, 7));
        System.out.printf("sequence: %d%n", readInt(msg, 9));
        System.out.printf("ack: %d%n", readInt(msg, 13));
        System.out.printf("ack field: %s%n", String.format("%16s",
                Integer.toBinaryString(readInt(msg, 17))).replace(' ', '0'));
        if (msg.length >= HEADER_SIZE + 2) {
            switch (readShort(msg, HEADER_SIZE)) {
                case OPEN_CONNECTION:
                    System.out.println("supposed type: OPEN_CONNECTION");
                    break;
                case ACK_OPEN_CONNECTION:
                    System.out.println("supposed type: ACK_OPEN_CONNECTION");
                    break;
                case ACK_ACK_OPEN_CONNECTION:
                    System.out.println("supposed type: ACK_ACK_OPEN_CONNECTION");
                    break;
                case TIMEOUT_WARNING:
                    System.out.println("supposed type: TIMEOUT_WARNING");
                    break;
                case ACK_TIMEOUT_WARNING:
                    System.out.println("supposed type: ACK_TIMEOUT_WARNING");
                    break;
                case SET_INDIRECT:
                    System.out.println("supposed type: SET_INDIRECT");
                    break;
                case ADD_PEERS:
                    System.out.println("supposed type: ADD_PEERS");
                    break;
                case REDIRECT_UNSENT_MSG:
                    System.out.println("supposed type: REDIRECT_UNSENT_MSG");
                    break;
                default:
                    System.out.println("cannot recognize type");
            }
        }
        System.out.print("-------------------------------------");
        for (int i = HEADER_SIZE; i < msg.length; i++) {
            if ((i - HEADER_SIZE) % 4 == 0) {
                System.out.println();
            }
            System.out.print(String.format("%2x", msg[i] & 0xFF).replace(' ', '0') + ' ');
        }
        System.out.println();
        System.out.println("-------------------------------------");
    }

    @Override
    public void openCommunication(String ip, int port) {
        InetSocketAddress address = new InetSocketAddress(ip, port);
        openConnection(address, 0);
    }

    private void openConnection(InetSocketAddress address, int remoteID) {
        Connection connection = new Connection(address, (short) remoteID);
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
            if (doDebug) System.out.println("cannot send to " + peer);
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
        //region debugging
//        if (Math.random() < 0.5) {
//            return;
//        }
        if (!isServer && readFlag(msg, IS_OPEN_CONNECTION) && !readFlag(msg, IS_INDIRECT) && source.getPort() == 8000) {
            System.out.println("drop initial message from other client");
            return;
        }
        //endregion

        if (readShort(msg, 0) != PROTOCOL_ID) {
            System.err.println("received illegal package");
            return;
        }

        if (readFlag(msg, IS_OPEN_CONNECTION)) {
            connectTo(msg, source);
        } else {
            deliverMsg(msg);
        }
    }

    /**
     * sens a message to its related connection
     */
    private void deliverMsg(byte[] msg) {
        short remoteID = readShort(msg, 3);
        Connection connection = connections.get(remoteID);
        if (connection != null) {
            connection.receive(msg);
        } else {
            System.out.println("CANNOT RECEIVE MSG");
            printMsg(msg);
        }
    }

    /**
     * handles a open connection message
     */
    private void connectTo(byte[] msg, DatagramPacket source) {
        InetSocketAddress socketAddress = (InetSocketAddress) source.getSocketAddress();
        String ip = socketAddress.getAddress().getHostAddress();
        int port = socketAddress.getPort();
        System.out.printf("incoming connection: ip=%s, port=%d%n", ip, port);

        Connection connection;
        if (isServer) {
            if (readFlag(msg, IS_INDIRECT)) {
                deliverMsg(msg);
            } else {
                idCounter++;
                connection = new Connection(socketAddress, idCounter);
                //send with the additional init message
                System.out.println("start from server");
                connection.startAndAcknowledge(false);
                connection.receive(msg);
            }
        } else {
            if (readFlag(msg, IS_INDIRECT)) {
                deliverMsg(msg);
            } else {
                System.out.println("start from client");
                connection = new Connection(socketAddress, readShort(msg, HEADER_SIZE + 2));
                connection.startAndAcknowledge(readFlag(msg, IS_INDIRECT));
                connection.receive(msg);
            }

        }
        //receive the msg because this is now necessary because it is reliable
    }

    private byte[] getPeerData(int withoutPeer) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            dataOutputStream.writeShort(0); //placeholder
            short connectionCount = 0;
            for (Short remoteID : peerSet) {
                Connection connectTo = connections.get(remoteID);
                if (connectTo.remoteID != withoutPeer && connectTo.isConnected) {
                    connectionCount++;
                    dataOutputStream.writeShort(connectTo.getRemoteID());
                    byte[] address = connectTo.socketAdress.getAddress().getAddress();
                    dataOutputStream.writeShort(address.length);
                    dataOutputStream.write(address);
                    dataOutputStream.writeInt(connectTo.socketAdress.getPort());
                }
            }
            dataOutputStream.flush();
            byte[] peersMsg = outputStream.toByteArray();
            writeShort(peersMsg, 0, connectionCount);
            dataOutputStream.close();
            return peersMsg;
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }
    }

    private synchronized void sendMsgInternal(byte[] msg, InetSocketAddress socketAddress) {
        DatagramPacket packet = new DatagramPacket(msg, msg.length, socketAddress);
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

    /**
     * remote a connection
     */
    private void removeConnection(short remoteID) {
        System.out.println("removed connection " + remoteID);
        Connection connection = connections.remove(remoteID);
        if (connection != null) {
            connection.close();
        }
        peerSet.remove(remoteID);
        //TODO notify program?
        //TODO remove from other peers?
    }

    /**
     * create a system message which is reliable, has set the id as first short and has set protocol and client id
     */
    private byte[] getSystemMsg(short id, int dataLength) {
        byte[] msg = new byte[HEADER_SIZE + 2 + dataLength];
        setFlag(msg, IS_RELIABLE, true);
        setFlag(msg, IS_SYSTEM, true);
        writeShort(msg, 0, PROTOCOL_ID);
        writeShort(msg, 3, clientID);
        writeShort(msg, HEADER_SIZE, id);
        return msg;
    }

    private class Connection extends Thread implements Closeable {
        private static final int TIMEOUT_RETRIES = 10;
        private short remoteID = -1;

        private InetSocketAddress socketAdress;

        private volatile boolean isConnected = false;
        /**
         * initial message that is resend even if it is not connected
         */
        private byte[] initialMsg = null;
        /**
         * list with unsent indirect messages because the connection was not initialised
         */
        private List<byte[]> unsentIndirectMsgs = new ArrayList<>();

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
         * the sequence Number that is expected next
         */
        private int sequenceAck = 0;

        private int sequenceAckField = 0;

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
            for (int i = 0; i < 32; i++) {
                reliableMessageBuffer.add(null);
            }
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
            byte[] msg = getSystemMsg(OPEN_CONNECTION, 2);
            //setFlag(msg, IS_RELIABLE, false);
            setFlag(msg, IS_OPEN_CONNECTION, true);
            writeShort(msg, HEADER_SIZE + 2, clientID);
            sendInitialMessage(msg);
        }

        /**
         * starts the thread and acks the connection
         *
         * @param msgs should only be reliable messages
         */
        public void startAndAcknowledge(boolean indirect, byte[]... msgs) {
            this.isIndirect = indirect;
            start();
            byte[] ackConnectionMsg = getSystemMsg(ACK_OPEN_CONNECTION, 2);
            writeShort(ackConnectionMsg, HEADER_SIZE + 2, remoteID);
            sendInitialMessage(ackConnectionMsg);
            for (byte[] msg : msgs) {
                send(msg);
            }
        }

        /**
         * handles all partial stuff, rest is done by sendInternal
         *
         * @param msg the message to send
         */
        private synchronized void send(byte[] msg) {
            if (readFlag(msg, IS_RELIABLE) && msg.length > MAX_SIZE) {
                int contentSize = MAX_SIZE - HEADER_SIZE;
                int parts = (int) Math.ceil((msg.length - HEADER_SIZE) / (float) (contentSize));
                for (int i = 0; i < parts - 1; i++) {
                    byte[] msgPart = new byte[HEADER_SIZE + contentSize];
                    System.arraycopy(msg, 0, msgPart, 0, HEADER_SIZE);
                    System.arraycopy(msg, contentSize * i + HEADER_SIZE, msgPart, HEADER_SIZE, contentSize);
                    writeShort(msgPart, 7, (short) parts);
                    setFlag(msgPart, IS_PARTIAL, true);
                    sendInternal(msgPart);
                }
                //send the last part which has a special length
                byte[] msgLastPart = new byte[msg.length - contentSize * (parts - 1)];
                System.arraycopy(msg, 0, msgLastPart, 0, HEADER_SIZE);
                System.arraycopy(msg, contentSize * (parts - 1) + HEADER_SIZE, msgLastPart, HEADER_SIZE, msgLastPart.length - HEADER_SIZE);
                writeShort(msgLastPart, 7, (short) parts);
                setFlag(msgLastPart, IS_PARTIAL, true);
                sendInternal(msgLastPart);
            } else {
                //nothing to do here, if it is to long than there's nothing I can do
                sendInternal(msg);
            }
        }

        /**
         * sends a message
         * requires the message to be completely initialized
         *
         * @param msg the message to send
         */
        private void sendInternal(byte[] msg) {
            prepareMsgToSend(msg);
            if (isConnected) {
                sendFinal(msg);
            } else if (readFlag(msg, IS_INDIRECT)) {
                System.out.println("add unsent indirect msg");
                unsentIndirectMsgs.add(msg);
            }
        }

        private void sendInitialMessage(byte[] initialMsg) {
            prepareMsgToSend(initialMsg);
            if (!isIndirect) {
                this.initialMsg = initialMsg;
            }
            sendFinal(initialMsg);
        }

        /**
         * sets isIndirect and indirectTarget if indirect
         * sets remoteId, sequence, ack and ackField if NOT indirect
         * requires the following fields to be set:
         * <ul>
         *     <li>protocolID</li>
         *     <li>clientID</li>
         *     <li>isSystem, isReliable, </li>
         *     <li>all partial stuff</li>
         * </ul>
         */
        private void prepareMsgToSend(byte[] msg) {
            if (isIndirect) {
                //set indirect flag
                setFlag(msg, IS_INDIRECT, true);
                writeShort(msg, 5, remoteID);
            } else {
                setFlag(msg, REQUEST_RESEND, resendRequestCounter > 3);
                writeShort(msg, 3, clientID);
                writeInt(msg, 9, sequence);
                writeInt(msg, 13, ack);
                writeInt(msg, 17, ackField);
                if (readFlag(msg, IS_RELIABLE)) {
                    sequence++;
                    sentMessagesBuffer.addLast(msg);
                }
            }
        }

        /**
         * sends the message finally to its destination
         */
        private synchronized void sendFinal(byte[] msg) {
            if (isIndirect) {
                connections.get((short) 0).sendInternal(msg);
            } else {
                sendMsgInternal(msg, socketAdress);
            }
        }

        /**
         * receive a message
         */
        public void receive(byte[] msg) {
            try {
                this.receiveQueue.put(msg);
            } catch (InterruptedException e) {
                System.out.println("unable to put message");
                e.printStackTrace();
            }
        }

        /**
         * handles a raw received message byte array
         */
        private void receiveInternal(byte[] msg) {
            timeoutCounter = 0;
            //only check for correct indirect message if not received from server
            if (remoteID != 0 && readFlag(msg, IS_INDIRECT) && !isServer()) {
                handleMessage(msg, readFlag(msg, IS_SYSTEM));
            } else {
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
        }

        private synchronized void handleSequenceAckAndResend(byte[] msg) {
            //new sequenceAck
            int newSequenceAck = readInt(msg, 13);
            //new sequence
            int seq = readInt(msg, 9);
            boolean resend = false;
            //check if the packet is new enough to do all this stuff
            if (newSequenceAck >= sequenceAck) {
                //reset the counter, because a new reliable msg was registered, so a request would happen on its own
                if (seq > ack && readFlag(msg, IS_RELIABLE)) {
                    resendRequestCounter = 0;
                }
                //update sequenceAck, sequenceAckField and the sentMessagesBuffer
                if (newSequenceAck - sequenceAck < 32) {
                    sequenceAckField >>>= (newSequenceAck - sequenceAck);
                } else {
                    sequenceAckField = 0;
                }

                for (int i = 0; i < newSequenceAck - sequenceAck; i++) {
                    //these messages are proven sent, so remove these
                    sentMessagesBuffer.removeFirst();
                }
                sequenceAckField |= readInt(msg, 17);
                sequenceAck = newSequenceAck;
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
                if (doDebug) System.out.println("resend because requested");
                resend = true;
            }

            //resend if necessary
            if (resend) {
                resend();
            }
        }

        private synchronized void handleReliableMessage(byte[] msg) {
            //get the message sequence
            int msgSequence = readInt(msg, 9);
            if (msgSequence < ack) {
                System.out.println("received delayed message, discard: " + msgSequence + ", " + ack);
                return;
            }
            //ensure the capacity
            if (msgSequence - messageBufferOffset >= reliableMessageBuffer.size()) {
                for (int i = reliableMessageBuffer.size(); i <= msgSequence + messageBufferOffset + 1; i++) {
                    reliableMessageBuffer.addLast(null);
                }
            }

            //add it to the buffer
            reliableMessageBuffer.set(msgSequence - messageBufferOffset, msg);
            //check if it has to be handled by updateAcknowledged
            //these values have two different meanings
            //DON'T DO THIS EVER AGAIN NIKLAS
            if ((msgSequence - ack) < 32) {
                ackField |= (1 << (msgSequence - ack));
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
                    partialMessageLength = readFlag(firstMsg, IS_PARTIAL) ? readShort(firstMsg, 7) : 1;
                }

                Iterator<byte[]> iter = reliableMessageBuffer.iterator();
                int amount = 0;
                int totalSize = 0;

                byte[] lastContinuouslyReceived = null;
                while (amount < partialMessageLength && iter.hasNext()) {
                    byte[] bytes = iter.next();
                    if (bytes == null) {
                        receivedAll = true;
                        break;
                    }
                    lastContinuouslyReceived = bytes;
                    amount++;
                    totalSize += bytes.length;
                }
                //update ack and ackfield
                if (amount > 0) {
                    //the amount for the ack update
                    int ackAmount = readInt(lastContinuouslyReceived, 9) + 1 - ack;
                    ack += ackAmount;
                    if (ackAmount < 32) {
                        ackField >>>= ackAmount;
                    } else {
                        ackField = 0;
                    }

                    int index = 0;
                    if (ack - messageBufferOffset < reliableMessageBuffer.size()) {
                        ListIterator<byte[]> listIterator = reliableMessageBuffer.listIterator(ack - messageBufferOffset);
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
                        System.arraycopy(reliableMessageBuffer.getFirst(), 0, msg, 0, HEADER_SIZE);
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
                if (isServer()) {
                    short target = readShort(msg, 5);
                    short origin = readShort(msg, 3);
                    writeShort(msg, 5, origin);
                    writeShort(msg, 3, target);
                    sendTo(target, msg);
                } else {
                    Connection indirectConnection = connections.get(readShort(msg, 5));
                    if (indirectConnection != null) {
                        indirectConnection.receive(msg);
                    } else if (readFlag(msg, IS_OPEN_CONNECTION)) {
                        System.out.println("CANNOT FIND CORRECT CONNECTION FOR MESSAGE");
                        Connection newIndirectConnection = new Connection(null, readShort(msg, 5));
                        System.out.println("created connection with remoteID " + newIndirectConnection.remoteID);
                        newIndirectConnection.startAndAcknowledge(true);
                    } else {
                        System.out.println("CANNOT HANDLE INDIRECT MESSAGE");
                    }
                }
            } else {
                //send it if necessary to the correct connection
                boolean isSystem = readFlag(msg, IS_SYSTEM);
                handleMessage(msg, isSystem);
            }
        }

        /**
         * adds a message to receivedTransmissions or handles it internal
         */
        void handleMessage(byte[] msg, boolean isSystem) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(msg);
            long skipped = inputStream.skip(HEADER_SIZE);
            inputStream.mark(0);
            if (skipped != HEADER_SIZE) if (doDebug) System.out.println("error: could not skip enough: " + skipped);
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
            if (doDebug) System.out.println("TIMEOUT WARNING");
            timeoutCounter++;
            if (timeoutCounter > TIMEOUT_RETRIES) {
                if (isIndirect) {
                    System.out.println("too many timeouts even if indirect, shutdown: " + remoteID);
                    removeConnection(remoteID);
                } else {
                    //set to indirect as a last try
                    timeoutCounter = 0;
                    if (!isConnected && initialMsg != null && !isServer && remoteID != 0) {
                        System.out.println("unconnected timeout, send initialMsg");
                        //make indirect because this is necessary for the send to function correctly
                        isIndirect = true;
                        sendInitialMessage(initialMsg);
                    }
                    activateIndirect(true);
                }
            }
            if (isConnected) {
                byte[] timeoutMsg = getSystemMsg(TIMEOUT_WARNING, 0);
                setFlag(timeoutMsg, IS_RELIABLE, false);
                send(timeoutMsg);
            }
            resend();
        }

        private synchronized void activateIndirect(boolean sendToPeer) {
            System.out.println("activate indirect: (" + clientID + "->" + remoteID + ")");
            if (remoteID == 0 || isServer()) {
                System.out.println("cannot make indirect: connection to server");
                removeConnection(remoteID);
            } else {
                isIndirect = true;
                if (sendToPeer) {
                    byte[] indirectMsg = getSystemMsg(SET_INDIRECT, 0);
                    send(indirectMsg);
                }
                //send all the unsent (direct) messages
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
                    if (doDebug) System.out.println("cannot write sentMessagesBuffer");
                }
            }
        }

        /**
         * resend the messages that are not acknowledged
         */
        private synchronized void resend() {
            if (!isIndirect) {
                if (isConnected) {
                    if (System.currentTimeMillis() - lastResendTimestamp > RESEND_TIMEOUT) {
                        int index = 0;
                        Iterator<byte[]> iter = sentMessagesBuffer.iterator();
                        while (index < 32 && iter.hasNext()) {
                            byte[] msg = iter.next();
                            if ((sequenceAckField & (1 << index)) == 0) {
                                sendMsgInternal(msg, socketAdress);
                            }
                            index++;
                        }
                        lastResendTimestamp = System.currentTimeMillis();
                    }
                } else if (initialMsg != null) {
                    sendMsgInternal(initialMsg, socketAdress);
                }
            } else {
                lastResendTimestamp = System.currentTimeMillis();
            }
        }

        @Override
        public void run() {
            super.run();
            while (!shutdown) {
                try {
                    byte[] msg = receiveQueue.poll(RECEIVE_TIMEOUT, TimeUnit.MILLISECONDS);
                    if (msg == null) {
                        timeout();
                    } else {
                        receiveInternal(msg);
                    }
                } catch (InterruptedException e) {
                    if (doDebug) System.out.println("interrupted while waiting for message");
                    e.printStackTrace();
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
                boolean isPublic = peerSet.remove(this.remoteID);

                this.remoteID = remoteID;

                connections.put(remoteID, this);
                if (isPublic) peerSet.add(remoteID);
            }
            //TODO notify other clients ??????
        }

        /**
         * makes this connection public, aka adds it to the connections set
         * does not notify other connections
         * TODO add some kind of synchronization
         */
        private void makePublic() {
            peerSet.add(remoteID);
        }

        private void connect() {
            isConnected = true;

            //send all unsent indirect messages
            for (byte[] unsentMsg : unsentIndirectMsgs) {
                send(unsentMsg);
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
                        System.out.println("received open message, ignore");
                        break;
                    case ACK_OPEN_CONNECTION:
                        short newClientID = inputStream.readShort();
                        connect();
                        if (newClientID != -1) {
                            if (newClientID == clientID) {
                                System.out.println("received ack_open_connection from other client");
                            } else {
                                if (clientID == -1) {
                                    //send request peers
                                    setClientID(newClientID);
                                    byte[] requestPeersMsg = getSystemMsg(REQUEST_PEERS, 0);
                                    send(requestPeersMsg);
                                } else {
                                    System.out.println("try to overwrite clientID");
                                }
                            }
                        }
                        byte[] ackMsg = getSystemMsg(ACK_ACK_OPEN_CONNECTION, 0);
                        send(ackMsg);
                        makePublic();
                        break;
                    case ACK_ACK_OPEN_CONNECTION:
                        //make it public
                        connect();
                        makePublic();
                        break;
                    case TIMEOUT_WARNING:
                        if (doDebug) System.out.println("TIMEOUT WARNING");
                        byte[] timeoutWarningAck = getSystemMsg(ACK_TIMEOUT_WARNING, 0);
                        setFlag(timeoutWarningAck, IS_RELIABLE, false);
                        send(timeoutWarningAck);
                        break;
                    case ACK_TIMEOUT_WARNING:
                        if (doDebug) System.out.println("Acked timout warning");
                        //do nothing, it fullfilled already its reason
                        break;
                    case SET_INDIRECT:
                        System.out.println("SET INDIRECT");
                        //this also sends the msg
                        activateIndirect(false);
                        break;
                    case REQUEST_PEERS:
                        System.err.println("resquest peers");
                        byte[] peerData = getPeerData(remoteID);
                        byte[] peerMsg = getSystemMsg(ADD_PEERS, peerData.length);
                        System.arraycopy(peerData, 0, peerMsg, HEADER_SIZE + 2, peerData.length);
                        send(peerMsg);
                        break;
                    case ADD_PEERS:
                        if (doDebug) System.out.println("add peers");
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
                        if (doDebug) System.out.println();
                        short amountMsgs = inputStream.readShort();
                        for (int i = 0; i < amountMsgs; i++) {
                            short msgLength = inputStream.readShort();
                            byte[] msg = inputStream.readNBytes(msgLength);
                            //receive this message the normal way
                            receiveInternal(msg);
                        }
                        break;
                    default:
                        System.out.println("unrecognized system msg: " + msgID);
                        throw new IllegalStateException();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
