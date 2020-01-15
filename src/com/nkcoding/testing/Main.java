package com.nkcoding.testing;

import com.nkcoding.communication.DatagramSocketCommunication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
	// write your code here
        Scanner in = new Scanner(System.in);
        System.out.println("start as a server? y/[n]");
        boolean startAsServer;
        String remoteIP = null;
        int remotePort = -1;
        if (in.nextLine().equals("y")) {
            System.out.println("start as a server");
            startAsServer = true;
        } else {
            System.out.println("start as client");
            startAsServer = false;
            System.out.println("enter remote ip");
            remoteIP = in.nextLine();
            System.out.println("enter remote port");
            remotePort = in.nextInt();
        }
        System.out.println("enter port");
        int port = in.nextInt();

        DatagramSocketCommunication communication = new DatagramSocketCommunication(startAsServer, port);
        if (!startAsServer) {
            System.out.println("connect to server...");
            communication.openCommunication(remoteIP, remotePort);
            System.out.println("connected to server");
        }
        String cmd = in.nextLine();

        System.out.println("enter exit to stop the program");
        while (!cmd.equals("exit")) {
            switch(cmd) {
                case "p":
                    System.out.println(communication.getTransmission());
                    break;
                case "s":
                    System.out.println("enter message");
                    //communication.sendToAll(new StringTransmission(1, in.nextLine()));
                    break;
                case "st":
                    System.out.println("enter peer id");
                    int id = in.nextInt();
                    in.nextLine();
                    System.out.println("enter message");
                    //communication.sendTo(id, new StringTransmission(1, in.nextLine()));
                    break;
                case "id":
                    System.out.println(communication.getId());
                    break;
                case "ip":
                    try {
                        InetAddress inetAddress = InetAddress.getLocalHost();
                        System.out.println("ip: " + inetAddress.getHostAddress());
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                    break;
                case "lst":
                    System.out.println(communication.getPeers());
                    break;
                case "test":
                    byte[] bytes = new byte[4];
                    DatagramSocketCommunication.writeInt(bytes, 0, 0x1234FAB1);
                    System.out.println(Arrays.toString(bytes));
                    System.out.printf("%#x%n", DatagramSocketCommunication.readInt(bytes, 0));
                    break;
                case "shutdown":
                    System.out.println("to:");
                    int to = in.nextInt();
                    in.nextLine();
                    communication.sendTo(0, new TransmissionTransmission(Transmission.REDIRECT_TRANSMISSION,
                            0, communication.getId(),
                            new IntTransmission(Transmission.REMOVE_CONNECTION, to)));
                    break;
                case "lock":
                    System.out.println(communication.stateCounter.get());
                    break;
                case "help":
                    System.out.println("p: print last recent message");
                    System.out.println("s: send message to all");
                    System.out.println("st: send message to specific peer");
                    System.out.println("id: get own id");
                    System.out.println("ip: get your own ip (not completely clear of which subnet)");
                    System.out.println("lst: list all available connections");
                    System.out.println("shutdown: close specific connection (debug reasons only)");
                    System.out.println("lock: find out if it is locked");
                    System.out.println("help: list all commands");
                    break;
                default:
                    System.out.println("unknown command");
                    break;
            }


            cmd = in.nextLine();
        }

    }
}
