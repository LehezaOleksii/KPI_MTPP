package ua.kpi.oleksii.leheza;

import java.io.*;
import java.net.*;
import java.util.Random;

public class SocketMessagePassing {
    private static final int PORT = 9876;
    private static final int NUM_OPERATIONS = 1000;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java SocketMessagePassing [server|client]");
            return;
        }

        if (args[0].equals("server")) {
            runServer();
        } else {
            runClient();
        }
    }

    private static void runServer() throws Exception {
        System.out.println("=== SERVER (Socket Message Passing) ===");

        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Waiting for client...");

        Socket clientSocket = serverSocket.accept();
        System.out.println("Client connected");

        DataInputStream in = new DataInputStream(clientSocket.getInputStream());
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            int number = in.readInt();

            if (i % 100 == 0) {
                System.out.println("Received: " + number);
            }

            out.writeInt(number * 2);
            out.flush();
        }

        clientSocket.close();
        serverSocket.close();
    }

    private static void runClient() throws Exception {
        System.out.println("=== CLIENT (Socket Message Passing) ===");

        Socket socket = new Socket("localhost", PORT);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());

        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            int number = random.nextInt(10000);

            long start = System.nanoTime();

            out.writeInt(number);
            out.flush();

            int result = in.readInt();

            long end = System.nanoTime();
            totalTime += (end - start);

            if (i % 100 == 0) {
                System.out.println("Sent: " + number + ", Received: " + result);
            }
        }

        System.out.println("\nAverage time per operation: " + (totalTime / NUM_OPERATIONS / 1000) + " μs");

        socket.close();
    }
}
