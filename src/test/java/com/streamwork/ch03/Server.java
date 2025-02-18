package com.streamwork.ch03;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(9992)) {
            System.out.println("Server started on port 9992...");
            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Handle client connection here
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
