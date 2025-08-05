package com.tcl.data_encryption;

import com.tcl.data_encryption.rpc.CryptoServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
public class CryptoServer {
    private static final int PORT = 9090;

    public static void main(String[] args) throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(PORT)
                .addService(new CryptoServiceImpl())
                .build()
                .start();

        System.out.println("Server started, listening on " + PORT);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down gRPC server");
            if (server != null) {
                server.shutdown();
            }
        }));

        server.awaitTermination();
    }
}
