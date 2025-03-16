import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Leecher {

    private static final int TRACKER_PORT = 5000;

    public static void main(String[] args) throws Exception {

        // supply file name and the IP address of the tracker
        
        String TARGET_FILE = args[0];

        String TRACKER_IP = args[1];


        // Query Tracker for seeders
        List<String> seeders = queryTracker(TARGET_FILE, TRACKER_IP);
        
        if (seeders.isEmpty()) {
            System.err.println("No seeders available for this file!");
            return;
        }

        // Get total chunks from the first seeder
        String firstSeeder = seeders.get(0);
        String[] parts = firstSeeder.split(":");
        int totalChunks = getTotalChunks(parts[0], Integer.parseInt(parts[1]));

        // Download chunks in parallel
        ExecutorService executor = Executors.newFixedThreadPool(seeders.size());
        byte[][] chunks = new byte[totalChunks][];
        for (int i = 0; i < totalChunks; i++) {
            int chunkNumber = i;
            
            executor.submit(new Runnable() {
                public void run() {
                    String seeder = seeders.get(chunkNumber % seeders.size());
                    String[] parts = seeder.split(":");
                    String ip = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    chunks[chunkNumber] = downloadChunk(ip, port, chunkNumber);
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        // Assemble file
        try (FileOutputStream fos = new FileOutputStream(TARGET_FILE)) {
            for (byte[] chunk : chunks) {
                fos.write(chunk);
            }
        }
        System.out.println("File downloaded!");

        
        // Be a Seeder

        Seeder.main(new String[]{TARGET_FILE});

    }

    private static List<String> queryTracker(String fileHash, String TRACKER_IP) throws Exception {
        try (DatagramSocket socket = new DatagramSocket()) {
            String message = "QUERY|" + fileHash;
            byte[] data = message.getBytes();
            InetAddress address = InetAddress.getByName(TRACKER_IP);
            socket.send(new DatagramPacket(data, data.length, address, TRACKER_PORT));

            byte[] buffer = new byte[1024];
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);
            return Arrays.asList(new String(response.getData()).split(","));
        }
    }

    private static byte[] downloadChunk(String ip, int port, int chunkNumber) {
        try (Socket socket = new Socket(ip, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeInt(chunkNumber);
            return in.readAllBytes();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }


    private static int getTotalChunks(String seederIP, int seederPort) {
        try (Socket socket = new Socket(seederIP, seederPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {
            out.writeInt(-1); // Special request for total chunks
            return in.readInt();
        } catch (IOException e) {
            System.err.println("Failed to get total chunks from seeder");
            return 0;
        }
    }

}