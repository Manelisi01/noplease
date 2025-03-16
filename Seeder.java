import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.*;

public class Seeder {
    private static final int CHUNK_SIZE = 512 * 1024; // 512 KB
    private static final int TRACKER_PORT = 5000;


    public static void main(String[] args) throws Exception {

        // Supply file name , tracker ip and the seeder ip
        
        String filePath = args[0];

        String TRACKER_IP = args[1];

        String SEEDER_IP = args[2];

        
        byte[][] chunks = splitFile(filePath);
        int seederPort = 6000;

        // Start TCP server
        ExecutorService executor = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(seederPort)) {
            // Send registration to Tracker (UDP)
            sendHeartbeat(filePath, TRACKER_IP, seederPort, SEEDER_IP);

            // Schedule periodic heartbeats
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            
            scheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    sendHeartbeat(filePath, TRACKER_IP ,seederPort, SEEDER_IP);
                }
            }, 0, 5, TimeUnit.SECONDS);

            // Handle incoming TCP connections
            while (true) {
                Socket clientSocket = serverSocket.accept();
               
                executor.submit(new Runnable() {
                    public void run() {
                        handleClient(clientSocket, chunks);
                    }
                });
            }
        }
    }

    private static void sendHeartbeat(String fileHash, String TRACKER_IP, int port, String SEEDER_IP) {
        try (DatagramSocket socket = new DatagramSocket()) {
            String message = "REGISTER|" + fileHash + "|"+  SEEDER_IP +"|" + port;
            byte[] data = message.getBytes();
            InetAddress address = InetAddress.getByName(TRACKER_IP);
            socket.send(new DatagramPacket(data, data.length, address, TRACKER_PORT));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static byte[][] splitFile(String filePath) throws IOException {
        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        int numChunks = (int) Math.ceil(fileBytes.length / (double) CHUNK_SIZE);
        byte[][] chunks = new byte[numChunks][];

        for (int i = 0; i < numChunks; i++) {
        int start = i * CHUNK_SIZE;
        int end = Math.min(start + CHUNK_SIZE, fileBytes.length);
        chunks[i] = Arrays.copyOfRange(fileBytes, start, end);
    }
        // Split into chunks...
        return chunks;
    }

    private static void handleClient(Socket socket, byte[][] chunks) {
        try (DataInputStream in = new DataInputStream(socket.getInputStream());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            int chunkNumber = in.readInt();

            if (chunkNumber == -1) {
                // Respond with total chunks
                out.writeInt(chunks.length);
            } else {
                // Send the requested chunk
                out.write(chunks[chunkNumber]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}