import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Tracker {

    // Constants
    private static final int PORT = 5000; // Port on which the tracker listens
    private static final long PEER_TIMEOUT = 60000; // Timeout for peer cleanup (60 seconds)

    // Data structures
    private static ConcurrentHashMap<String, PeerInfo> peers = new ConcurrentHashMap<>(); // Stores composite key
                                                                                          // (ip:port) to PeerInfo
                                                                                          // mappings
    private static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1); // Scheduler for periodic
                                                                                             // tasks

    public static void main(String[] args) throws Exception {
        // Create a UDP socket to listen for incoming packets
        DatagramSocket socket = new DatagramSocket(PORT);
        System.out.println("Tracker started on port " + PORT);

        // Schedule a periodic task to clean up stale peers
        scheduler.scheduleAtFixedRate(
                new Runnable() {
                    public void run() {
                        Tracker.cleanupPeers();
                    }
                }, 0, 1, TimeUnit.SECONDS); // Run every second

        // Buffer to store incoming data
        byte[] buffer = new byte[102400000];

        // Main loop to process incoming packets
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet); // Wait for incoming packet

            // Extract the message from the packet
            String message = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Received message: " + message);

            // Split the message into parts
            String[] parts = message.split("\\|");
            String command = parts[0]; // First part is the command

            // Process the command
            switch (command) {
                case "REGISTER":
                    // Register a new seeder
                    String fileHash = parts[1]; // File hash
                    String ip = parts[2]; // Seeder IP
                    int port = Integer.parseInt(parts[3]); // Seeder port

                    // Create a composite key (ip:port)
                    String compositeKey = ip + ":" + port;

                    // Add the peer to the map
                    peers.put(compositeKey, new PeerInfo(ip, port, fileHash));
                    System.out.println("Seeder registered for file hash " + fileHash + ": " + ip + ":" + port);

                    break;

                case "QUERY":
                    // Handle a query for seeders of a specific file
                    String hash = parts[1]; // File hash to query

                    // Find all matching peers for the file hash
                    List<String> matchingPeers = new ArrayList<>();
                    for (Map.Entry<String, PeerInfo> entry : peers.entrySet()) {
                        if (entry.getValue().fileHash.equals(hash)) {
                            matchingPeers.add(entry.getValue().toString());
                        }
                    }

                    // Prepare the response
                    String response = String.join(",", matchingPeers);
                    byte[] respData = response.getBytes();

                    // Send the response back to the requester
                    socket.send(new DatagramPacket(respData, respData.length, packet.getAddress(), packet.getPort()));
                    System.out.println("Sent response to query for file hash " + hash + ": " + response);

                    break;

                case "UPDATE":
                    // Update the last seen timestamp for a seeder
                    String updateFileHash = parts[1]; // File hash
                    String updateIP = parts[2]; // Seeder IP
                    int updatePort = Integer.parseInt(parts[3]); // Seeder port

                    // Create a composite key (ip:port)
                    String updateCompositeKey = updateIP + ":" + updatePort;

                    // Update the last seen timestamp for this peer
                    PeerInfo peerInfo = peers.get(updateCompositeKey);
                    if (peerInfo != null) {
                        peerInfo.lastSeen = System.currentTimeMillis();
                        System.out.println("Updated last seen for seeder: " + updateIP + ":" + updatePort);
                    }

                    break;
            }
        }
    }

    /**
     * Cleans up stale peers that haven't been seen for more than PEER_TIMEOUT
     * milliseconds.
     */
    private static void cleanupPeers() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, PeerInfo>> iterator = peers.entrySet().iterator();

        // Iterate through all peers and remove stale ones
        while (iterator.hasNext()) {
            Map.Entry<String, PeerInfo> entry = iterator.next();
            if (now - entry.getValue().lastSeen > PEER_TIMEOUT) {
                iterator.remove(); // Remove the stale peer
                System.out.println("Removed stale peer: " + entry.getValue().ip + ":" + entry.getValue().port);
            }
        }
    }

    /**
     * Represents information about a peer (seeder).
     */
    static class PeerInfo {
        String ip; // IP address of the peer
        int port; // Port of the peer
        String fileHash; // File hash served by the peer
        long lastSeen; // Timestamp of the last update

        /**
         * Constructor for PeerInfo.
         *
         * @param ip       The IP address of the peer.
         * @param port     The port of the peer.
         * @param fileHash The file hash served by the peer.
         */
        PeerInfo(String ip, int port, String fileHash) {
            this.ip = ip;
            this.port = port;
            this.fileHash = fileHash;
            this.lastSeen = System.currentTimeMillis(); // Set the last seen timestamp to now
        }

        /**
         * Returns a string representation of the peer in the format "IP:Port".
         *
         * @return The string representation of the peer.
         */
        @Override
        public String toString() {
            return ip + ":" + port;
        }
    }
}