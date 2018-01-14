package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/*
Create 2 threads : one to send heart beats to Controller and the other to receive file chunks from client
 */
public class StorageNode {
    public static void main(String[] args) {
        final String cluster = args[0]; // to know where the controller is running
        System.out.println("storage node");

        try {
            final String hostname = InetAddress.getLocalHost().getHostName();
            System.out.println("Starting storage node on " + hostname + "...");

            // Listen to Client to store the chunk
            new Thread(new Runnable() {
                public void run() {
                    try (ServerSocket srvSocket = new ServerSocket(12001);) // client sends the chunk for storage
                    {
                        System.out.println("Storage Node Listening for chunk storage");
                        while (true) {
                            Socket socket = srvSocket.accept();
                            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                    .parseDelimitedFrom(socket.getInputStream());

                            if (msgWrapper.hasStoreChunkMsg()) {
                                StorageMessages.StoreChunk storeChunkMsg = msgWrapper.getStoreChunkMsg();
                                String[] nodes = storeChunkMsg.getNodes().split(";");
                                String fileName = storeChunkMsg.getFileName();
                                String checksumFileName = fileName.replace("/home2/bbalasubramanian/","/home2/bbalasubramanian/checksum_");
                                OutputStream output = new BufferedOutputStream(new FileOutputStream(fileName));
                                OutputStream checksumoutput = new BufferedOutputStream(new FileOutputStream(checksumFileName));
                                //generate checksum
                                byte[] checksum = MessageDigest.getInstance("MD5").digest(storeChunkMsg.getData().toByteArray());
                                checksumoutput.write(checksum);
                                output.write(storeChunkMsg.getData().toByteArray());
                                System.out.println(fileName +" :: checksum is "+checksum+" :: writing is successful in " + hostname);
                                checksumoutput.close();
                                output.close();
                                // to replicate in other storage nodes
                                System.out.println("Storing file name: " + fileName + " ::Node: " + nodes[1]);
                                Socket replicateSocket1 = new Socket(nodes[1], 12004); // replicate the first chunk
                                StorageMessages.ReplicateChunk replicateChunk
                                        = StorageMessages.ReplicateChunk.newBuilder()
                                        .setFileName(fileName)
                                        .setChunkId(storeChunkMsg.getChunkId())
                                        .setData(storeChunkMsg.getData())
                                        .setNode(nodes[1])
                                        .build();

                                StorageMessages.StorageMessageWrapper msgWrapper1 =
                                        StorageMessages.StorageMessageWrapper.newBuilder()
                                                .setReplicateChunk(replicateChunk)
                                                .build();
                                msgWrapper1.writeDelimitedTo(replicateSocket1.getOutputStream());
                                replicateSocket1.close();


                                System.out.println("first replication done");
                                Socket replicateSocket2 = new Socket(nodes[2], 12004); // replicate the second chunk
                                StorageMessages.ReplicateChunk replicateChunk2
                                        = StorageMessages.ReplicateChunk.newBuilder()
                                        .setFileName(fileName)
                                        .setChunkId(storeChunkMsg.getChunkId())
                                        .setData(storeChunkMsg.getData())
                                        .setNode(nodes[2])
                                        .build();

                                StorageMessages.StorageMessageWrapper msgWrapper2 =
                                        StorageMessages.StorageMessageWrapper.newBuilder()
                                                .setReplicateChunk(replicateChunk2)
                                                .build();
                                msgWrapper2.writeDelimitedTo(replicateSocket2.getOutputStream());
                                replicateSocket2.close();
                                System.out.println("second replication done");
                            }
                        }
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            // send heart beats to controller
            new Timer().schedule(new TimerTask() { // sending heartbeats
                @Override
                public void run() {
                    try (Socket controllerSock = new Socket(cluster, 12003);) { // send heartbeat to controller
                        System.out.println("Sending heart beat thread");

                        File home2Dir = new File("/home2/bbalasubramanian");
                        File[] files = home2Dir.listFiles();
                        ArrayList<String> allFiles = new ArrayList<>(); // list of files this storage node contain
                        for (int i = 0; i < files.length; i++) {
                            if (files[i].isFile())
                                allFiles.add(files[i].toString().replace("/home2/bbalasubramanian/",""));
                        }

                        File storageLocation = new File("/home2/bbalasubramanian");
                        StorageMessages.SendHeartBeat heartBeat = StorageMessages.SendHeartBeat.newBuilder()
                                .setNodename(hostname).addAllFiles(allFiles)
                                .setTimestamp(System.currentTimeMillis())
                                .setAvailableSpace((int) (storageLocation.getFreeSpace()/1024/1024/1024))
                                .build();

                        StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                .newBuilder().setSendHeartBeatMsg(heartBeat).build();
                        try {
                            msgWrapper.writeDelimitedTo(controllerSock.getOutputStream());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }, 5 * 1000, 5 * 1000);

            // Listen to other storage nodes to store replica
            new Thread(new Runnable() {
                public void run() {
                    try (ServerSocket srvSocket = new ServerSocket(12004);) // receive replicas from other storage nodes
                    {
                        System.out.println("Storage Node Listening to other storage nodes for replication");
                        while (true) {
                            Socket socket = srvSocket.accept();
                            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                    .parseDelimitedFrom(socket.getInputStream());

                            if (msgWrapper.hasReplicateChunk()) {

                                StorageMessages.ReplicateChunk replicateChunk = msgWrapper.getReplicateChunk();
                                String node = replicateChunk.getNode();
                                String fileName = replicateChunk.getFileName();
                                System.out.println("Replicating " + fileName + " in Node: "+ node);
                                String checksumFileName = fileName.replace("/home2/bbalasubramanian/","/home2/bbalasubramanian/checksum_");
                                OutputStream output = new BufferedOutputStream(new FileOutputStream(fileName));
                                OutputStream checksumoutput = new BufferedOutputStream(new FileOutputStream(checksumFileName));
                                //generate checksum
                                byte[] checksum = MessageDigest.getInstance("MD5").digest(replicateChunk.getData().toByteArray());
                                checksumoutput.write(checksum);
                                output.write(replicateChunk.getData().toByteArray());
                                System.out.println(fileName +" :: checksum is "+checksum+" :: writing is successful in " + hostname);
                                checksumoutput.close();
                                output.close();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            // Listen to Client for file retrieval and send response
            new Thread(new Runnable() {
                public void run() {
                    try (ServerSocket srvSocket = new ServerSocket(12007);) // send replicas to client
                    {
                        System.out.println("Storage Node Listening to client to send replicas");
                        while (true) {
                            Socket socket = srvSocket.accept();
                            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                    .parseDelimitedFrom(socket.getInputStream());
                            System.out.println("-------"+hostname+" "+msgWrapper.hasRetrieveFileRequestToSN());
                            if (msgWrapper.hasRetrieveFileRequestToSN()) {
                                System.out.println("---- "+hostname+" ------- is requested for file retrieval");
                                StorageMessages.RetrieveFileRequestToSN retrieveFileRequestToSN = msgWrapper.getRetrieveFileRequestToSN();
                                String filename = retrieveFileRequestToSN.getFileName();
                                File toread = new File(filename);
                                byte[] data = new byte[(int) toread.length()];
                                InputStream inStream = new BufferedInputStream(new FileInputStream(toread));
                                inStream.read(data, 0, (int) toread.length());
                                inStream.close();

                                String trimmedFileName = filename.replace("/home2/bbalasubramanian/", "");
                                System.out.println(Files.readAllBytes(Paths.get("/home2/bbalasubramanian/checksum_"+trimmedFileName)).length);

                                //Socket retrieveFileSocket = new Socket(retrieveFileRequestToSN.getClientNode(), 12008); // send chunk data to client

                                String newchecksum = DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(data));
                                String oldchecksum = DatatypeConverter.printHexBinary(Files.readAllBytes(Paths.get("/home2/bbalasubramanian/checksum_"+trimmedFileName)));
                                if (oldchecksum.equalsIgnoreCase(newchecksum)) {
                                    System.out.println("Old and new checksum matches");
                                    StorageMessages.RetrieveFile retrieveFile
                                            = StorageMessages.RetrieveFile.newBuilder()
                                            .setFileName(trimmedFileName)
                                            .setChecksum(ByteString.copyFrom(Files.readAllBytes(Paths.get("/home2/bbalasubramanian/checksum_"+trimmedFileName))))
                                            .setData(ByteString.copyFrom(data))
                                            .build();

                                    StorageMessages.StorageMessageWrapper msgWrapper2 =
                                            StorageMessages.StorageMessageWrapper.newBuilder()
                                                    .setRetrieveFileMsg(retrieveFile)
                                                    .build();
                                    msgWrapper2.writeDelimitedTo(socket.getOutputStream());
                                    socket.close();
                                    System.out.println("SN responded to client ");
                                }
                                else {
                                    System.out.println("Old and new checksum do not match. The file has been modified!!");
                                    socket.close();
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }).start();


            // Listen to Client for re-replication
            new Thread(new Runnable() {
                public void run() {
                    try (ServerSocket srvSocket = new ServerSocket(12010);) // re-replicate the chunks in active node
                    {
                        System.out.println("Storage Node Listening to client for re-replication");
                        while (true) {
                            Socket socket = srvSocket.accept();
                            StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                    .parseDelimitedFrom(socket.getInputStream());
                            System.out.println("-------"+hostname+" "+msgWrapper.hasRereplication());
                            if (msgWrapper.hasRereplication()) {
                                System.out.println("---- "+hostname+" ------- is requested for re-replication");
                                StorageMessages.ReReplication reReplication = msgWrapper.getRereplication();
                                String replicateInNode = reReplication.getNode();
                                String replicateFile = "/home2/bbalasubramanian/"+reReplication.getFilename();
                                System.out.println("Rereplicating file name: " + replicateFile + " ::Node: " + replicateInNode);
                                Socket replicateSocket = new Socket(replicateInNode, 12004); // replicate the first chunk
                                StorageMessages.ReplicateChunk replicateChunk
                                        = StorageMessages.ReplicateChunk.newBuilder()
                                        .setFileName(replicateFile)
                                        .setChunkId(111111111)
                                        .setData(ByteString.copyFrom(Files.readAllBytes(Paths.get(replicateFile))))
                                        .setNode(replicateInNode)
                                        .build();

                                StorageMessages.StorageMessageWrapper msgWrapper1 =
                                        StorageMessages.StorageMessageWrapper.newBuilder()
                                                .setReplicateChunk(replicateChunk)
                                                .build();
                                msgWrapper1.writeDelimitedTo(replicateSocket.getOutputStream());
                                replicateSocket.close();
                                System.out.println("replication done in "+replicateInNode);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
