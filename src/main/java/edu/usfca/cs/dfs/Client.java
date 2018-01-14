package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Client {
    static final int CHUNK_SIZE = 2000000;
    static String option = "";
    static String controllerNode = "";
    static String file = "";
    static Map<String, String> chunkStorage = new HashMap<>();  //each chunk and its replica location
    static int randomChunkId = Math.abs(new Random().nextInt());

    public static void main(String[] args) {

        entryMethod();
    }

    private static void entryMethod() {
        System.out.println("Enter your option: ");
        Scanner scanner = new Scanner(System.in);
        controllerNode = scanner.next();
        option = scanner.next();
        if(option.equalsIgnoreCase("store") || option.equalsIgnoreCase("retrieve"))
            file = scanner.next();
        System.out.println("entered option is "+option +" :: file "+file);

        if (option.equalsIgnoreCase("store")) {

            try (Socket sock = new Socket(controllerNode, 12000);) { //ask controller for a list of destination nodes
                if (file != null) {
                    HashMap<String, Integer> fileChunks = chunkFile(file,false);
                    System.out.println("file chunks");
                    for (String k:
                            fileChunks.keySet()) {
                        System.out.println(fileChunks.get(k));
                    }
                    //ask controller where to store the chunks
                    StorageMessages.StorageRequest storageRequest
                            = StorageMessages.StorageRequest.newBuilder().setFilename(file).putAllChunkSizes(fileChunks)
                            .setClientNode(getHostname()).build();
                    StorageMessages.StorageMessageWrapper msgWrapper =
                            StorageMessages.StorageMessageWrapper.newBuilder().setStorageRequest(storageRequest)
                                    .build();

                    msgWrapper.writeDelimitedTo(sock.getOutputStream());


                    System.out.println("received response from controller");


                    StorageMessages.StorageMessageWrapper msgWrapper1
                            = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                            sock.getInputStream());
                    if (msgWrapper1.hasStorageResponse()) {
                        StorageMessages.StorageResponse storageResponse
                                = msgWrapper1.getStorageResponse();

                        chunkStorage = storageResponse.getChunkStorageMap();
                        File f = new File(file);
                        System.out.println("Received response from Controller for storage. file path " + f.getPath()+" :: chunkStorage "+chunkStorage.size());
                        chunkFile(file, true);

                    }
                    System.out.println("done");
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            entryMethod();
        } else if (option.equalsIgnoreCase("retrieve")) {
            try (Socket sock = new Socket(controllerNode, 12005);) { //request controller for file retrieval
                StorageMessages.RetrieveFileRequest retrieveFileRequest
                        = StorageMessages.RetrieveFileRequest.newBuilder().setFileName(file).setClientNode(getHostname()).build();
                StorageMessages.StorageMessageWrapper msgWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder().setRetrieveFileRequest(retrieveFileRequest)
                                .build();
                msgWrapper.writeDelimitedTo(sock.getOutputStream());



                StorageMessages.StorageMessageWrapper msgWrapper1
                        = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        sock.getInputStream());
                if (msgWrapper1.hasRetriveFileResponse()) {
                    StorageMessages.RetrieveFileResponse retrieveFileResponse
                            = msgWrapper1.getRetriveFileResponse();

                    System.out.println("Received response from Controller for retrieval. retrieval chunk count : " + retrieveFileResponse.getRetrieveChunkCount());

                    Map<String, String> chunkAndLocations = retrieveFileResponse.getRetrieveChunkMap(); //chunks and its location
                    ArrayList<String> nameList = new ArrayList();
                    System.out.println(chunkAndLocations.size());
                    for (String chunk :
                            chunkAndLocations.keySet()) {
                        System.out.println(chunk + "******" + chunkAndLocations.get(chunk));
                        String[] nodes = chunkAndLocations.get(chunk).split(";");
                        FileRetrieval fileRetrieval = new FileRetrieval(chunk, nodes[0], getHostname());
                        Thread worker = new Thread(fileRetrieval);
                        worker.start();
                        Thread.sleep(10000);
                        String retrievedFile = fileRetrieval.getFilename();
                        System.out.println("------ retrieved file is "+retrievedFile);
                        System.out.println(retrievedFile.isEmpty());
                        if(retrievedFile.equals("checksumMismatch")){
                            System.out.println("contacting "+nodes[1]+" for retrieving the correct file.");
                                /*FileRetrieval fileRetrieval1 = new FileRetrieval(chunk, nodes[1], getHostname());
                                Thread worker1 = new Thread(fileRetrieval1);
                                worker1.start();
                                Thread.sleep(10000);
                                retrievedFile = fileRetrieval1.getFilename();
                                if(retrievedFile == null){
                                    FileRetrieval fileRetrieval2 = new FileRetrieval(chunk, nodes[2], getHostname());
                                    Thread worker2 = new Thread(fileRetrieval2);
                                    worker2.start();
                                    Thread.sleep(10000);
                                    retrievedFile = fileRetrieval1.getFilename();
                                }*/
                        }
                        nameList.add(retrievedFile);
                    }
                    mergeParts(nameList,"/home4/bbalasubramanian/retrievedFile/"+file);
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            entryMethod();
        }
        else if (option.equalsIgnoreCase("fileList")) {
            try (Socket sock = new Socket(controllerNode, 12009);) { //request controller for listing files
                //ask controller for retrieving file
                StorageMessages.ListFileRequest listFileRequest
                        = StorageMessages.ListFileRequest.newBuilder().setClientNode(getHostname()).build();
                StorageMessages.StorageMessageWrapper msgWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder().setListFileRequest(listFileRequest)
                                .build();
                msgWrapper.writeDelimitedTo(sock.getOutputStream());

                StorageMessages.StorageMessageWrapper msgWrapper1
                        = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        sock.getInputStream());
                if (msgWrapper1.hasListFileResponse()) {
                    StorageMessages.ListFileResponse listFileResponse
                            = msgWrapper1.getListFileResponse();

                    System.out.println("Received list of files from controller : " + listFileResponse.getFilesCount());
                    Map<String,String> filesMap = listFileResponse.getFilesMap();
                    for (String chunk:
                            filesMap.keySet()) {
                        System.out.println(chunk+" --------- "+filesMap.get(chunk));
                    }
                    Map<String,Integer> storageMap = listFileResponse.getStorageSpaceMap();
                    for (String storage:
                            storageMap.keySet()) {
                        System.out.println(storage+"---- available space : "+storageMap.get(storage)+" GB");
                    }
                }
                System.out.println("done");
            } catch (IOException e) {
                e.printStackTrace();
            }
            entryMethod();
        }
        else {
            System.out.println("need an argument");
        }
    }

    public static HashMap<String, Integer> chunkFile(String fileName, boolean write) throws IOException {
        int chunkSize = CHUNK_SIZE;
        File willBeRead = new File(fileName);
        int FILE_SIZE = (int) willBeRead.length();
        HashMap<String, Integer> chunkSizes = new HashMap<>();

        int NUMBER_OF_CHUNKS = 1;
        byte[] data = null;

        try {
            int totalBytesRead = 0;

            try (InputStream inStream = new BufferedInputStream(new FileInputStream(willBeRead));) {

                while (totalBytesRead < FILE_SIZE) {
                    String chunkedFile = "";

                    chunkedFile=fileName.split("\\.")[0] + NUMBER_OF_CHUNKS + ".bin";
                    int bytesRemaining = FILE_SIZE - totalBytesRead;
                    if (bytesRemaining < chunkSize) // Remaining Data Part is Smaller Than chunkSize
                    // chunkSize is assigned to remain volume
                    {
                        chunkSize = bytesRemaining;

                    }
                    data = new byte[chunkSize]; // Temporary Byte Array
                    int bytesRead = inStream.read(data, 0, chunkSize);
                    System.out.println("bytes read " + bytesRead);
                    if (bytesRead > 0) // If bytes read is not empty
                    {
                        totalBytesRead += bytesRead;
                        NUMBER_OF_CHUNKS++;
                    }
                    String chunkName = "/home2/bbalasubramanian/" + chunkedFile;
                    chunkSizes.put(chunkName, bytesRead);
                    if (write) {
                        if (!chunkStorage.isEmpty()) {
                            System.out.println("storing " + chunkName + " in " + chunkStorage.get(chunkName));
                            String[] SN = chunkStorage.get(chunkName).split(";");
                            Socket storeSocket = new Socket(SN[0], 12001); //to store the file in destination

                            StorageMessages.StoreChunk storeChunkMsg
                                    = StorageMessages.StoreChunk.newBuilder()
                                    .setFileName(chunkName)
                                    .setChunkId(randomChunkId)
                                    .setData(ByteString.copyFrom(data))
                                    .setNodes(chunkStorage.get(chunkName))
                                    .build();

                            StorageMessages.StorageMessageWrapper msgWrapper =
                                    StorageMessages.StorageMessageWrapper.newBuilder()
                                            .setStoreChunkMsg(storeChunkMsg)
                                            .build();
                            msgWrapper.writeDelimitedTo(storeSocket.getOutputStream());

                            System.out.println("writing is successful");
                        }
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return chunkSizes;
    }

    static void write(byte[] DataByteArray, String DestinationFileName) {
        try {
            OutputStream output = null;
            try {
                output = new BufferedOutputStream(new FileOutputStream(DestinationFileName));
                output.write(DataByteArray);
                System.out.println("writing is successful");
            } finally {
                output.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void mergeParts(ArrayList<String> nameList, String destinationFileName) {
        System.out.println("merge parts called");
        File[] file = new File[nameList.size()];
        byte AllFilesContent[] = null;

        int TOTAL_SIZE = 0;
        int FILE_NUMBER = nameList.size();
        int FILE_LENGTH = 0;
        int CURRENT_LENGTH = 0;

        for (int i = 0; i < FILE_NUMBER; i++) {
            file[i] = new File(nameList.get(i));
            TOTAL_SIZE += file[i].length();
        }
        InputStream inStream = null;
        try {
            AllFilesContent = new byte[TOTAL_SIZE]; // Length of All Files, Total Size
            for (int j = 0; j < FILE_NUMBER; j++) {
                inStream = new BufferedInputStream(new FileInputStream(file[j]));
                FILE_LENGTH = (int) file[j].length();
                inStream.read(AllFilesContent, CURRENT_LENGTH, FILE_LENGTH);
                CURRENT_LENGTH += FILE_LENGTH;
                inStream.close();
            }

        } catch (FileNotFoundException e) {
            System.out.println("File not found " + e);
        } catch (IOException ioe) {
            System.out.println("Exception while reading the file " + ioe);
        } finally {
            write(AllFilesContent, destinationFileName);
            try {
                inStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Merge was executed successfully.!");
    }

    // Retrieves the short host name of the current host.
    private static String getHostname() {
        String hostname="";
        try {
            hostname= InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostname;
    }
}