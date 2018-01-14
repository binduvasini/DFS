package edu.usfca.cs.dfs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Controller extends Thread {
    static HashMap<String,Long> activeStorageNodesWithTimestamp = new HashMap<>(); //active nodes & current timestamp
    static HashMap<String,Integer> activeStorageNodesWithSpace = new HashMap<>(); //active nodes & available space
    static HashMap<String, List<String>> activeStorageNodeWithFiles = new HashMap<>(); //active nodes & list of files
    static Map<String,Integer> fileChunks = new HashMap<>();  //chunks & their sizes
    static Map<String,String> chunkStorage = new HashMap<>();  //each chunk & its replica location
    static Map<String,List<String>> chunkStorageList = new HashMap<>(); //each chunk & its replica location as a list
    static Map<String,List<String>> fileAndChunknames = new HashMap<>(); //each file and its chunk names
    String retrieveFile = "";
    String storageClientHost = "";
    String retrieveClientHost = "";
    String listFileClientHost = "";
    public static void main(String[] args) {

        new Thread(new Runnable() {
            public void run() {
                Socket socket = null;
                try (ServerSocket controllerSocket = new ServerSocket(12003);) //receive heartbeats from Storage Node
                {
                    System.out.println("Controller Listening for heartbeat");

                    while (true) {

                        socket = controllerSocket.accept();
                        StorageMessages.StorageMessageWrapper msgWrapper
                                = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                                socket.getInputStream());
                        if (msgWrapper.hasSendHeartBeatMsg()) {
                            StorageMessages.SendHeartBeat sendHeartBeat
                                    = msgWrapper.getSendHeartBeatMsg();
                            activeStorageNodesWithTimestamp.put(sendHeartBeat.getNodename(), sendHeartBeat.getTimestamp());
                            activeStorageNodesWithSpace.put(sendHeartBeat.getNodename(), sendHeartBeat.getAvailableSpace());
                            activeStorageNodeWithFiles.put(sendHeartBeat.getNodename(), sendHeartBeat.getFilesList());
                            System.out.println("heart beat from " + sendHeartBeat.getNodename());
                        }
                        System.out.println("No. of active storage nodes : "+ activeStorageNodesWithTimestamp.size());
                    }

                } catch (IOException ioe) {
                    ioe.printStackTrace();
                } finally {
                    if (!socket.isClosed()) {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }).start();

        new Timer().schedule(new TimerTask() {  //check heartbeat failure
            @Override
            public void run() {
                Iterator<String> actSNWithTSIterator = activeStorageNodesWithTimestamp.keySet().iterator();
                String inactiveNode = "";
                while (actSNWithTSIterator.hasNext()) {
                    String eachNode = actSNWithTSIterator.next();
                    Long timestamp = activeStorageNodesWithTimestamp.get(eachNode);
                    Long currentTime = System.currentTimeMillis();
                    System.out.println(eachNode + " time : " + timestamp + " :: current time " + currentTime);
                    if (Math.abs(timestamp - currentTime) > 30000) {
                        inactiveNode = eachNode;
                        actSNWithTSIterator.remove();
                        System.out.println("removed " + eachNode + " from the active nodes list and other 2 hashmaps");
                    }
                }

                //fault taulerance : re-replication
                if (!inactiveNode.isEmpty()){
                    List<String> files = activeStorageNodeWithFiles.get(inactiveNode);
                    Iterator<String> nowactSNWithTSIterator = activeStorageNodesWithTimestamp.keySet().iterator();

                    for (String file :
                            files) {
                        String filename = "/home2/bbalasubramanian/"+file;
                        System.out.println("re-replicating file "+filename);
                        List<String> replicaLocations = chunkStorageList.get(filename);

                        /*System.out.println("printing keyset of chunkstoragelist ");
                        for (String k:
                             chunkStorageList.keySet()) {
                            System.out.println(k + " :: "+chunkStorageList.get(k));

                        }*/
                        System.out.println(chunkStorageList.size()+" "+chunkStorageList.get(filename));
                        System.out.println(chunkStorage.size()+ "  "+chunkStorage.get(filename));
                        String replicaLocation = "";
                        for(int i=0;i<replicaLocations.size();i++){
                            if(!replicaLocations.get(i).contains(inactiveNode)){
                                replicaLocation=replicaLocations.get(i);
                                break;
                            }
                        }
                        System.out.println("replica locations for " + filename + " are " + replicaLocations.size()+"... Take the replica from "+replicaLocation);
                        while(nowactSNWithTSIterator.hasNext()){
                            String actNode = nowactSNWithTSIterator.next();

                            if(!replicaLocations.contains(actNode)){
                                //OutputStream output = new BufferedOutputStream(new FileOutputStream(file));
                                try (Socket sock = new Socket(replicaLocation, 12010);) {

                                    StorageMessages.ReReplication reReplication
                                            = StorageMessages.ReReplication.newBuilder().setNode(actNode).setFilename(file)
                                            .build();
                                    StorageMessages.StorageMessageWrapper msgWrapper =
                                            StorageMessages.StorageMessageWrapper.newBuilder().setRereplication(reReplication)
                                                    .build();

                                    msgWrapper.writeDelimitedTo(sock.getOutputStream());

                                } catch (UnknownHostException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                    activeStorageNodeWithFiles.remove(inactiveNode);
                    activeStorageNodesWithSpace.remove(inactiveNode);

                }

            }
        },30*1000,30*1000);

        //listenToClientForStorageRequest
        new Thread(new Runnable() {
            public void run() {
                new Controller().listenToClientForStorageRequest();
            }
        }).start();

        //listenToClientForRetrieval
        new Thread(new Runnable() {
            public void run() {
                new Controller().listenToClientForRetrieval();
            }
        }).start();

        //listenToClientForListFile
        new Thread(new Runnable() {
            public void run() {
                new Controller().listenToClientForListFile();
            }
        }).start();

    }

    public void listenToClientForStorageRequest(){
        Socket socket=null;

        try (ServerSocket controllerSocket = new ServerSocket(12000);) //Client asking for list of destination nodes
        {
            System.out.println("Controller Listening to client for storage request");
            while(true){
                socket = controllerSocket.accept();
                StorageMessages.StorageMessageWrapper msgWrapper
                        = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());
                System.out.println(msgWrapper);
                if (msgWrapper.hasStorageRequest()) {
                    StorageMessages.StorageRequest storageRequest
                            = msgWrapper.getStorageRequest();

                    fileChunks = storageRequest.getChunkSizesMap();
                    List<String> fileChunkArray = new ArrayList<>();

                    for (String key:
                            fileChunks.keySet()) {
                        System.out.println(key +" :::::: "+fileChunks.get(key));
                        fileChunkArray.add(key);
                    }
                    storageClientHost = storageRequest.getClientNode();
                    System.out.println("file chunks " + fileChunks.size() + "::" + storageRequest.getChunkSizesCount() + " Client " + storageClientHost + " :: file name " + storageRequest.getFilename());

                    fileAndChunknames.put("/home2/bbalasubramanian/"+storageRequest.getFilename(), fileChunkArray);
                    System.out.println("file and chunk names stored " + fileAndChunknames.size());

                    for (String key:
                            fileAndChunknames.keySet()) {
                        System.out.println(key +" ------- "+fileAndChunknames.get(key));
                    }

                    System.out.println("responding to client about storage request");
                    respondToClientForStorageRequest(socket);
                }
            }
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
        finally{
            if(!socket.isClosed()){
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void respondToClientForStorageRequest(Socket socket) {

        System.out.println("respond to client called for storage request");
        try {

            List<String> fileChunkObj = new ArrayList<>();
            for (String filechunk :
                    fileChunks.keySet()) {
                fileChunkObj.add(filechunk);
            }

            List<String> activeNodesArray = new ArrayList<>();
            for (String activenode : activeStorageNodesWithTimestamp.keySet()) {
                activeNodesArray.add(activenode);
            }

            List<String> testal1 = new ArrayList<>();
            List<String> testal2 = new ArrayList<>();
            List<String> testal3 = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                System.out.println("1 $$$$$$$ " + activeNodesArray.get(i));
                testal1.add(activeNodesArray.get(i));
            }
            Collections.shuffle(activeNodesArray);
            for (int i = 0; i < 3; i++) {
                System.out.println("2 $$$$$$$ " + activeNodesArray.get(i));
                testal2.add(activeNodesArray.get(i));
            }

            Collections.shuffle(activeNodesArray);
            for (int i = 0; i < 3; i++) {
                System.out.println("2 $$$$$$$ " + activeNodesArray.get(i));
                testal3.add(activeNodesArray.get(i));
            }
            String chunkFile = fileChunkObj.get(0);
            String checksumchunkFile = chunkFile.replace("/home2/bbalasubramanian/","/home2/bbalasubramanian/checksum_");
            chunkStorageList.put(chunkFile, testal1);
            chunkStorageList.put(checksumchunkFile, testal1);
            System.out.println("file chunk obje " + fileChunkObj.size());

            if (fileChunkObj.size() > 1) {
                String chunkFile1 = fileChunkObj.get(1);
                String checksumchunkFile1 = chunkFile.replace("/home2/bbalasubramanian/","/home2/bbalasubramanian/checksum_");
                String chunkFile2 = fileChunkObj.get(2);
                String checksumchunkFile2 = chunkFile.replace("/home2/bbalasubramanian/","/home2/bbalasubramanian/checksum_");

                chunkStorageList.put(chunkFile1, testal2);
                chunkStorageList.put(checksumchunkFile1, testal2);
                chunkStorageList.put(chunkFile2, testal3);
                chunkStorageList.put(checksumchunkFile2, testal3);
            }
            for (String key : chunkStorageList.keySet()
                    ) {
                StringBuffer sb = new StringBuffer();
                List<String> array = chunkStorageList.get(key);
                for (int i = 0; i < array.size(); i++) {
                    sb.append(array.get(i) + ";");
                }
                chunkStorage.put(key, sb.toString());
            }
            for (String s :
                    chunkStorage.keySet()) {
                System.out.println("--------------- " + s + " ------------- " + chunkStorage.get(s));
            }

            StorageMessages.StorageResponse storageResponse
                    = StorageMessages.StorageResponse.newBuilder().putAllChunkStorage(chunkStorage).build();
            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder().setStorageResponse(storageResponse)
                            .build();
            msgWrapper.writeDelimitedTo(socket.getOutputStream());
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void listenToClientForRetrieval(){
        Socket socket=null;

        try (ServerSocket controllerSocket = new ServerSocket(12005);) //Client asking for retrieval of files
        {
            System.out.println("Controller Listening to client for retrieval");
            while(true) {
                socket = controllerSocket.accept();
                StorageMessages.StorageMessageWrapper msgWrapper
                        = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());
                System.out.println(msgWrapper);
                if (msgWrapper.hasRetrieveFileRequest()) {
                    StorageMessages.RetrieveFileRequest retrieveFileRequest
                            = msgWrapper.getRetrieveFileRequest();
                    retrieveFile = retrieveFileRequest.getFileName();
                    retrieveClientHost = retrieveFileRequest.getClientNode();
                    System.out.println("retrieve file " + retrieveFile + " :: retrieve client " + retrieveClientHost);

                    System.out.println("responding to client about retrieval");
                    respondToClientForRetrieval(socket);
                }
            }
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
        finally{
            if(!socket.isClosed()){
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void respondToClientForRetrieval(Socket sock) {

        System.out.println("respond to client called for retrieval");
        try //to send the list of destinations to Client
        {
            System.out.println("fileAndChunkNames :  "+fileAndChunknames+ " :: "+fileAndChunknames.size() +":: retrieve file "+retrieveFile);
            if(!fileAndChunknames.isEmpty()) {
                List<String> activeNodesArray = new ArrayList<>();
                for (String activenode:
                        activeStorageNodesWithTimestamp.keySet()) {
                    activeNodesArray.add(activenode);
                }


                List<String> fileList = fileAndChunknames.get("/home2/bbalasubramanian/"+retrieveFile);
                System.out.println("file list " + fileList.size());
                Map<String, String> chunkMap = new HashMap<>();  //chunks and its location
                for (int i = 0; i < fileList.size(); i++) {

                    String[] fileLocation = chunkStorage.get(fileList.get(i)).split(";");
                    String activeFileLocations = "";
                    for(int j=0;j<fileLocation.length;j++) {  //to achieve fault tolerance
                        if (activeNodesArray.contains(fileLocation[j])){
                            activeFileLocations = fileLocation[j]+";";
                        }
                    }
                    chunkMap.put(fileList.get(i), activeFileLocations);
                }

                System.out.println("chunk map is loaded ");
                for (String s:
                        chunkMap.keySet()) {
                    System.out.println(s+" &&&&&&&&&& "+chunkMap.get(s));
                }
                StorageMessages.RetrieveFileResponse retrieveFileResponse
                        = StorageMessages.RetrieveFileResponse.newBuilder().putAllRetrieveChunk(chunkMap).build();
                StorageMessages.StorageMessageWrapper msgWrapper =
                        StorageMessages.StorageMessageWrapper.newBuilder().setRetriveFileResponse(retrieveFileResponse)
                                .build();
                msgWrapper.writeDelimitedTo(sock.getOutputStream());
            }
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    public void listenToClientForListFile(){
        Socket socket=null;

        try (ServerSocket controllerSocket = new ServerSocket(12009);) //Client asking for retrieval of files
        {
            System.out.println("Controller Listening to client for file list");
            while(true) {
                socket = controllerSocket.accept();
                StorageMessages.StorageMessageWrapper msgWrapper
                        = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());
                System.out.println(msgWrapper);
                if (msgWrapper.hasListFileRequest()) {
                    System.out.println("list file request received from client");
                    StorageMessages.ListFileRequest listFileRequest
                            = msgWrapper.getListFileRequest();
                    listFileClientHost = listFileRequest.getClientNode();
                    respondToClientForListFile(socket);
                }
            }
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
        finally{
            if(!socket.isClosed()){
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void respondToClientForListFile(Socket sock) {

        System.out.println("respond to client called for list file");
        try //to send the file list to Client
        {
            Map<String,String> storageNodesWithFiles = new HashMap<>();
            for (String storageNode:
                    activeStorageNodeWithFiles.keySet()) {
                List<String> fileList = activeStorageNodeWithFiles.get(storageNode);
                StringBuffer files = new StringBuffer();
                for(int i=0;i<fileList.size();i++){
                    files.append(fileList.get(i)+"; ");
                }
                storageNodesWithFiles.put(storageNode,files.toString());
            }

            StorageMessages.ListFileResponse listFileResponse
                    = StorageMessages.ListFileResponse.newBuilder().putAllStorageSpace(activeStorageNodesWithSpace).putAllFiles(storageNodesWithFiles).build();
            StorageMessages.StorageMessageWrapper msgWrapper =
                    StorageMessages.StorageMessageWrapper.newBuilder().setListFileResponse(listFileResponse)
                            .build();
            msgWrapper.writeDelimitedTo(sock.getOutputStream());
        }
        catch (IOException ioe){
            ioe.printStackTrace();
        }
    }
}
