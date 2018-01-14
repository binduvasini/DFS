package edu.usfca.cs.dfs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class FileRetrieval implements Runnable {
    public final String chunk;
    public final String node;
    public final String currentHost;

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String filename;

    public FileRetrieval(String chunk, String node, String currentHost) {
        this.chunk = chunk;
        this.node = node;
        this.currentHost = currentHost;
    }


    @Override
    public void run() {
        try (Socket storageSocket = new Socket(node, 12007);) { // get the chunk from storage node
            System.out.println("requesting node " + node + " for retrieveFile");
            StorageMessages.RetrieveFileRequestToSN retrieveFileRequestToSN
                    = StorageMessages.RetrieveFileRequestToSN.newBuilder()
                    .setFileName(chunk).setClientNode(currentHost)
                    .build();

            StorageMessages.StorageMessageWrapper msgWrapper2 =
                    StorageMessages.StorageMessageWrapper.newBuilder()
                            .setRetrieveFileRequestToSN(retrieveFileRequestToSN)
                            .build();
            msgWrapper2.writeDelimitedTo(storageSocket.getOutputStream());

            StorageMessages.StorageMessageWrapper msgWrapper3 = StorageMessages.StorageMessageWrapper
                    .parseDelimitedFrom(storageSocket.getInputStream());
            if (msgWrapper3!=null && msgWrapper3.hasRetrieveFileMsg()) {
                StorageMessages.RetrieveFile retrieveFile
                        = msgWrapper3.getRetrieveFileMsg();

                System.out.println("Received data from storage node : filename : " + retrieveFile.getFileName());
                String fileName = "/home4/bbalasubramanian/temporaryRetrieval/" + retrieveFile.getFileName();
                OutputStream output = new BufferedOutputStream(new FileOutputStream(fileName));

                output.write(retrieveFile.getData().toByteArray());
                output.close();
                setFilename(fileName);
            }
            else
                setFilename("checksumMismatch");
            storageSocket.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

}
