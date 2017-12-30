package servers;

import common.*;
import ResInterface.*;

public class ServerThread extends Thread {

    private ResourceManager resourceManager;
    private long timeOutMillis;

    public ServerThread(ResourceManager rm, long timeOutTime) {
        this.resourceManager = rm;
        this.timeOutMillis = timeOutTime;
    }

    public void run() {

        System.out.println("Started server thread...");
        while(true) {
            try{
                resourceManager.pingOthers();
            }
            catch(Exception e){
                System.out.println("Ping threw exception, retrying to connect ");
                try{
                    resourceManager.connectToServers();
                }
                catch(Exception eInner){
                }
            }
            
            try {
                resourceManager.checkForTimeOuts();
                Thread.sleep(timeOutMillis / 10);
            }
            catch (Exception e) {
            }
        }

    }

}