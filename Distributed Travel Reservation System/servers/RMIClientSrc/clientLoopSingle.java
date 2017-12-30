package servers.RMIClientSrc;

import ResInterface.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RMISecurityManager;

import java.util.*;
import java.io.*;

    
public class clientLoopSingle
{
    static String message = "blank";
    static ResourceManager rm = null;

    static final int MINLOADIR = 1;
    static final int MAXLOADIR = 10;

    static final boolean accessAllRMs = true;

    ArrayList<String> carCommands = new ArrayList<String>(Arrays.asList("newCar", "deleteCar", "queryCar", "queryCarPrice", "reserveCar"));
    ArrayList<String> roomCommands = new ArrayList<String>(Arrays.asList("newRoom", "deleteRoom", "queryRoom", "queryRoomPrice", "reserveRoom"));
    ArrayList<String> flightCommands = new ArrayList<String>(Arrays.asList("newFlight", "deleteFlight", "queryFlight", "queryFlightPrice", "reserveFlight"));
    ArrayList<String> customerCommands = new ArrayList<String>(Arrays.asList("newCustomer", "deleteCustomer", "queryCustomer"));
    String itineraryCommand = "itinerary";

    ArrayList<String> randomLocations = new ArrayList<String>(Arrays.asList("montreal", "toronto", "vancouver", "ottawa", "new york"));

    public static void main(String args[])
    {
        clientLoopSingle obj = new clientLoopSingle();
        
        int rmNumber = -1;
        int loadIR = -1;

        String server = "localhost";
        int port = 1099;
        if (args.length > 0)
        {
            server = args[0];
        }
        if (args.length > 1)
        {
            port = Integer.parseInt(args[1]);
        }
        if (args.length > 2) 
        {
            rmNumber = Integer.parseInt(args[2]);
        }
        else{
            rmNumber = (int)(Math.random() * 5);
            System.out.print("Random RM number = " + rmNumber);
            System.out.println("rmNumber: 1 = CarRM, 2 = RoomRM, 3 = FlightRM, 4 = Middleware (customer)");
        }
        if (args.length > 3) 
        {
            loadIR = Integer.parseInt(args[3]);
        }
        else{
            loadIR = MINLOADIR + (int)(Math.random() * ((MAXLOADIR - MINLOADIR) + 1));
            System.out.print("Selected Load (IR) = " + loadIR);
        }
        if (args.length > 4)
        {
            System.out.println ("Usage: java client [rmihost [rmiport]] [rmNumber] [loadIR]");
            System.out.println("Where rmNumber: 1 = CarRM, 2 = RoomRM, 3 = FlightRM, 4 = Middleware (customer)");
            System.exit(1);
        }
        
        try 
        {
            // get a reference to the rmiregistry
            Registry registry = LocateRegistry.getRegistry(server, port);
            // get the proxy and the remote reference by rmiregistry lookup
            rm = (ResourceManager) registry.lookup("PG7MiddlewareServer");
            if(rm!=null)
            {
                System.out.println("Successful");
                System.out.println("Connected to RM");
            }
            else
            {
                System.out.println("Unsuccessful");
            }
            // make call on remote method
        } 
        catch (Exception e) 
        {    
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
        
        if (System.getSecurityManager() == null) {
            //System.setSecurityManager(new RMISecurityManager());
        }

        int transactionID = -1;
        int customerID = 10000;
        long startTime, endTime, runningTime = 0;
        try{
            transactionID= rm.start();
            System.out.println("Started a transaction with ID =  " + transactionID);
    
            rm.newCustomer(transactionID, customerID);
            System.out.println("Created customer with ID = " + customerID);

            startTime = System.currentTimeMillis();
            obj.fillRMsWithItems(transactionID);
            rm.commit(transactionID);
            endTime = System.currentTimeMillis();
    
            runningTime = endTime - startTime;
            System.out.println("Seeding RMs took " + runningTime + " ms");
        }
        catch(Exception e) {
            System.out.println(e);
        }
	int counter=0;
        while(true){
            try{
				counter+=1;
                System.out.println("starting a transaction");
                transactionID= rm.start();
                System.out.println("Started a transaction with ID =  " + transactionID);
        
                //customerID=rm.newCustomer(transactionID);
                //System.out.println("Created customer with ID = " + customerID);    

                if(accessAllRMs) {
                    String randlocation = obj.getRandomFromList(obj.randomLocations);
                    System.out.println("Random location selected = " + randlocation);
                    int locationFlightNum = obj.getLetterNumberTotal(randlocation);

                    startTime = System.currentTimeMillis();
                    rm.reserveCar(transactionID, customerID, randlocation);
                    rm.reserveRoom(transactionID, customerID, randlocation);
                    rm.reserveFlight(transactionID, customerID, locationFlightNum);

                    rm.commit(transactionID);
                    endTime = System.currentTimeMillis();

                    runningTime = endTime - startTime;
                    System.out.println("\nACCESSING ALL 3 RMs took " + runningTime + " ms\n");
					Thread.sleep(10);
                }
                else{
                    String randlocation = obj.getRandomFromList(obj.randomLocations);
                    System.out.println("Random location selected = " + randlocation);
                    int locationFlightNum = obj.getLetterNumberTotal(randlocation);

                    switch(rmNumber) {
                        case 1:
                            startTime = System.currentTimeMillis();
                            rm.reserveCar(transactionID, customerID, randlocation);
                            //rm.reserveCar(transactionID, customerID, randlocation);
                            //rm.reserveCar(transactionID, customerID, randlocation);

                            rm.commit(transactionID);
                            endTime = System.currentTimeMillis();

                            runningTime = endTime - startTime;
                            System.out.println("\nACCESSING CAR RM 3 times took " + runningTime + " ms\n");
							
                            break;
                        case 2:
                            startTime = System.currentTimeMillis();
                            rm.reserveRoom(transactionID, customerID, randlocation);
                            rm.reserveRoom(transactionID, customerID, randlocation);
                            rm.reserveRoom(transactionID, customerID, randlocation);

                            rm.commit(transactionID);
                            endTime = System.currentTimeMillis();

                            runningTime = endTime - startTime;
                            System.out.println("\nACCESSING ROOM RM 3 times took " + runningTime + " ms\n");
                            break;
                        case 3:
                            startTime = System.currentTimeMillis();
                            rm.reserveFlight(transactionID, customerID, locationFlightNum);
                            rm.reserveFlight(transactionID, customerID, locationFlightNum);
                            rm.reserveFlight(transactionID, customerID, locationFlightNum);

                            rm.commit(transactionID);
                            endTime = System.currentTimeMillis();

                            runningTime = endTime - startTime;
                            System.out.println("\nACCESSING FLIGHT RM 3 times took " + runningTime + " ms\n");
                            break;
                        case 4: 
                            
                            break;
                        default:
                            break;
                    }
                }
            }
            catch (Exception e) {
                System.out.println(e);
            }
        }//end of while(true)
    }

    public int getInt(Object temp) throws Exception {
    try {
        return (new Integer((String)temp)).intValue();
        }
    catch(Exception e) {
        throw e;
        }
    }
    
    public boolean getBoolean(Object temp) throws Exception {
        try {
            return (new Boolean((String)temp)).booleanValue();
            }
        catch(Exception e) {
            throw e;
            }
    }

    public String getString(Object temp) throws Exception {
    try {    
        return (String)temp;
        }
    catch (Exception e) {
        throw e;
        }
    }

    private void fillRMsWithItems(int trxID) throws Exception {
        for(int i = 0; i < randomLocations.size(); i++) {
            String location = randomLocations.get(i);
            int randomQuantityNum = (int)(Math.random() * 100000);
            int randomPrice = (int)(Math.random() * 1000);
            rm.addCars(trxID, location, randomQuantityNum, randomPrice);
            System.out.println("Added " + randomQuantityNum + " cars at location: " + location + " and price = " + randomPrice);

            randomQuantityNum = (int)(Math.random() * 100000);
            randomPrice = (int)(Math.random() * 1000);

            rm.addRooms(trxID, location, randomQuantityNum, randomPrice);
            System.out.println("Added " + randomQuantityNum + " rooms at location: " + location + " and price = " + randomPrice);            

            randomQuantityNum = (int)(Math.random() * 100000);
            randomPrice = (int)(Math.random() * 1000);
            int flightNum = getLetterNumberTotal(location);

            rm.addFlight(trxID, flightNum, randomQuantityNum, randomPrice);
            System.out.println("Added " + randomQuantityNum + " seats of flight with number: " + flightNum + " and price of each = " + randomPrice);            
        }
    }

    private int getLetterNumberTotal(String s) {
        String sTrim = s.replace(" ", "");
        char[] charArray = sTrim.toCharArray();

        int total = 0;
    
        for (Character c : charArray){
            total += c - 'a' + 1;
        }
        return total;
    }

    private String getRandomFromList(ArrayList<String> list) {
        int size = list.size();
        int randomIndex = (int)(Math.random() * size);
        return list.get(randomIndex);
    }
}