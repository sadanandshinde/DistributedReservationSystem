package servers.RMIClientSrc;

import ResInterface.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RMISecurityManager;

import java.util.*;
import java.io.*;

    
public class clientLoopMultiple
{
    static String message = "blank";
    static ResourceManager rm = null;

    static final int MINLOADIR = 1;
    static final int MAXLOADIR = 10;

    static final int NUMTIMESEXECUTECOMMAND = 3;

    static final String GENERICFILENAME = "singleClient.txt";

    static final boolean SLEEPBETWEENTRANSACTIONS = true;
    static final boolean RANDOMWAIT = false;

    static final boolean accessAllRMs = false;

    static ArrayList<String> carCommands = new ArrayList<String>(Arrays.asList("newCar", "deleteCar", "queryCar", "queryCarPrice", "reserveCar"));
    static ArrayList<String> roomCommands = new ArrayList<String>(Arrays.asList("newRoom", "deleteRoom", "queryRoom", "queryRoomPrice", "reserveRoom"));
    static ArrayList<String> flightCommands = new ArrayList<String>(Arrays.asList("newFlight", "deleteFlight", "queryFlight", "queryFlightPrice", "reserveFlight"));
    static ArrayList<String> customerCommands = new ArrayList<String>(Arrays.asList("newCustomer", "deleteCustomer", "queryCustomer"));
    static String itineraryCommand = "itinerary";

    static ArrayList<String> genericCommands = new ArrayList<String>(Arrays.asList("new", "delete", "query", "queryPrice", "reserve"));
    
    static ArrayList<String> randomLocations = new ArrayList<String>(Arrays.asList("montreal", "toronto", "vancouver", "ottawa", "new york"));

    public static void main(String args[])
    {
        clientLoopMultiple obj = new clientLoopMultiple();
        
        int rmNumber = -1;
        int loadIR = -1;
        String command = "";
        String fileName = "";

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
            rmNumber = (int)(Math.random() * 6);
            System.out.print("Random RM number = " + rmNumber);
            System.out.println("rmNumber: 0 = All three RMs, 1 = CarRM, 2 = RoomRM, 3 = FlightRM, 4 = Middleware (customer)");
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
            command = args[4];
            System.out.println("Command is " + command);
            if(command == null || command.isEmpty()) {
                switch(rmNumber) {
                    case 0:
                        command = genericCommands.get((int)(Math.random() * genericCommands.size()));
                    case 1:
                        command = carCommands.get((int)(Math.random() * carCommands.size()));
                        break;
                    case 2:
                        command = roomCommands.get((int)(Math.random() * roomCommands.size()));
                        break;
                    case 3:
                        command = flightCommands.get((int)(Math.random() * flightCommands.size()));
                        break;
                    case 4:
                        command = customerCommands.get((int)(Math.random() * customerCommands.size()));
                        break;
                    default:
                        command = "default";
                        break;
                }
            }
            else if(!(carCommands.contains(command) || roomCommands.contains(command) 
                || flightCommands.contains(command) || customerCommands.contains(command)
                || genericCommands.contains(command) || command.equals("itinerary"))) {
                    System.out.println ("Usage: java client [rmihost [rmiport]] [rmNumber] [loadIR] [commandName] [fileName]");
                    System.out.println("Where rmNumber: 0 = All three RMs, 1 = CarRM, 2 = RoomRM, 3 = FlightRM, 4 = Middleware (customer)");
                    System.exit(1);
            }
        }
        else{
            switch(rmNumber) {
                case 0:
                    command = genericCommands.get((int)(Math.random() * genericCommands.size()));
                case 1:
                    command = carCommands.get((int)(Math.random() * carCommands.size()));
                    break;
                case 2:
                    command = roomCommands.get((int)(Math.random() * roomCommands.size()));
                    break;
                case 3:
                    command = flightCommands.get((int)(Math.random() * flightCommands.size()));
                    break;
                case 4:
                    command = customerCommands.get((int)(Math.random() * customerCommands.size()));
                    break;
                default:
                    command = "default";
                    break;
            }
        }
        if(args.length > 5) {
            fileName = args[5];
        }
        if (args.length > 6)
        {
            System.out.println ("Usage: java client [rmihost] [rmiport] [rmNumber] [loadIR] [commandName] [fileName]");
            System.out.println("Where rmNumber: 0 = All three RMs, 1 = CarRM, 2 = RoomRM, 3 = FlightRM, 4 = Middleware (customer)");
            System.exit(1);
        }
        
        try {
            // get a reference to the rmiregistry
            Registry registry = LocateRegistry.getRegistry(server, port);
            // get the proxy and the remote reference by rmiregistry lookup
            rm = (ResourceManager) registry.lookup("PG7MiddlewareServer");
            
            if(rm!=null) {
                System.out.println("Successful");
                System.out.println("Connected to RM");
            }
            else {
                System.out.println("Unsuccessful");
            }
            // make call on remote method
        } 
        catch (Exception e) {    
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
        
        if (System.getSecurityManager() == null) {
            //System.setSecurityManager(new RMISecurityManager());
        }

        BufferedWriter writer = null;
        try{
            writer = new BufferedWriter(new FileWriter(fileName));
        } catch(IOException e) {
            System.out.println(e);
        }

        int transactionID = -1;
        int customerID = -1;
        long startTime, endTime, runningTime = 0;

        int numIterations = 1;
        try{
            transactionID= rm.start();
            System.out.println("Started a transaction with ID =  " + transactionID);
    
            customerID = rm.newCustomer(transactionID);
            System.out.println("Created customer with ID = " + customerID);

            startTime = System.currentTimeMillis();
            obj.fillRMsWithItems(transactionID);
            rm.commit(transactionID);
            endTime = System.currentTimeMillis();
    
            runningTime = endTime - startTime;
            System.out.println("Seeding RMs took " + runningTime + " ms");

            // write to file
            String lineToWrite = String.format("%d, %d\n", numIterations, runningTime);
            System.out.print(lineToWrite);
            writer.write(lineToWrite);
            numIterations++;
        }
        catch(Exception e) {
            System.out.println(e);
        }

        while(numIterations < 500){
            try{
                String randlocation = obj.getRandomFromList(obj.randomLocations);
                System.out.println("Random location selected = " + randlocation);
                int locationFlightNum = obj.getLetterNumberTotal(randlocation);

                int randomQuantityNum = (int)(Math.random() * 100000);
                int randomPrice = (int)(Math.random() * 1000);

                System.out.println("starting a transaction");
                transactionID= rm.start();

                System.out.println("Started a transaction with ID =  " + transactionID);

                startTime = System.currentTimeMillis();
                
                if(rmNumber == 0) {
                    for(int i = 0; i < NUMTIMESEXECUTECOMMAND / 3; i++) {
                        switch(command.toLowerCase()) {
                            case "new":
                                rm.addFlight(transactionID, locationFlightNum, randomQuantityNum, randomPrice);
                                rm.addCars(transactionID, randlocation, randomQuantityNum, randomPrice);
                                rm.addRooms(transactionID, randlocation, randomQuantityNum, randomPrice);
                                break;
                            case "delete":
                                rm.deleteFlight(transactionID, locationFlightNum);
                                rm.deleteCars(transactionID, randlocation);
                                rm.deleteRooms(transactionID, randlocation);
                                break;
                            case "query":
                                rm.queryFlight(transactionID, locationFlightNum);
                                rm.queryCars(transactionID, randlocation);
                                rm.queryRooms(transactionID, randlocation);
                                break;
                            case "queryPrice":
                                rm.queryFlightPrice(transactionID, locationFlightNum);
                                rm.queryCarsPrice(transactionID, randlocation);
                                rm.queryRoomsPrice(transactionID, randlocation);
                                break;
                            case "reserve":
                                rm.reserveFlight(transactionID, customerID, locationFlightNum);
                                rm.reserveCar(transactionID, customerID, randlocation);
                                rm.reserveRoom(transactionID, customerID, randlocation);
                                break;
                            default:
                                break;
                        }
                    }
                }
                else{
                    for(int i = 0; i < NUMTIMESEXECUTECOMMAND; i++) {
                        switch(command.toLowerCase()) {
                            case "newflight":
                                rm.addFlight(transactionID, locationFlightNum, randomQuantityNum, randomPrice);
                                break;
        
                            case "deleteflight":
                                rm.deleteFlight(transactionID, locationFlightNum);
                                //rm.deleteFlight(transactionID, locationFlightNum);
                                //rm.deleteFlight(transactionID, locationFlightNum);
                                break;
        
                            case "queryflight":
                                rm.queryFlight(transactionID, locationFlightNum);
                                break;
        
                            case "queryflightprice":
                                rm.queryFlightPrice(transactionID, locationFlightNum);
                                break;
        
                            case "reserveflight":
                                rm.reserveFlight(transactionID, customerID, locationFlightNum);
                                break;
        
                            case "newcar":
                                rm.addCars(transactionID, randlocation, randomQuantityNum, randomPrice);
                                break;
        
                            case "deletecar":
                                rm.deleteCars(transactionID, randlocation);
                                //rm.deleteCars(transactionID, randlocation);
                                //rm.deleteCars(transactionID, randlocation);
                                break;
        
                            case "querycar":
                                rm.queryCars(transactionID, randlocation);
                                break;
        
                            case "querycarprice":
                                rm.queryCarsPrice(transactionID, randlocation);
                                break;
        
                            case "reservecar":
                                rm.reserveCar(transactionID, customerID, randlocation);
                                break;
        
                            case "newroom":
                                rm.addRooms(transactionID, randlocation, randomQuantityNum, randomPrice);
                                break;
        
                            case "deleteroom":
                                rm.deleteRooms(transactionID, randlocation);
                                //rm.deleteRooms(transactionID, randlocation);
                                //rm.deleteRooms(transactionID, randlocation);
                                break;
        
                            case "queryroom":
                                rm.queryRooms(transactionID, randlocation);
                                break;
        
                            case "queryroomprice":
                                rm.queryRoomsPrice(transactionID, randlocation);
                                break;
        
                            case "reserveroom":
                                rm.reserveRoom(transactionID, customerID, randlocation);
                                break;
        
                            case "newcustomer":
                                rm.newCustomer(transactionID);
                                break;
        
                            case "deletecustomer":
                                rm.deleteCustomer(transactionID, customerID);
                                //rm.deleteCustomer(transactionID, customerID);
                                //rm.deleteCustomer(transactionID, customerID);
                                break;
        
                            case "querycustomer":
                                rm.queryCustomerInfo(transactionID, customerID);
                                break;
        
                            case "itinerary":
                                rm.itinerary(transactionID, customerID, new Vector<Integer>(Arrays.asList(locationFlightNum)), randlocation, true, true);
                                break;
        
                            default:
                                System.out.println("Unknown command. Try again...");
                                break;
                        }
                    }
                }

                rm.commit(transactionID);
                endTime = System.currentTimeMillis();
                
                runningTime = endTime - startTime;
                System.out.println("Executing the command " + command + " took " + runningTime + " ms");

                // write to file
                String lineToWrite = String.format("%d, %d\n", numIterations, runningTime);
                System.out.print(lineToWrite);
                writer.write(lineToWrite);
                numIterations++;

                long waitTime = 1000 / loadIR;
                long sleepTime = waitTime - runningTime;

                if(SLEEPBETWEENTRANSACTIONS) {
                    if(sleepTime > 0) {
                        long variationTime = obj.randomRange((int) (-1 * sleepTime / 20), (int) (1 * sleepTime / 20));
    
                        if(RANDOMWAIT && sleepTime - variationTime > 0) {
                            Thread.sleep(sleepTime - variationTime);
                        }
                        else{
                            Thread.sleep(sleepTime);
                        }
                    }
                }
            }
            catch (Exception e) {
                System.out.println(e);
            }
        }
        
        try{
            writer.close();
        } catch(IOException e) {
            System.out.println(e);
        }
        //end of while(true)
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

    private int randomRange(int min, int max) {
        return min + (int)(Math.random() * ((max - min) + 1));
    }
}