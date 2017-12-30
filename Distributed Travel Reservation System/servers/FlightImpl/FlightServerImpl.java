// -------------------------------
// adapted from Kevin T. Manley
// CSE 593
//
package servers.FlightImpl;

import ResInterface.*;
import common.*;
import servers.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import java.io.*;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RMISecurityManager;

import TwoPhaseCommit.*;
import TwoPhaseCommit.Shadowing.*;
import object.ObjectParam;

public class FlightServerImpl extends ShadowPager implements ResourceManager 
{   
    protected RMHashtable m_itemHT = new RMHashtable();

    static    ResourceManager rm;
    static Registry registry;

    static ResourceManager rm_middleware;

    static int lastRetreivedValidXid = -1;

    static String server = "localhost";
    static int port = 1099;
    static int mwPort = 1099;
    static String mwServer = "";

    static boolean transactionStarted = false;
    static TransactionLogItem currentTransaction = null;
    static long timeoutCounter = 0;

    private static final Boolean ASKFORCRASH = false;

    public static void main(String args[]) {
        // Figure out where server is running
        
        if (args.length == 1) {
			mwServer=args[0];
            //server = server + ":" + args[0];
		} else if(args.length == 2){
			mwServer = args[0];
            port = Integer.parseInt(args[1]);
			mwPort = Integer.parseInt(args[1]);
        } else {
            System.err.println ("Wrong usage");
            System.out.println("Usage: java FlightImpl.FlightServerImpl [mwServer:host] [mwPort]");
            System.exit(1);
        }

        connectMWServer(mwServer,mwPort);

        try {
            // create a new Server object
            FlightServerImpl obj = new FlightServerImpl();
            DBCopyObject dbCopy = obj.initShadowpaging("FlightImpl");
            
            if(dbCopy != null) {
                lastRetreivedValidXid = dbCopy.currentTransactionID;

                if(dbCopy.itemsInDBCopy != null) {
                    obj.m_itemHT = dbCopy.itemsInDBCopy;

                    System.out.println("\nRecovered Items Hashtable:");
                    if(!dbCopy.itemsInDBCopy.isEmpty()) {
                        Set<String> recoveredHTKeys = dbCopy.itemsInDBCopy.keySet();
                        for(String key : recoveredHTKeys) {
                            Flight flight = (Flight) dbCopy.itemsInDBCopy.get(key);
                            System.out.println("Key: " + key + ", Value: " + flight.toString());
                        }
                    }
                }
            }

            obj.initTwoPhaseCommitRecovery();
            
            // dynamically generate the stub (client proxy)
            rm = (ResourceManager) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
             registry = LocateRegistry.getRegistry(port);
            registry.rebind("PG7FlightServer", rm);

            ServerThread serverThread = new ServerThread(obj, RESPONSETIMEOUT);
            serverThread.start();

            System.err.println(" PG7FlightServer Server ready");
        } catch (Exception e) {
            System.err.println("PG7FlightServer Server exception: " + e.toString());
            e.printStackTrace();
        }

        // Create and install a security manager
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new RMISecurityManager());
        }
    }

    protected static void connectMWServer(String mwServer, int mwPort){
		Trace.info("connecting mwServer.... ");
		try 
		{
			// get a reference to the rmiregistry
			Registry registry = LocateRegistry.getRegistry(mwServer, mwPort);
			// get the proxy and the remote reference by rmiregistry lookup
			rm_middleware = (ResourceManager) registry.lookup("PG7MiddlewareServer");
			if(rm_middleware!=null)
			{
				System.out.println("Successful");
                System.out.println("Connected to MW RM");
			}
			else
			{
				System.out.println("Unsuccessful to PG7MiddlewareServer");
			}
			// make call on remote method
		} 
		catch (Exception e) {
            System.out.println("Unsuccessful to PG7MiddlewareServer");
		}
    }
    
    @Override
    public void connectToServers() {
        connectMWServer(mwServer,mwPort);
    }

    @Override
    public boolean pingServer() throws RemoteException {
        return true;
    }

    @Override
    public boolean pingOthers() throws RemoteException {
        return rm_middleware.pingServer();
    }

    @Override
	public void checkForTimeOuts() throws RemoteException {
        if(transactionStarted) {
            System.out.println("Checking for timeouts...");
            long currentTime = System.currentTimeMillis();

            if(currentTime - timeoutCounter > RESPONSETIMEOUT) {
                System.out.println("PARTICIPANT TIMEOUT FOR TRANSACTION ID = " + currentTransaction.transactionID + ", ABORTING!");
                currentTransaction.setTransactionState(TransactionState.ABORTED);
                rewriteTransactionLogItem(currentTransaction);

                lastRetreivedValidXid = currentTransaction.transactionID;
                transactionStarted = false;
                currentTransaction = null;
            }
        }
	}
     
    public FlightServerImpl() throws RemoteException {
    }

    public ReservableItem getRMEntry(int id, String key) throws RemoteException {
        System.out.println("Reading rm entry with xid = " + id + " and key = " + key);
        
        TransactionLogItem initialTransactionLogItem = new TransactionLogItem(id);
        initialTransactionLogItem.setTransactionState(TransactionState.INITIAL);
        rewriteTransactionLogItem(initialTransactionLogItem);

        if(!transactionStarted) {
            transactionStarted = true;
            currentTransaction = new TransactionLogItem(id);
        }
        timeoutCounter = System.currentTimeMillis();

        return (ReservableItem) readData( id, Flight.getKey(Integer.valueOf(key)));
    }

    public void writeRMEntry(int id, String key, RMItem value) throws RemoteException {
        writeData(id, key, value);
    }

    public RMItem removeRMEntry(int id, String key) throws RemoteException {
        return removeData(id, key);
    }

    // Reads a data item
    private RMItem readData( int id, String key )
    {
        synchronized(m_itemHT) {
            return (RMItem) m_itemHT.get(key);
        }
    }

    // Writes a data item
    private void writeData( int id, String key, RMItem value )
    {
        synchronized(m_itemHT) {
            m_itemHT.put(key, value);
        }
    }
    
    // Remove the item out of storage
    protected RMItem removeData(int id, String key) {
        synchronized(m_itemHT) {
            return (RMItem)m_itemHT.remove(key);
        }
    }
    
    
    // deletes the entire item
    protected boolean deleteItem(int id, String key)
    {
        Trace.info("RM::deleteItem(" + id + ", " + key + ") called" );
        ReservableItem curObj = (ReservableItem) readData( id, key );
        // Check if there is such an item in the storage
        if ( curObj == null ) {
            Trace.warn("RM::deleteItem(" + id + ", " + key + ") failed--item doesn't exist" );
            return false;
        } else {
            if (curObj.getReserved()==0) {
                removeData(id, curObj.getKey());
                Trace.info("RM::deleteItem(" + id + ", " + key + ") item deleted" );
                return true;
            }
            else {
                Trace.info("RM::deleteItem(" + id + ", " + key + ") item can't be deleted because some customers reserved it" );
                return false;
            }
        } // if
    }
    
    // checks if the entire item can be deleted
    protected boolean canDeleteItem(int id, String key)
    {
        ReservableItem curObj = (ReservableItem) readData( id, key );
        // Check if there is such an item in the storage
        if ( curObj == null ) {
            return false;
        } else {
            if (curObj.getReserved()==0) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    // query the number of available seats/rooms/cars
    protected int queryNum(int id, String key) {
        Trace.info("RM::queryNum(" + id + ", " + key + ") called" );
        ReservableItem curObj = (ReservableItem) readData( id, key);
        int value = 0;  
        if ( curObj != null ) {
            value = curObj.getCount();
        } // else
        Trace.info("RM::queryNum(" + id + ", " + key + ") returns count=" + value);
        return value;
    }    
    
    // query the price of an item
    protected int queryPrice(int id, String key) {
        Trace.info("RM::queryCarsPrice(" + id + ", " + key + ") called" );
        ReservableItem curObj = (ReservableItem) readData( id, key);
        int value = 0; 
        if ( curObj != null ) {
            value = curObj.getPrice();
        } // else
        Trace.info("RM::queryCarsPrice(" + id + ", " + key + ") returns cost=$" + value );
        return value;        
    }
    
    // reserve an item
    protected boolean reserveItem(int id, int customerID, String key, String location) {
        Trace.info("RM::reserveItem( " + id + ", customer=" + customerID + ", " +key+ ", "+location+" ) called" );        
        // Read customer object if it exists (and read lock it)
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );        
        if ( cust == null ) {
            Trace.warn("RM::reserveCar( " + id + ", " + customerID + ", " + key + ", "+location+")  failed--customer doesn't exist" );
            return false;
        } 
        
        // check if the item is available
        ReservableItem item = (ReservableItem)readData(id, key);
        if ( item == null ) {
            Trace.warn("RM::reserveItem( " + id + ", " + customerID + ", " + key+", " +location+") failed--item doesn't exist" );
            return false;
        } else if (item.getCount()==0) {
            Trace.warn("RM::reserveItem( " + id + ", " + customerID + ", " + key+", " + location+") failed--No more items" );
            return false;
        } else {            
            cust.reserve( key, location, item.getPrice());        
            writeData( id, cust.getKey(), cust );
            
            // decrease the number of available items in the storage
            item.setCount(item.getCount() - 1);
            item.setReserved(item.getReserved()+1);
            
            Trace.info("RM::reserveItem( " + id + ", " + customerID + ", " + key + ", " +location+") succeeded" );
            return true;
        }        
    }
    
    // check if you can reserve an item
    protected boolean canReserveItem(int id, int customerID, String key, String location) {
        // Read customer object if it exists (and read lock it)
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );        
        if ( cust == null ) {
            return false;
        } 
        
        // check if the item is available
        ReservableItem item = (ReservableItem)readData(id, key);
        if ( item == null ) {
            return false;
        } else if (item.getCount()==0) {
            return false;
        } else {
            return true;
        }        
    }
    
    // Create a new flight, or add seats to existing flight
    //  NOTE: if flightPrice <= 0 and the flight already exists, it maintains its current price
    public boolean addFlight(int id, int flightNum, int flightSeats, int flightPrice)
        throws RemoteException
    {
        Trace.info("RM::addFlight(" + id + ", " + flightNum + ", $" + flightPrice + ", " + flightSeats + ") called" );
        Flight curObj = (Flight) readData( id, Flight.getKey(flightNum) );
        if ( curObj == null ) {
            // doesn't exist...add it
            Flight newObj = new Flight( flightNum, flightSeats, flightPrice );
            writeData( id, newObj.getKey(), newObj );
            Trace.info("RM::addFlight(" + id + ") created new flight " + flightNum + ", seats=" +
                    flightSeats + ", price=$" + flightPrice );
        } else {
            // add seats to existing flight and update the price...
            curObj.setCount( curObj.getCount() + flightSeats );
            if ( flightPrice > 0 ) {
                curObj.setPrice( flightPrice );
            } // if
            writeData( id, curObj.getKey(), curObj );
            Trace.info("RM::addFlight(" + id + ") modified existing flight " + flightNum + ", seats=" + curObj.getCount() + ", price=$" + flightPrice );
        } // else
        return(true);
    }


    
    public boolean deleteFlight(int id, int flightNum)
        throws RemoteException
    {
        return deleteItem(id, Flight.getKey(flightNum));
    }

    public boolean canDeleteFlight(int id, int flightNum)
        throws RemoteException
    {
        return canDeleteItem(id, Flight.getKey(flightNum));
    }


    // Create a new room location or add rooms to an existing location
    //  NOTE: if price <= 0 and the room location already exists, it maintains its current price
    public boolean addRooms(int id, String location, int count, int price)
        throws RemoteException
    {
        Trace.info("RM::addRooms(" + id + ", " + location + ", " + count + ", $" + price + ") called" );
        Hotel curObj = (Hotel) readData( id, Hotel.getKey(location) );
        if ( curObj == null ) {
            // doesn't exist...add it
            Hotel newObj = new Hotel( location, count, price );
            writeData( id, newObj.getKey(), newObj );
            Trace.info("RM::addRooms(" + id + ") created new room location " + location + ", count=" + count + ", price=$" + price );
        } else {
            // add count to existing object and update price...
            curObj.setCount( curObj.getCount() + count );
            if ( price > 0 ) {
                curObj.setPrice( price );
            } // if
            writeData( id, curObj.getKey(), curObj );
            Trace.info("RM::addRooms(" + id + ") modified existing location " + location + ", count=" + curObj.getCount() + ", price=$" + price );
        } // else
        return(true);
    }

    // Delete rooms from a location
    public boolean deleteRooms(int id, String location)
        throws RemoteException
    {
        return deleteItem(id, Hotel.getKey(location));
        
    }

    // Create a new car location or add cars to an existing location
    //  NOTE: if price <= 0 and the location already exists, it maintains its current price
    public boolean addCars(int id, String location, int count, int price)
        throws RemoteException
    {
        Trace.info("RM::addCars(" + id + ", " + location + ", " + count + ", $" + price + ") called" );
        Car curObj = (Car) readData( id, Car.getKey(location) );
        if ( curObj == null ) {
            // car location doesn't exist...add it
            Car newObj = new Car( location, count, price );
            writeData( id, newObj.getKey(), newObj );
            Trace.info("RM::addCars(" + id + ") created new location " + location + ", count=" + count + ", price=$" + price );
        } else {
            // add count to existing car location and update price...
            curObj.setCount( curObj.getCount() + count );
            if ( price > 0 ) {
                curObj.setPrice( price );
            } // if
            writeData( id, curObj.getKey(), curObj );
            Trace.info("RM::addCars(" + id + ") modified existing location " + location + ", count=" + curObj.getCount() + ", price=$" + price );
        } // else
        return(true);
    }


    // Delete cars from a location
    public boolean deleteCars(int id, String location)
        throws RemoteException
    {
        return deleteItem(id, Car.getKey(location));
    }



    // Returns the number of empty seats on this flight
    public int queryFlight(int id, int flightNum)
        throws RemoteException
    {
        return queryNum(id, Flight.getKey(flightNum));
    }

    // Returns the number of reservations for this flight. 
//    public int queryFlightReservations(int id, int flightNum)
//        throws RemoteException
//    {
//        Trace.info("RM::queryFlightReservations(" + id + ", #" + flightNum + ") called" );
//        RMInteger numReservations = (RMInteger) readData( id, Flight.getNumReservationsKey(flightNum) );
//        if ( numReservations == null ) {
//            numReservations = new RMInteger(0);
//        } // if
//        Trace.info("RM::queryFlightReservations(" + id + ", #" + flightNum + ") returns " + numReservations );
//        return numReservations.getValue();
//    }


    // Returns price of this flight
    public int queryFlightPrice(int id, int flightNum )
        throws RemoteException
    {
        return queryPrice(id, Flight.getKey(flightNum));
    }


    // Returns the number of rooms available at a location
    public int queryRooms(int id, String location)
        throws RemoteException
    {
        return queryNum(id, Hotel.getKey(location));
    }


    
    
    // Returns room price at this location
    public int queryRoomsPrice(int id, String location)
        throws RemoteException
    {
        return queryPrice(id, Hotel.getKey(location));
    }


    // Returns the number of cars available at a location
    public int queryCars(int id, String location)
        throws RemoteException
    {
        return queryNum(id, Car.getKey(location));
    }


    // Returns price of cars at this location
    public int queryCarsPrice(int id, String location)
        throws RemoteException
    {
        return queryPrice(id, Car.getKey(location));
    }

    // Returns data structure containing customer reservation info. Returns null if the
    //  customer doesn't exist. Returns empty RMHashtable if customer exists but has no
    //  reservations.
    public RMHashtable getCustomerReservations(int id, int customerID)
        throws RemoteException
    {
        Trace.info("RM::getCustomerReservations(" + id + ", " + customerID + ") called" );
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );
        if ( cust == null ) {
            Trace.warn("RM::getCustomerReservations failed(" + id + ", " + customerID + ") failed--customer doesn't exist" );
            return null;
        } else {
            return cust.getReservations();
        } // if
    }

    // return a bill
    public String queryCustomerInfo(int id, int customerID)
        throws RemoteException
    {
        Trace.info("RM::queryCustomerInfo(" + id + ", " + customerID + ") called" );
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );
        if ( cust == null ) {
            Trace.warn("RM::queryCustomerInfo(" + id + ", " + customerID + ") failed--customer doesn't exist" );
            return "";   // NOTE: don't change this--WC counts on this value indicating a customer does not exist...
        } else {
                String s = cust.printBill();
                Trace.info("RM::queryCustomerInfo(" + id + ", " + customerID + "), bill follows..." );
                System.out.println( s );
                return s;
        } // if
    }

    // customer functions
    // new customer just returns a unique customer identifier
    
    public int newCustomer(int id)
        throws RemoteException
    {
        Trace.info("INFO: RM::newCustomer(" + id + ") called" );
        // Generate a globally unique ID for the new customer
        int cid = Integer.parseInt( String.valueOf(id) +
                                String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
                                String.valueOf( Math.round( Math.random() * 100 + 1 )));
        Customer cust = new Customer( cid );
        writeData( id, cust.getKey(), cust );
        Trace.info("RM::newCustomer(" + cid + ") returns ID=" + cid );
        return cid;
    }

    // I opted to pass in customerID instead. This makes testing easier
    public boolean newCustomer(int id, int customerID )
        throws RemoteException
    {
        Trace.info("INFO: RM::newCustomer(" + id + ", " + customerID + ") called" );
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );
        if ( cust == null ) {
            cust = new Customer(customerID);
            writeData( id, cust.getKey(), cust );
            Trace.info("INFO: RM::newCustomer(" + id + ", " + customerID + ") created a new customer" );
            return true;
        } else {
            Trace.info("INFO: RM::newCustomer(" + id + ", " + customerID + ") failed--customer already exists");
            return false;
        } // else
    }

    // Check if you can create new customer with given ID
    public boolean canCreateNewCustomer(int id, int customerID )
        throws RemoteException
    {
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );
        if ( cust == null ) {
            return true;
        } else {
            return false;
        } // else
    }

    // Deletes customer from the database. 
    public boolean deleteCustomer(int id, int customerID)
        throws RemoteException
    {
        Trace.info("RM::deleteCustomer(" + id + ", " + customerID + ") called" );
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );
        if ( cust == null ) {
            Trace.warn("RM::deleteCustomer(" + id + ", " + customerID + ") failed--customer doesn't exist" );
            return false;
        } else {            
            // Increase the reserved numbers of all reservable items which the customer reserved. 
            RMHashtable reservationHT = cust.getReservations();
            for (Enumeration e = reservationHT.keys(); e.hasMoreElements();) {        
                String reservedkey = (String) (e.nextElement());
                ReservedItem reserveditem = cust.getReservedItem(reservedkey);
                Trace.info("RM::deleteCustomer(" + id + ", " + customerID + ") has reserved " + reserveditem.getKey() + " " +  reserveditem.getCount() +  " times"  );
                ReservableItem item  = (ReservableItem) readData(id, reserveditem.getKey());
                Trace.info("RM::deleteCustomer(" + id + ", " + customerID + ") has reserved " + reserveditem.getKey() + "which is reserved" +  item.getReserved() +  " times and is still available " + item.getCount() + " times"  );
                item.setReserved(item.getReserved()-reserveditem.getCount());
                item.setCount(item.getCount()+reserveditem.getCount());
            }
            
            // remove the customer from the storage
            removeData(id, cust.getKey());
            
            Trace.info("RM::deleteCustomer(" + id + ", " + customerID + ") succeeded" );
            return true;
        } // if
    }

    public boolean canDeleteCustomer(int id, int customerID)
        throws RemoteException
    {
        Customer cust = (Customer) readData( id, Customer.getKey(customerID) );
        if ( cust == null ) {
            return false;
        } else {
            return true;
        } 
    }

    /*
    // Frees flight reservation record. Flight reservation records help us make sure we
    // don't delete a flight if one or more customers are holding reservations
    public boolean freeFlightReservation(int id, int flightNum)
        throws RemoteException
    {
        Trace.info("RM::freeFlightReservations(" + id + ", " + flightNum + ") called" );
        RMInteger numReservations = (RMInteger) readData( id, Flight.getNumReservationsKey(flightNum) );
        if ( numReservations != null ) {
            numReservations = new RMInteger( Math.max( 0, numReservations.getValue()-1) );
        } // if
        writeData(id, Flight.getNumReservationsKey(flightNum), numReservations );
        Trace.info("RM::freeFlightReservations(" + id + ", " + flightNum + ") succeeded, this flight now has "
                + numReservations + " reservations" );
        return true;
    }
    */

    
    // Adds car reservation to this customer. 
    public boolean reserveCar(int id, int customerID, String location)
        throws RemoteException
    {
        return reserveItem(id, customerID, Car.getKey(location), location);
    }


    // Adds room reservation to this customer. 
    public boolean reserveRoom(int id, int customerID, String location)
        throws RemoteException
    {
        return reserveItem(id, customerID, Hotel.getKey(location), location);
    }
    // Adds flight reservation to this customer.  
    public boolean reserveFlight(int id, int customerID, int flightNum)
        throws RemoteException
    {
        return reserveItem(id, customerID, Flight.getKey(flightNum), String.valueOf(flightNum));
    }

    public boolean canReserveFlight(int id, int customerID, int flightNum)
        throws RemoteException
    {
        return canReserveItem(id, customerID, Flight.getKey(flightNum), String.valueOf(flightNum));
    }
    
    // Reserve an itinerary 
    public boolean itinerary(int id,int customer,Vector flightNumbers,String location,boolean Car,boolean Room)
        throws RemoteException
    {
        return false;
    }

    @Override
	public int start() throws RemoteException {
        // TODO: Auto generated method stub
        return 0;
	}

	@Override
	public boolean commit(int transactionId) throws RemoteException {
        System.out.println("Committing transaction with id = " + transactionId);
        
        DBCopyObject dbCopy = new DBCopyObject(transactionId, m_itemHT);
        performShadowPaging(dbCopy);
        
        lastRetreivedValidXid = transactionId;
        transactionStarted = false;
        currentTransaction = null;
        
        return true;
	}

	@Override
	public boolean abort(int transactionId) throws RemoteException{
        System.out.println("Aborting transaction with id = " + transactionId);
        DBCopyObject recoveredDBCopy = recoverFromMasterFile();
        
        if(recoveredDBCopy != null) {
            this.m_itemHT = recoveredDBCopy.itemsInDBCopy;
        }

        lastRetreivedValidXid = transactionId;
        transactionStarted = false;
        currentTransaction = null;

		return false;
	}

    @Override
  	public boolean shutdown() throws RemoteException {
  		try {
  			registry.unbind("PG7FlightServer");
  			UnicastRemoteObject.unexportObject(rm, true);
  		} catch (Exception e) {
  			throw new RemoteException("Could not unregister service, quiting anyway", e);
  		}
  			
  		return true;	
    }
      
    @Override
    public boolean selfDestruct() throws RemoteException {
        System.exit(1);
        return false;
    }

    private void initTwoPhaseCommitRecovery() {
        File participatorLogFile = new File(LOGDIRECTORY +  PARTICIPATORLOG_FILENAME);

        if(participatorLogFile.isFile()) {
            System.out.println("RECOVERING FROM PARTICIPATOR LOG FILE");
            // RECOVER!!!
            // store all the objects into the list until EOF is reached
            ArrayList<TransactionLogItem> transactionLogItems = returnLoggedTransactionItems();
            TransactionLogItem interruptedTransaction = null;

            if(transactionLogItems.size() == 0) {
                return;
            }
            else{
                interruptedTransaction = transactionLogItems.get(transactionLogItems.size() - 1);
            }

            System.out.println("Interrupted transaction had id: " + interruptedTransaction.transactionID);

            if(rm_middleware == null) {
                connectMWServer(mwServer, mwPort);
            }

            ExecutorService executor = null;

            try{
                switch(interruptedTransaction.currentState) {
                    case INITIAL:
                        // ABORT and send ABORTED to coordinator
                        System.out.println("Participant was waiting for VOTEREQ. ABORTING");
                        abort(interruptedTransaction.transactionID);
                        interruptedTransaction.setTransactionState(TransactionState.ABORTED);
                        rewriteTransactionLogItem(interruptedTransaction);
                        
                        executor = Executors.newSingleThreadExecutor();
                        final TransactionLogItem waitingVoteReqMessageCopy = new TransactionLogItem(interruptedTransaction);
                        executor.submit(() -> {
                            return rm_middleware.transfer2PCMessage(waitingVoteReqMessageCopy);
                        });
                        break;
                    case VOTE_YES:
                        // ask coordinator what happened!!
                        // new thread, calls rm_middleware.transfer2PCMessage(interruptedTransaction)
                        System.out.println("Participant last voted YES. Asking coordinator to resend decision");
                        interruptedTransaction.senderServerName = "flight";
                        executor = Executors.newSingleThreadExecutor();
                        final TransactionLogItem voteYesMessageCopy = new TransactionLogItem(interruptedTransaction);
                        Future<TransactionLogItem> coordinatorResponseTask = executor.submit(() -> {
                            return rm_middleware.transfer2PCMessage(voteYesMessageCopy);
                        });
    
                        System.out.println("Waiting for coordinator to respond");
                        while(!coordinatorResponseTask.isDone()) {
                            // block until coordinator responds (homework states no need for cooperative termination protocol)
                        }
    
                        TransactionLogItem coordinatorResponse = null;
                        try{
                            coordinatorResponse = coordinatorResponseTask.get();
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }

                        if(coordinatorResponse != null) {
                            System.out.println("Response with state " + coordinatorResponse.currentState + " is received.");
                            transfer2PCMessage(coordinatorResponse);
                        }
                        else{
                            System.out.println("Coordinator response is NULL!");
                        }
                        break;
                    case COMMITTED:
                        break;
                    case ABORTED:
                        break;
                    default:
                        break;
                }
            }
            catch(Exception e) {
            }
        }
        else{
            try{
                participatorLogFile.createNewFile();
            }
            catch(IOException e) {
            }

        }
    }

    @Override
    public TransactionLogItem transfer2PCMessage(TransactionLogItem receivedLogItem) throws RemoteException {
        System.out.println("Flight server received log item with xid = " + receivedLogItem.transactionID + " and state = " + receivedLogItem.currentState);
        
        TransactionLogItem response = new TransactionLogItem(receivedLogItem);
        
        switch(receivedLogItem.currentState) {
            case SEND_VOTEREQ:
                transactionStarted = false;
                // iterate through receivedLogItem.operationsList and check if all of them can be performed
                // if yes, write VOTE_YES to response. otherwise, write VOTE_NO
                askToEnforceCrash("Crash Flight RM after receive vote request but before sending answer?");
                for(int i = 0; i < response.operationsInTransaction.size(); i++) {
                    ObjectParam operation = response.operationsInTransaction.get(i);
                    Boolean canBeExecuted = true;

                    switch(operation.getMethodName().toLowerCase()){
                        case "newflight":
                            canBeExecuted = true;
                            break;
                        case "deleteflight":
                            canBeExecuted = true;
                            break;
                        case "reserveflight":
                            canBeExecuted = true;
                            break;
                        default:
                            break;
                    }

                    if(!canBeExecuted) {
                        try{
                            System.out.println("Operation " + operation.getMethodName() + " for transaction # " + receivedLogItem.transactionID + " cannot be performed!");
                            System.out.println("Writing ABORT to log!");
                            response.setTransactionState(TransactionState.ABORTED);
                            rewriteTransactionLogItem(response);
                            
                            System.out.println("Returning VOTE_NO to coordinator");
                            response.setTransactionState(TransactionState.VOTE_NO);
                            return response;
                        }
                        finally {
                            ExecutorService executor = Executors.newSingleThreadExecutor();
                            executor.submit(() -> {
                                askToEnforceCrash("Crash Flight RM after sending answer?");
                            });
                        }
                    }
                }

                try{
                    TransactionLogItem alreadyFound = getLogItemWithID(receivedLogItem.transactionID);
                    if(alreadyFound != null && alreadyFound.currentState.equals(TransactionState.ABORTED)) {
                        System.out.println("Returning VOTE_NO to coordinator");
                        response.setTransactionState(TransactionState.VOTE_NO);
                        return response;
                    }

                    System.out.println("All operations for transaction # " + receivedLogItem.transactionID + " are valid");
                    System.out.println("Writing VOTE_YES to log!");
                    response.setTransactionState(TransactionState.VOTE_YES);
                    System.out.println("Returning VOTE_YES to coordinator");
                    rewriteTransactionLogItem(response);
                    return response;
                }
                finally {
                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    executor.submit(() -> {
                        askToEnforceCrash("Crash Flight RM after sending answer?");
                    });
                }
            case SEND_COMMIT:
                if(receivedLogItem.transactionID > lastRetreivedValidXid) {
                    // commit the working set received by the coordinator and write committed to your log
                    // if something still goes wrong, write ABORTED to response
                    // finally based on the outcome, write COMMITED or ABORTED to the participant log
                    askToEnforceCrash("Crash Flight RM after receiving decision but before committing?");
                    if(!receivedLogItem.workingSet.isEmpty()) {
                        Set<String> workingSetKeys = receivedLogItem.workingSet.keySet();
                        for(String key : workingSetKeys) {
                            Flight item = (Flight) receivedLogItem.workingSet.get(key);
                            if(item != null) {
                                // effectively addRoom (with quantity = final number after reservations & price = latest updated price)
                                writeRMEntry(receivedLogItem.transactionID, item.getKey(), item);
                                System.out.println("Updating flight RM hashtable entry with key = " + item.getKey());
                            }
                            else{
                                // effectively deleteRoom
                                removeRMEntry(receivedLogItem.transactionID, item.getKey());
                                System.out.println("REMOVING flight RM hashtable entry with key = " + item.getKey());
                            }
                        }
                    }

                    commit(receivedLogItem.transactionID);
                    System.out.println("Committed transaction after receiving commit request from coordinator!");
                    response.setTransactionState(TransactionState.COMMITTED);
                    rewriteTransactionLogItem(response);
                    return response;
                }
                break;
            case SEND_ABORT:
                if(receivedLogItem.transactionID > lastRetreivedValidXid) {
                    askToEnforceCrash("Crash Flight RM after receiving decision but before aborting?");
                    abort(receivedLogItem.transactionID);
                    System.out.println("Aborted transaction after receiving abort request from coordinator!");
                    response.setTransactionState(TransactionState.ABORTED);
                    rewriteTransactionLogItem(response);
                    return response;
                }
                break;
            default:
                break;
        }

        return receivedLogItem;
    }

    private TransactionLogItem getLogItemWithID(int transactionID) {
        ArrayList<TransactionLogItem> logItems = returnLoggedTransactionItems();
        TransactionLogItem itemFound = null;

        for(int i = 0; i < logItems.size(); i++) {
            if(logItems.get(i).transactionID == transactionID) {
                itemFound = logItems.get(i);
                break;
            }
        }

        return itemFound;
    }

    private ArrayList<TransactionLogItem> returnLoggedTransactionItems() {
		return setAndReturnTransactionLogItems(null, (logItem -> true), true);
	}

	// get all the objects in the log file, write only the ones that have endOfTransaction == false
	private void garbageCollectOnLogRecords(int currentTransactionId) {
		setAndReturnTransactionLogItems(null, (logItem -> logItem.endOfTransaction == false || logItem.transactionID >= currentTransactionId - 2), false);
	}

	// remove all log items in the log with the same transactionId as the parameter. write it with its new state
	private void rewriteTransactionLogItem(TransactionLogItem transactionLogItem) {
        System.out.println("Writing transaction with id: " + transactionLogItem.transactionID + " to participator log");        
		setAndReturnTransactionLogItems(transactionLogItem, (logItem -> logItem.transactionID != transactionLogItem.transactionID), false);
	}

    // remove all log items in the log with the same transactionId as the parameter. write it with its new state
	private ArrayList<TransactionLogItem> setAndReturnTransactionLogItems(TransactionLogItem transactionLogItemToWrite, 
        Predicate<TransactionLogItem> predicateFunction, boolean readonly) {
        ArrayList<TransactionLogItem> transactionLogItems = new ArrayList<TransactionLogItem>();
        
        File participatorLogFile = new File(LOGDIRECTORY +  PARTICIPATORLOG_FILENAME);
        
        FileOutputStream fos;
        ObjectOutputStream oos;

        FileInputStream fis;
        ObjectInputStream ois;

        if(participatorLogFile.isFile()) {
			if(participatorLogFile.length() != 0) {
				try {
					System.out.println("Participant log file is NOT empty.");
					fis = new FileInputStream(participatorLogFile);
					ois = new ObjectInputStream(fis);
	
					// store all the objects into the list until EOF is reached
					transactionLogItems = (ArrayList<TransactionLogItem>)ois.readObject();
					System.out.println("READ TRANSACTION LIST WITH SIZE = " + transactionLogItems.size());
					for(int i = 0; i < transactionLogItems.size(); i++) {
						System.out.println("Found transaction with id = " + transactionLogItems.get(i).transactionID + 
						", numOperations = " + transactionLogItems.get(i).operationsInTransaction.size() + ", workingSet.size = " + 
						transactionLogItems.get(i).workingSet.size() + " and state = " + transactionLogItems.get(i).currentState);
					}
					ois.close();
				} catch (Exception e) {
				}
			}
			else{
				System.out.println("Participant log file is empty.");
			}
			
			// filter the list via the predicate
			//transactionLogItems = (ArrayList<TransactionLogItem>)transactionLogItems.stream().filter(logItem -> logItem.transactionID != transactionLogItem.transactionID).collect(Collectors.toList());
			transactionLogItems = (ArrayList<TransactionLogItem>)transactionLogItems.stream().filter(predicateFunction).collect(Collectors.toList());
			
			if(transactionLogItemToWrite != null){
				transactionLogItems.add(transactionLogItemToWrite);
			}
			
			Collections.sort(transactionLogItems, new Comparator<TransactionLogItem>() {
				public int compare(TransactionLogItem t1, TransactionLogItem t2) {
					if(t1.transactionID == t2.transactionID){
						return 0;
					}	
					return t1.transactionID < t2.transactionID ? -1 : 1;
				}
			});

			if(!readonly) {
				for(int i = 0; i < transactionLogItems.size(); i++) {
					System.out.println("Writing transaction with id = " + transactionLogItems.get(i).transactionID + 
					", numOperations = " + transactionLogItems.get(i).operationsInTransaction.size() + ", workingSet.size = " + 
					transactionLogItems.get(i).workingSet.size() + " and state = " + transactionLogItems.get(i).currentState + " to log.");
				}
				try{
					// create output streams, write back the filtered list to the log file
					fos = new FileOutputStream(participatorLogFile);
					oos = new ObjectOutputStream(fos);

					oos.writeObject(transactionLogItems);

					oos.close();
				}
				catch(Exception e) {
				}
			}
		}

        return transactionLogItems;
    }

    public static void askToEnforceCrash(String messageToDisplay) {
        if(ASKFORCRASH) {
            Scanner scanner = new Scanner( System.in );
            String input;
            boolean validInput = false;

            do{
                System.out.println(messageToDisplay + " (yes/no):");
                input = scanner.next();

                if(input.toLowerCase().equals("yes") || input.toLowerCase().equals("no")) {
                    validInput = true;
                }
            } while(!validInput);

            if(input.toLowerCase().equals("yes")) {
                System.exit(1);
            }
        }
	}

}