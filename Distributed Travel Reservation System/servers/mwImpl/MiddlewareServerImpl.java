package servers.mwImpl;

import ResInterface.*;
import common.*;
import servers.*;
import servers.mwImpl.lockManager.*;

import java.util.*;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RMISecurityManager;

import object.ObjectParam;

import TwoPhaseCommit.*;
import TwoPhaseCommit.Shadowing.*;

public class MiddlewareServerImpl extends ShadowPager implements ResourceManager 
{
	public static MiddlewareServerImpl instance;

	static ResourceManager rm_car= null;
	static ResourceManager rm_flight= null;
	static ResourceManager rm_room= null;
	static ResourceManager rm;
	static LockManager lm;
	TransactionManager tm=null;

	static String server = "localhost";
	static String carServer="localhost";
	static String roomServer="localhost";
	static String flightServer="localhost";
	static int port = 1099;

	protected RMHashtable m_itemHT = new RMHashtable();

	public static final String CAR = "car";
	public static final String FLIGHT = "flight";
	public static final String ROOM = "room";
	public static final String CUSTOMER = "customer";
	public static final int READ = 0;
	public static final int WRITE = 1;

	private long startTime, endTime, runningTime = 0;

	public static void main(String args[]) {
		// Figure out where server is running
		server = "localhost";
		carServer="localhost";
		roomServer="localhost";
		flightServer="localhost";
		port = 1099;

		if (args.length == 2) {
			server = args[0];
			carServer=server;
			flightServer=server;
			roomServer=server;
			port = Integer.parseInt(args[1]);

		} else if (args.length == 5 ){
			server = args[0];
			carServer=args[1];
			roomServer=args[2];
			flightServer=args[3];
			port = Integer.parseInt(args[4]);
		}
		else  { //
			System.err.println ("Wrong usage");
		System.out.println("Usage: java mwImpl.middlewareServerImpl [Host] [RMCarHost] [RMRoomHost] [RMFlightHost] [port]");
		System.exit(1);
		}
		// Create and install a security manager
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}              


		connectCarServer(carServer,  port);
		connectRoomServer(roomServer, port);
		connectFlightServer(flightServer, port);

		Trace.info("All server RM are connected ");
		MyThread t  =  new MyThread ();
		t.start();
		Trace.warn(" Time2Live Thread started ");

		try {
			// create a new Server object
			MiddlewareServerImpl obj = new MiddlewareServerImpl();
			DBCopyObject dbCopy = obj.initShadowpaging("mwImpl");
            
            if(dbCopy != null) {
                if(dbCopy.itemsInDBCopy != null) {
				  obj.m_itemHT = dbCopy.itemsInDBCopy;

				  System.out.println("\nRecovered Items Hashtable:");
				  if(!dbCopy.itemsInDBCopy.isEmpty()) {
					  Set<String> recoveredHTKeys = dbCopy.itemsInDBCopy.keySet();
					  for(String key : recoveredHTKeys) {
						  Customer customer = (Customer) dbCopy.itemsInDBCopy.get(key);
						  //System.out.println("Key: " + key + ", Value: " + customer);
					  }
				  }
                }
                if(TransactionManager.numTransaction < dbCopy.currentTransactionID) {
					TransactionManager.numTransaction = dbCopy.currentTransactionID;
				}
            }
			
			obj.initTwoPhaseCommitRecovery();
			
			// dynamically generate the stub (client proxy)
			rm = (ResourceManager) UnicastRemoteObject.exportObject(obj, 0);

			// Bind the remote object's stub in the registry
			Registry registry = LocateRegistry.getRegistry(port);
			registry.rebind("PG7MiddlewareServer", rm);

			ServerThread serverThread = new ServerThread(obj, TransactionManager.RESPONSETIMEOUT);
            serverThread.start();

			System.err.println("Server ready");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}

		try{
			//instantiate the LockManager Class
			lm = new LockManager();
			System.out.println("lock Manager Is instatiated");
			//


		}catch(Exception e){
			System.out.println("Exception in MyThread "+e.toString());

		}

	}

	protected static void connectCarServer(String carServer, int port){
		Trace.info("connecting CarServer.... ");
		try 
		{
			// get a reference to the rmiregistry
			Registry registry = LocateRegistry.getRegistry(carServer, port);
			// get the proxy and the remote reference by rmiregistry lookup
			rm_car = (ResourceManager) registry.lookup("PG7CarServer");
			if(rm_car!=null)
			{
				System.out.println("Successful");
				System.out.println("Connected to CAR RM");
			}
			else
			{
				System.out.println("Unsuccessful to PG7CarServer");
			}
			// make call on remote method
		} 
		catch (Exception e) 
		{    
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}

	}
	protected static void connectRoomServer(String roomServer,int port){
		Trace.info("connecting RoomServer.... ");
		try 
		{
			// get a reference to the rmiregistry
			Registry registry = LocateRegistry.getRegistry(roomServer, port);
			// get the proxy and the remote reference by rmiregistry lookup
			rm_room = (ResourceManager) registry.lookup("PG7RoomServer");
			if(rm_room!=null)
			{
				System.out.println("Successful");
				System.out.println("Connected to PG7RoomServer RM");
			}
			else
			{
				System.out.println("Unsuccessful to PG7RoomServer");
			}
			// make call on remote method
		} 
		catch (Exception e) 
		{    
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}


	}
	protected static void connectFlightServer(String flightServer, int port){
		Trace.info("connecting Flight Server.... ");
		try 
		{
			// get a reference to the rmiregistry
			Registry registry = LocateRegistry.getRegistry(flightServer, port);
			// get the proxy and the remote reference by rmiregistry lookup
			rm_flight = (ResourceManager) registry.lookup("PG7FlightServer");
			if(rm_flight!=null)
			{
				System.out.println("Successful");
				System.out.println("Connected to PG7FlightServer RM");
			}
			else
			{
				System.out.println("Unsuccessful to PG7FlightServer");
			}
			// make call on remote method
		} 
		catch (Exception e) 
		{    
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}

	}

	@Override
    public void connectToServers() {
        connectCarServer(carServer, port);
		connectRoomServer(roomServer, port);
		connectFlightServer(flightServer, port);
	}
	
	@Override
    public boolean pingServer() throws RemoteException {
        return true;
    }

    @Override
    public boolean pingOthers() throws RemoteException {
        return rm_car.pingServer() && rm_flight.pingServer() && rm_flight.pingServer();
	}
	
	@Override
	public void checkForTimeOuts() throws RemoteException {
	}

	public MiddlewareServerImpl() throws RemoteException {
		instance = this;
	}

	public ReservableItem getRMEntry(int id, String key) throws RemoteException {
        return (ReservableItem) readData( id, Customer.getKey(Integer.parseInt(key)) );
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
    protected boolean canDeleteItem(ReservableItem item) {
        // Check if there is such an item in the storage
        if (item == null ) {
            return false;
        } else {
            if (item.getReserved()==0) {
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
		Trace.info("RM::queryPrice(" + id + ", " + key + ") called" );
		ReservableItem curObj = (ReservableItem) readData( id, key);
		int value = 0; 
		if ( curObj != null ) {
			value = curObj.getPrice();
		} // else
		Trace.info("RM::queryPrice(" + id + ", " + key + ") returns cost=$" + value );
		return value;        
	}

	// reserve an item
	protected boolean reserveItem(int id, int customerID, String key, String location) {
		Trace.info("RM::reserveItem( " + id + ", customer=" + customerID + ", " +key+ ", "+location+" ) called" );        
		// Read customer object if it exists (and read lock it)
		Customer cust = (Customer) readData( id, Customer.getKey(customerID) );        
		if ( cust == null ) {
			Trace.warn("RM::reserveItem( " + id + ", " + customerID + ", " + key + ", "+location+")  failed--customer doesn't exist" );
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
    protected boolean canReserveItem(Customer customer, ReservableItem item) {
        // Read customer object if it exists (and read lock it)
        //Customer cust = (Customer) readData( id, Customer.getKey(customerID) );        
        if ( customer == null ) {
            return false;
        } 
        
        // check if the item is available
        //ReservableItem item = (ReservableItem)readData(id, key);
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
	public boolean addFlight(int id, int flightNum, int flightSeats, int flightPrice) throws RemoteException {
		Trace.info("RM::addFlight(" + id + ", " + flightNum + ", $" + flightPrice + ", " + flightSeats + ") called" ); 
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = FLIGHT + ", " + flightNum;
			ReservableItem flight;
			
			if(tml.workingSet.containsKey(strData)) {
				flight = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
			}

			if(lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " FLIGHT, " +  flightNum + " resource for addFlight operation");
				if(tml.workingSet.containsKey(strData)) {
					flight = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
				}
				if(flight == null) {
					flight = (ReservableItem) new Flight(flightNum, flightSeats, flightPrice);
				}
				else{
					flight.setCount( flight.getCount() + flightSeats );
					if ( flightPrice > 0 ) {
						flight.setPrice( flightPrice );
					}
				}
				tml.workingSet.put(strData, flight);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.addFlightNum(flightNum);
				newObj.setFlightSeats(flightSeats);
				newObj.setPrice(flightPrice);
				newObj.setClassName("Flight");
				newObj.setMethodName("newflight");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
				//return rm_flight.addFlight(id,flightNum,flightSeats,flightPrice)
			}
		} catch (DeadlockException e) {
			System.out.println(e);
		}
		return false;
	}

	public boolean deleteFlight(int id, int flightNum) throws RemoteException {
		Trace.info("RM::deleteFlight(" + id + ", " + flightNum + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = FLIGHT + ", " + flightNum;
			ReservableItem flight;
			
			if(tml.workingSet.containsKey(strData)) {
				flight = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
			}

			if(canDeleteItem(flight) && lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " FLIGHT, " +  flightNum + " resource for deleteFlight operation");
				if(tml.workingSet.containsKey(strData)) {
					flight = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
				}
				if(tml.workingSet.containsKey(strData)) {
					tml.workingSet.put(strData, null);
					if(flight == null) {
						tml.workingSet.remove(strData);
					}
				}
				else{
					System.out.println("Working set has no entry but trying to delete. This should never happen.");
					return false;
				}

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.addFlightNum(flightNum);
				newObj.setClassName("Flight");
				newObj.setMethodName("deleteflight");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
		}
		return false;
	}

	// Create a new room location or add rooms to an existing location
	//  NOTE: if price <= 0 and the room location already exists, it maintains its current price
	public boolean addRooms(int id, String location, int count, int price) throws RemoteException {
		Trace.info("RM::addRooms(" + id + ", " + location + ", " + count + ", $" + price + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = ROOM + ", " + location;
			ReservableItem room;
			
			if(tml.workingSet.containsKey(strData)) {
				room = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				room = (ReservableItem) rm_room.getRMEntry(id, location );
			}

			if(lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " ROOM, " +  location + " resource for newRoom operation");
				if(tml.workingSet.containsKey(strData)) {
					room = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					room = (ReservableItem) rm_room.getRMEntry(id, location );
				}
				if(room == null) {
					room = (ReservableItem) new Hotel(location, count, price);
				}
				else{
					room.setCount( room.getCount() + count );
					if ( price > 0 ) {
						room.setPrice( price );
					}
				}
				tml.workingSet.put(strData, room);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setNumRooms(count);
				newObj.setPrice(price);
				newObj.setClassName("Room");
				newObj.setMethodName("newroom");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
				//return rm_room.addRooms(id,location,count,price)
			}
		} catch (DeadlockException e) {
			System.out.println(e);
		}
		return false;
	}

	// Delete rooms from a location
	public boolean deleteRooms(int id, String location) throws RemoteException {
		Trace.info("RM::deleteRooms(" + id + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = ROOM + ", " + location;
			ReservableItem room;
			
			if(tml.workingSet.containsKey(strData)) {
				room = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				room = (ReservableItem) rm_room.getRMEntry(id, location );
			}

			if(canDeleteItem(room) && lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " ROOM, " +  location + " resource for deleteRoom operation");
				if(tml.workingSet.containsKey(strData)) {
					room = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					room = (ReservableItem) rm_room.getRMEntry(id, location );
				}
				
				if(tml.workingSet.containsKey(strData)) {
					tml.workingSet.put(strData, null);
					if(room == null) {
						tml.workingSet.remove(strData);
					}
				}
				else{
					System.out.println("Working set has no entry but trying to delete. This should never happen.");
					return false;
				}

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setClassName("Room");
				newObj.setMethodName("deleteroom");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
		}
		return false;
	}

	// Create a new car location or add cars to an existing location
	//  NOTE: if price <= 0 and the location already exists, it maintains its current price
	public boolean addCars(int id, String location, int count, int price) throws RemoteException {
		Trace.info("RM::addCars(" + id + ", " + location + ", " + count + ", $" + price + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = CAR + ", " + location;
			ReservableItem car;
			
			if(tml.workingSet.containsKey(strData)) {
				car = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				car = (ReservableItem) rm_car.getRMEntry(id, location );
			}

			if(lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CAR, " +  location + " resource for newCar operation");
				if(tml.workingSet.containsKey(strData)) {
					car = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					car = (ReservableItem) rm_car.getRMEntry(id, location );
				}

				if(car == null) {
					car = (ReservableItem) new Car(location, count, price);
				}
				else{
					car.setCount( car.getCount() + count );
					if ( price > 0 ) {
						car.setPrice( price );
					}
				}
				tml.workingSet.put(strData, car);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setNumCars(count);
				newObj.setPrice(price);
				newObj.setClassName("Car");
				newObj.setMethodName("newcar");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
	}

	// Delete cars from a location
	public boolean deleteCars(int id, String location) throws RemoteException {
		Trace.info("RM::deleteCars(" + id + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);
		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = CAR + ", " + location;
			ReservableItem car;
			
			if(tml.workingSet.containsKey(strData)) {
				car = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				car = (ReservableItem) rm_car.getRMEntry(id, location );
			}

			if(canDeleteItem(car) && lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CAR, " +  location + " resource for deleteCar operation");
				if(tml.workingSet.containsKey(strData)) {
					car = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					car = (ReservableItem) rm_car.getRMEntry(id, location );
				}
				if(tml.workingSet.containsKey(strData)) {
					tml.workingSet.put(strData, null);
					if(car == null) {
						tml.workingSet.remove(strData);
					}
				}
				else{
					System.out.println("Working set has no entry but trying to delete. This should never happen.");
					return false;
				}

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setClassName("Car");
				newObj.setMethodName("deletecar");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
	}

	// Returns the number of empty seats on this flight
	public int queryFlight(int id, int flightNum) throws RemoteException {
		Trace.info("RM::queryFlight(" + id + ", " + flightNum + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			String strData = FLIGHT + ", " + flightNum;
			ReservableItem flight;
			
			if(tml.workingSet.containsKey(strData)) {
				flight = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
			}

			if(flight != null &&lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " FLIGHT, " +  flightNum + " resource for queryFlight operation");
				if(tml.workingSet.containsKey(strData)) {
					flight = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
				}
				
				tml.workingSet.put(strData, flight);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.addFlightNum(flightNum);
				newObj.setClassName("Flight");
				newObj.setMethodName("queryflight");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return flight.getCount();
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}

	// Returns price of this flight
	public int queryFlightPrice(int id, int flightNum )
		throws RemoteException
	{
		Trace.info("RM::queryFlightPrice(" + id + ", " + flightNum + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			String strData = FLIGHT + ", " + flightNum;
			ReservableItem flight;
			
			if(tml.workingSet.containsKey(strData)) {
				flight = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
			}

			if(flight != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " FLIGHT, " +  flightNum + " resource for queryFlightPrice operation");
				
				if(tml.workingSet.containsKey(strData)) {
					flight = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
				}
				
				tml.workingSet.put(strData, flight);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.addFlightNum(flightNum);
				newObj.setClassName("Flight");
				newObj.setMethodName("queryflightprice");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return flight.getPrice();
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}


	// Returns the number of rooms available at a location
	public int queryRooms(int id, String location) throws RemoteException {
		Trace.info("RM::queryRoom(" + id + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			String strData = ROOM + ", " + location;
			ReservableItem room;
			
			if(tml.workingSet.containsKey(strData)) {
				room = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				room = (ReservableItem) rm_room.getRMEntry(id, location );
			}

			if(room != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " ROOM, " +  location + " resource for queryRoom operation");
				
				if(tml.workingSet.containsKey(strData)) {
					room = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					room = rm_room.getRMEntry(id, location );
				}
				
				tml.workingSet.put(strData, room);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setClassName("Room");
				newObj.setMethodName("queryroom");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return room.getCount();
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}

	// Returns room price at this location
	public int queryRoomsPrice(int id, String location) throws RemoteException {
		Trace.info("RM::queryRoomPrice(" + id + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			String strData = ROOM + ", " + location;
			ReservableItem room;
			
			if(tml.workingSet.containsKey(strData)) {
				room = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				room = rm_room.getRMEntry(id, location );
			}

			if(room != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " ROOM, " +  location + " resource for queryRoomPrice operation");
				
				if(tml.workingSet.containsKey(strData)) {
					room = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					room = rm_room.getRMEntry(id, location );
				}
				
				tml.workingSet.put(strData, room);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setClassName("Room");
				newObj.setMethodName("queryroomprice");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return room.getPrice();
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}


	// Returns the number of cars available at a location
	public int queryCars(int id, String location) throws RemoteException {
		Trace.info("RM::queryCars(" + id + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction 
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			String strData = CAR + ", " + location;
			ReservableItem car;
			
			if(tml.workingSet.containsKey(strData)) {
				car = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				car = rm_car.getRMEntry(id, location );
			}

			if(car != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " CAR, " +  location + " resource for queryCar operation");
				
				if(tml.workingSet.containsKey(strData)) {
					car = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					car = rm_car.getRMEntry(id, location );
				}
				
				tml.workingSet.put(strData, car);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setClassName("Car");
				newObj.setMethodName("querycar");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return car.getCount();
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}


	// Returns price of cars at this location
	public int queryCarsPrice(int id, String location) throws RemoteException {
		Trace.info("RM::queryCarsPrice(" + id + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction 
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			String strData = CAR + ", " + location;
			ReservableItem car;

			if(tml.workingSet.containsKey(strData)) {
				car = (ReservableItem) tml.workingSet.get(strData);
			}
			else{
				car = (ReservableItem) rm_car.getRMEntry(id, location );
			}
			
			if(car != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " CAR, " +  location + " resource for queryCar operation");
				
				if(tml.workingSet.containsKey(strData)) {
					car = (ReservableItem) tml.workingSet.get(strData);
				}
				else{
					car = (ReservableItem) rm_car.getRMEntry(id, location );
				}
				
				tml.workingSet.put(strData, car);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setLocation(location);
				newObj.setClassName("Car");
				newObj.setMethodName("querycarprice");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return car.getPrice();
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}

	// Returns data structure containing customer reservation info. Returns null if the
	//  customer doesn't exist. Returns empty RMHashtable if customer exists but has no
	//  reservations.
	public RMHashtable getCustomerReservations(int id, int customerID) throws RemoteException {
		Trace.info("Middelware::getCustomerReservations(" + id + ", " + customerID + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return null;
		}

		try {
			String strData = CUSTOMER + ", " + customerID;
			Customer customer;
			
			if(tml.workingSet.containsKey(strData)) {
				customer = (Customer) tml.workingSet.get(strData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}

			if(customer != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for getCustomerReservations operation");

				if(tml.workingSet.containsKey(strData)) {
					customer = (Customer) tml.workingSet.get(strData);
				}
				else{
					customer = (Customer) readData( id, Customer.getKey(customerID) );
				}

				tml.workingSet.put(strData, customer);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(customerID);
				newObj.setClassName("middleware");
				newObj.setMethodName("getreservations");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return customer.getReservations();
			}
			else{
				Trace.warn("Middelware::getCustomerReservations failed(" + id + ", " + customerID + ") failed--customer doesn't exist" );
				return null;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return null;
	}

	// return a bill
	public String queryCustomerInfo(int id, int customerID) throws RemoteException {
		Trace.info("Middelware::queryCustomerInfo(" + id + ", " + customerID + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return "";
		}

		try {
			String strData = CUSTOMER + ", " + customerID;
			Customer customer;

			if(tml.workingSet.containsKey(strData)) {
				customer = (Customer) tml.workingSet.get(strData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}

			if(customer != null && lm.Lock(id,strData, READ)){
				System.out.println("READ Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for getCustomerReservations operation");

				if(tml.workingSet.containsKey(strData)) {
					customer = (Customer) tml.workingSet.get(strData);
				}
				else{
					customer = (Customer) readData( id, Customer.getKey(customerID) );
				}
				if(customer == null) {
					System.out.println("Customer does not exist. No bill was found...");
					return "";
				}

				Trace.info("Middelware::queryCustomerInfo(" + id + ", " + customerID + "), bill follows..." );
				String custInfo = "Bill for customer " + customerID + " is: \n" + customer.printBill();
				System.out.println( custInfo );

				tml.workingSet.put(strData, customer);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(customerID);
				newObj.setClassName("middleware");
				newObj.setMethodName("querycustomer");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return custInfo;
			}
			else{
				Trace.warn("RM::queryCustomerInfo(" + id + ", " + customerID + ") failed--customer doesn't exist" );
				return "";
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return "";
	}

	// customer functions

	// customer functions
	// new customer just returns a unique customer identifier
	public int getUniqueCid(int id) {
		return Integer.parseInt( String.valueOf(id) +
		String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
		String.valueOf( Math.round( Math.random() * 100 + 1 )));
	}

	// new customer just returns a unique customer identifier
	public int newCustomer(int id) throws RemoteException {
		Trace.info("INFO: Middelware::newCustomer(" + id + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return -1;
		}

		try {
			// Generate a globally unique ID for the new customer
			int cid = Integer.parseInt( String.valueOf(id) +
					String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
					String.valueOf( Math.round( Math.random() * 100 + 1 )));

			String strData = CUSTOMER + ", " + cid;
			if(canCreateNewCustomer(id, cid) && lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CUSTOMER, " +  cid + " resource for newCustomer operation");

				if(!tml.workingSet.containsKey(strData)) {
					Customer cust = (Customer) readData( id, Customer.getKey(cid) );
					if ( cust == null && canCreateNewCustomer(id, cid)) {
						cust = new Customer(cid);
						tml.workingSet.put(strData, cust);
					}
					else{
						System.out.println("Trying to create a customer that already exists! This should never happen!");
					}
				}
				else{
					System.out.println("Working set has customer entry but trying to add anyway. This should never happen.");
				}

				Trace.info("Middelware::newCustomer(" + cid + ") returns ID=" + cid );

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(cid);
				newObj.setClassName("middleware");
				newObj.setMethodName("newcustomer");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return cid;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return -1;
	}

	// I opted to pass in customerID instead. This makes testing easier
	public boolean newCustomer(int id, int customerID ) throws RemoteException {
		Trace.info("INFO: Middelware::newCustomer(" + id + ", " + customerID + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = CUSTOMER + ", " + customerID;
			Customer cust;
			if(tml.workingSet.containsKey(strData)) {
				cust = (Customer) tml.workingSet.get(strData);
			}
			else{
				cust = (Customer) readData( id, Customer.getKey(customerID) );
			}
			if ( cust == null ) {
				if(canCreateNewCustomer(id, customerID) && lm.Lock(id,strData, WRITE)){
					System.out.println("WRITE Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for newCustomer operation");

					cust = (Customer) readData( id, Customer.getKey(customerID) );
					if( cust != null || !canCreateNewCustomer(id, customerID)) {
						System.out.println("Customer with id already exists in system, can not be created!");
						return false;
					}
					if(!tml.workingSet.containsKey(strData)) {
						cust = new Customer(customerID);
						tml.workingSet.put(strData, cust);
					}
					else{
						System.out.println("Working set has customer entry but trying to add anyway. This should never happen.");
						return false;
					}

					ObjectParam newObj= new ObjectParam();
					newObj.setId(id);
					newObj.setCid(customerID);
					newObj.setClassName("middleware");
					newObj.setMethodName("newcustomer");

					tml.transactionLogItem.operationsInTransaction.add(newObj);
					
					return true;
				}
			}
			else{
				Trace.info("INFO: Middelware::newCustomer(" + id + ", " + customerID + ") failed--customer already exists");
				return false;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
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
		}
	}

	// Deletes customer from the database. 
	public boolean deleteCustomer(int id, int customerID) throws RemoteException {
		Trace.info("Middelware::deleteCustomer(" + id + ", " + customerID + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String strData = CUSTOMER + ", " + customerID;
			Customer customer;
			if(tml.workingSet.containsKey(strData)) {
				customer = (Customer) tml.workingSet.get(strData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}
			if(customer != null && canDeleteCustomer(id, customerID) && lm.Lock(id,strData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for deleteCustomer operation");

				if(tml.workingSet.containsKey(strData)) {
					customer = (Customer) tml.workingSet.get(strData);
				}
				else{
					customer = (Customer) readData( id, Customer.getKey(customerID) );
				}

				if(tml.workingSet.containsKey(strData)) {
					tml.workingSet.put(strData, null);
					if(customer == null) {
						tml.workingSet.remove(strData);
					}
				}
				else{
					System.out.println("Working set has no entry but trying to delete. This should never happen.");
					return false;
				}

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(customerID);
				newObj.setClassName("middleware");
				newObj.setMethodName("deletecustomer");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
	}

	// Check if you can delete the given customer from the database. 
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

	// Adds car reservation to this customer. 
	public boolean reserveCar(int id, int customerID, String location) throws RemoteException {
		Trace.info("RM::reserveCar(" + id + ", " + customerID + ", " + location + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String custStrData = CUSTOMER + ", " + customerID;
			String carStrData = CAR + ", " + location;

			Customer customer;
			ReservableItem car;

			if(tml.workingSet.containsKey(custStrData)) {
				customer = (Customer) tml.workingSet.get(custStrData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}
			if(tml.workingSet.containsKey(carStrData)) {
				car = (ReservableItem) tml.workingSet.get(carStrData);
			}
			else{
				car = (ReservableItem) rm_car.getRMEntry(id, location );
			}

			if(canReserveItem(customer, car) && lm.Lock(id,custStrData, WRITE) && lm.Lock(id, carStrData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for reserveCar operation");
				System.out.println("WRITE Lock granted to Tranx " + id + " CAR, " +  location + " resource for reserveCar operation");                

				if(tml.workingSet.containsKey(custStrData)) {
					customer = (Customer) tml.workingSet.get(custStrData);
				}
				else{
					customer = (Customer) readData( id, Customer.getKey(customerID) );
				}
				if(tml.workingSet.containsKey(carStrData)) {
					car = (ReservableItem) tml.workingSet.get(carStrData);
				}
				else{
					car = (ReservableItem) rm_car.getRMEntry(id, location );
				}

				if(!canReserveItem(customer, car)) {
					return false;
				}
				// make edits so that you write the updated versions in working set
				customer.reserve( car.getKey(), car.getLocation(), car.getPrice());        
	
				// decrease the number of available items in the storage
				car.setCount(car.getCount() - 1);
				car.setReserved(car.getReserved()+1);

				tml.workingSet.put(custStrData, customer);
				tml.workingSet.put(carStrData, car);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(customerID);
				newObj.setLocation(location);
				newObj.setClassName("Car");
				newObj.setMethodName("reservecar");

				tml.transactionLogItem.operationsInTransaction.add(newObj);
				
				return true;
				//return rm_car.reserveCar(id,customerID,location);
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
	}


	// Adds room reservation to this customer. 
	public boolean reserveRoom(int id, int customerID, String location) throws RemoteException {
		Trace.info("RM::reserveRoom(" + id + ", " + customerID + ", " + location + ") called" );
		startTime = System.currentTimeMillis();
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String custStrData = CUSTOMER + ", " + customerID;
			String roomStrData = ROOM + ", " + location;

			Customer customer;
			ReservableItem room;

			if(tml.workingSet.containsKey(custStrData)) {
				customer = (Customer) tml.workingSet.get(custStrData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}
			if(tml.workingSet.containsKey(roomStrData)) {
				room = (ReservableItem) tml.workingSet.get(roomStrData);
			}
			else{
				room = (ReservableItem) rm_room.getRMEntry(id, location);
			}

			if(canReserveItem(customer, room) && lm.Lock(id,custStrData, WRITE) && lm.Lock(id, roomStrData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for reserveRoom operation");
				System.out.println("WRITE Lock granted to Tranx " + id + " ROOM, " +  location + " resource for reserveRoom operation");                

				if(tml.workingSet.containsKey(custStrData)) {
					customer = (Customer) tml.workingSet.get(custStrData);
				}
				else{
					customer = (Customer) readData( id, Customer.getKey(customerID) );
				}
				if(tml.workingSet.containsKey(roomStrData)) {
					room = (ReservableItem) tml.workingSet.get(roomStrData);
				}
				else{
					room = (ReservableItem) rm_room.getRMEntry(id, location);
				}
				if(!canReserveItem(customer, room)) {
					return false;
				}

				// make edits so that you write the updated versions in working set
				customer.reserve( room.getKey(), room.getLocation(), room.getPrice());        
				
				// decrease the number of available items in the storage
				room.setCount(room.getCount() - 1);
				room.setReserved(room.getReserved()+1);

				tml.workingSet.put(custStrData, customer);
				tml.workingSet.put(roomStrData, room);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(customerID);
				newObj.setLocation(location);
				newObj.setClassName("Room");
				newObj.setMethodName("reserveroom");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
				//return rm_room.reserveRoom(id,customerID,location);
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
	}
	// Adds flight reservation to this customer.  
	public boolean reserveFlight(int id, int customerID, int flightNum) throws RemoteException {
		Trace.info("RM::reserveFlight(" + id + ", " + customerID + ", " + flightNum + ") called" );
		startTime = System.currentTimeMillis();
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}

		try {
			String custStrData = CUSTOMER + ", " + customerID;
			String flightStrData = FLIGHT + ", " + flightNum;

			Customer customer;
			ReservableItem flight;

			if(tml.workingSet.containsKey(custStrData)) {
				customer = (Customer) tml.workingSet.get(custStrData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}
			if(tml.workingSet.containsKey(flightStrData)) {
				flight = (ReservableItem) tml.workingSet.get(flightStrData);
			}
			else{
				flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
			}

			if(canReserveItem(customer, flight) && lm.Lock(id,custStrData, WRITE) && lm.Lock(id, flightStrData, WRITE)){
				System.out.println("WRITE Lock granted to Tranx " + id + " CUSTOMER, " +  customerID + " resource for reserveFlight operation");
				System.out.println("WRITE Lock granted to Tranx " + id + " FLIGHT, " +  flightNum + " resource for reserveFlight operation");                

				if(tml.workingSet.containsKey(custStrData)) {
					customer = (Customer) tml.workingSet.get(custStrData);
				}
				else{
					customer = (Customer) readData( id, Customer.getKey(customerID) );
				}
				if(tml.workingSet.containsKey(flightStrData)) {
					flight = (ReservableItem) tml.workingSet.get(flightStrData);
				}
				else{
					flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
				}
				if(!canReserveItem(customer, flight)) {
					return false;
				}
				// make edits so that you write the updated versions in working set
				customer.reserve( flight.getKey(), flight.getLocation(), flight.getPrice());        
				
				// decrease the number of available items in the storage
				flight.setCount(flight.getCount() - 1);
				flight.setReserved(flight.getReserved()+1);

				tml.workingSet.put(custStrData, customer);
				tml.workingSet.put(flightStrData, flight);

				ObjectParam newObj= new ObjectParam();
				newObj.setId(id);
				newObj.setCid(customerID);
				newObj.addFlightNum(flightNum);
				newObj.setClassName("Flight");
				newObj.setMethodName("reserveflight");

				tml.transactionLogItem.operationsInTransaction.add(newObj);

				return true;
				//return rm_flight.reserveFlight(id,customerID,flightNum);
			}
		} catch (DeadlockException e) {
			System.out.println(e);
			abort(id);
		}
		return false;
	}

	// Reserve an itinerary 
	@SuppressWarnings("null")
	public boolean itinerary(int id, int customerID, Vector flightNumbers, String location, boolean Car, boolean Room) throws RemoteException {
		Trace.info("itinerary(" + id + ", " + customerID +  ", FlightNums = " + Arrays.toString(flightNumbers.toArray()) +  ", " + location + ", Car = " + Car +  ", Room = " + Room + ") called" );
		TransactionManager tml= TransactionManager.getTransactionById(id);

		try{
			if (tml==null){
				throw new InvalidTransactionException("Transaction id is not present in Active Tranaction List");
			}
			else{
				//set reset time to live for the Transaction
				System.out.println("Transaction found in the activeTransactionList");
				tml.setTimeToLive(new Date());
			}
		} catch(InvalidTransactionException ex){
			System.out.println(ex.toString());
			return false;
		}
		try{
			if (flightNumbers == null || flightNumbers.isEmpty()){
				System.out.println("Flight numbers can not be null or empty");
				return false;
			}
			String customerStrData = CUSTOMER + ", " + customerID;
			Customer customer;
			
			if(tml.workingSet.containsKey(customerStrData)) {
				customer = (Customer) tml.workingSet.get(customerStrData);
			}
			else{
				customer = (Customer) readData( id, Customer.getKey(customerID) );
			}

			// first check if all of them are possible. if not, return false.
			for(int i=0;i<flightNumbers.size();i++){
				int flightNum=Integer.parseInt(flightNumbers.elementAt(i).toString());
				String flightStrData = FLIGHT + ", " + flightNum;
				ReservableItem flight;
				
				if(tml.workingSet.containsKey(flightStrData)) {
					flight = (ReservableItem) tml.workingSet.get(flightStrData);
				}
				else{
					flight = (ReservableItem) rm_flight.getRMEntry(id, Integer.toString(flightNum));
				}

				if(!canReserveItem(customer, flight)) {
					return false;
				}      	  
			}

			if (Car){
				String carStrData = CAR + ", " + location;
				ReservableItem car;
				
				if(tml.workingSet.containsKey(carStrData)) {
					car = (ReservableItem) tml.workingSet.get(carStrData);
				}
				else{
					car = (ReservableItem) rm_car.getRMEntry(id, location );
				}

				if(!canReserveItem(customer, car)) {
					return false;
				}
			}
			if(Room){
				String roomStrData = ROOM + ", " + location;
				ReservableItem room;
				
				if(tml.workingSet.containsKey(roomStrData)) {
					room = (ReservableItem) tml.workingSet.get(roomStrData);
				}
				else{
					room = (ReservableItem) rm_room.getRMEntry(id, location );
				}
				if(!canReserveItem(customer, room)) {
					return false;
				}
			}

			for(int i=0;i<flightNumbers.size();i++){
				int flightNum=Integer.parseInt(flightNumbers.elementAt(i).toString());
				String flightStrData = FLIGHT + ", " + flightNum;

				reserveFlight(id, customerID, flightNum);
			}
			if (Car){
				reserveCar(id, customerID, location);
			}
			if(Room){
				reserveRoom(id, customerID, location);
			}
			return true;
		} catch(Exception e){
			System.out.println("EXCEPTION:");
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		Trace.info("Middelware::itinerary(" + id + ", " + customerID + ","+location+") request completed, Reserved" );

		return true;
	}

	@Override
	public int start() throws RemoteException {
		//call to Transaction Manager Start method and return the TransId
		System.out.println("In Middleware start()");

		TransactionManager tm= new TransactionManager();
		tm.start();

		return tm.getXid() ;
	}

	@Override
	public boolean commit(int transactionId) throws RemoteException {
		try{
			Boolean result = TransactionManager.commit(transactionId);
			DBCopyObject dbCopy = new DBCopyObject(transactionId, m_itemHT);
			performShadowPaging(dbCopy);
			return result;
		}catch(TransactionAbortedException t ){
			System.out.println("TransactionAbortedException" +t);
			abort(transactionId);
		}catch(InvalidTransactionException i){
			System.out.println("InvalidTransactionException"+i);
		}
		return false;
	}

	@Override
	public boolean shutdown() throws RemoteException {
		if (TransactionManager.getActiveTransMap()==null || TransactionManager.getActiveTransMap().isEmpty()){
			try{
				rm_car.shutdown();
				rm_flight.shutdown();
				rm_room.shutdown();
				System.out.println("RM servers shutdown successfull");
			} catch(Exception e){
				System.out.println("Error in middlware shutdown");
			}
			try {
				// registry.unbind("PG7MiddlewareServer");
				UnicastRemoteObject.unexportObject(instance, true);
			} catch (Exception e) {
				throw new RemoteException("Could not unregister service, quiting anyway", e);
			}

			System.out.println("Sutdown successfull in middleware, returning now");
			//System.exit(0);
			return true;
		}

		System.out.println("could not shutdown, one or more active transactions exist");
		return false;
	}

	@Override
	public boolean abort(int transactionId) throws RemoteException{
		try {
			TransactionManager.abort(transactionId);
		} catch (InvalidTransactionException e) {
			System.out.println("InvalidTransactionException"+e);

			DBCopyObject recoveredDBCopy = recoverFromMasterFile();
			this.m_itemHT = recoveredDBCopy.itemsInDBCopy;

			if(TransactionManager.numTransaction < recoveredDBCopy.currentTransactionID) {
				TransactionManager.numTransaction = recoveredDBCopy.currentTransactionID;
			}

			return false;
		}

		DBCopyObject recoveredDBCopy = recoverFromMasterFile();
		this.m_itemHT = recoveredDBCopy.itemsInDBCopy;

		if(TransactionManager.numTransaction < recoveredDBCopy.currentTransactionID) {
			TransactionManager.numTransaction = recoveredDBCopy.currentTransactionID;
		}
		
		return true;
	}

	public boolean crash(String which) throws RemoteException {
		switch(which.toLowerCase()) {
			case "car":
				rm_car.selfDestruct();
				break;
			case "room":
				rm_room.selfDestruct();
				break;
			case "flight":
				rm_flight.selfDestruct();
				break;
			case "middleware":
				selfDestruct();
				break;
			default:
				break;
		}
		return true;
	}

	public void initTwoPhaseCommitRecovery() {
		TransactionManager.initTwoPhaseCommitRecovery();
	}

    public boolean twoPhaseCommit(int transactionId) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		return TransactionManager.twoPhaseCommit(transactionId);
	}

	@Override
    public boolean selfDestruct() throws RemoteException {
		System.exit(1);
		return false;
	}
	
	@Override
    public TransactionLogItem transfer2PCMessage(TransactionLogItem receivedLogItem) throws RemoteException {
        return TransactionManager.transfer2PCMessage(receivedLogItem);
    }

}