package servers.mwImpl;

import ResInterface.*;
import common.*;
import servers.mwImpl.lockManager.*;

import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.Set;
import java.util.Scanner;
import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.function.Predicate;

import object.ObjectParam;

import TwoPhaseCommit.*;

public class TransactionManager {

	public int xid;
	public static ConcurrentMap <Integer,TransactionManager> activeTransMap= null;
	private Date timeToLive=null;
	public static int numTransaction=0;

	public static final int NUM_RMS = 3;

	public RMHashtable workingSet;

	private static long startTime, endTime, runningTime = 0;

	public TransactionLogItem transactionLogItem;
	public TransactionState[] participantResponses;

	private static final String COORDINATORLOG_FILENAME = "coordinatorLog.txt";
	public static final long RESPONSETIMEOUT = 30000;
	private static final Boolean ASKFORCRASH = false;

	public TransactionManager(){
		timeToLive= new Date();
		workingSet = new RMHashtable();
	}
	
	public TransactionManager(int xid, Date timeToLive){
		this.xid= xid;
		this.timeToLive= timeToLive;
		workingSet = new RMHashtable();
	}

	public int getXid(){		
		return this.xid;	
	}
	
	public void setXid(int xid){
		this.xid=xid;
	}
	
	public static ConcurrentMap<Integer, TransactionManager> getActiveTransMap() {
		return activeTransMap;
	}

	public static void setActiveTransMap(ConcurrentMap<Integer, TransactionManager> activeTransList) {
		TransactionManager.activeTransMap = activeTransList;
	}

	public Date getTimeToLive() {
		return timeToLive;
	}

	public void setTimeToLive(Date timeToLive) {
		this.timeToLive = timeToLive;
	}

	public int start() throws RemoteException {
		numTransaction+=1;
		//TransactionManager tm = new TransactionManager(numTransaction, new Date());
		this.setXid(numTransaction);
		
		if (TransactionManager.activeTransMap==null)
		{
			TransactionManager.setActiveTransMap(new ConcurrentHashMap <Integer,TransactionManager>());
		}
		
		TransactionManager.getActiveTransMap().put(Integer.valueOf(this.getXid()), this);
		System.out.println("Transaction , Xid"+this.getXid() +"added to the activeTransaList "+getActiveTransMap().size() );
		
		this.transactionLogItem = new TransactionLogItem(xid);
	
		return this.getXid();	
	}

	public static boolean commit(int transactionId) throws RemoteException,
		TransactionAbortedException, InvalidTransactionException {
		// TODO Auto-generated method stub
		//check transactions in active list, if not then throw InvalidTransactionException
		LockManager lm = new LockManager ();
		TransactionManager tm= getTransactionById(transactionId);
		
		if (tm==null){
			throw new InvalidTransactionException("InvalidTransactionException");
		}

		tm.transactionLogItem.workingSet = tm.workingSet;
		
		System.out.println("Committing transaction with id = " + transactionId);

		if(twoPhaseCommit(transactionId)) {
			lm.UnlockAll(tm.getXid());
			//tm.transactionLogItem.operationsInTransaction.clear();
			//tm.transactionLogItem.workingSet.clear();
			TransactionManager.activeTransMap.remove(Integer.valueOf(tm.getXid()));
	
			System.out.println("Two Phase Commit successfull, Tranaction removed from the list "+transactionId);
			return true;
		}
		else{
			return false;
		}
	}

	public boolean shutdown() throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}
	
	public static boolean abort(int transactionId) throws RemoteException,
			InvalidTransactionException {
		// TODO Auto-generated method stub
		LockManager lm = new LockManager ();
		TransactionManager tm= getTransactionById(transactionId);
		
		if (tm==null){
			throw new InvalidTransactionException("InvalidTransactionException");
		}
		System.out.println("Aborting transaction with id = " + transactionId);
		
		//remove the tranasction from the activeTransMap
		lm.UnlockAll(transactionId);
		//tm.transactionLogItem.operationsInTransaction.clear();
		//tm.transactionLogItem.workingSet.clear();
		//System.out.println("objList is cleared, removing the transObj from activeTransMap");
		TransactionManager.activeTransMap.remove(Integer.valueOf(tm.getXid()));

		return true;
	}

	// Algorithm:
	// add a method like receiveVoteRequest(ArrayList<ObjParam> operationsInTransaction) to ResourceManager interface
	// send a filtered version of operationsInTransaction to each RM. so that means dont send anything to an RM if
	// there are no operations related to it in that transaction. This will prevent flooding, make things more efficient

	// at each RM, traverse the list sent to you, check if each operation is possible to be performed. if yes, return vote-req YES
	// else, at the first inoperable request, send no

	// use a while loop (and a timeout counter) to block until you receive all votes from RMs that are voting
	// if timeout, return false
	// else, either call commit() or better yet, return true so that you can make this a part of commit()
	// commit() only proceeds if the return value is true. else, it aborts.
	public static boolean twoPhaseCommit(int transactionId) throws RemoteException, 
		TransactionAbortedException, InvalidTransactionException {
		System.out.println("\nSTART OF TWO PHASE COMMIT:");
		// If exists, find and append the coordinator log file. If not, create it
		TransactionManager tm= getTransactionById(transactionId);
		tm.participantResponses = new TransactionState[NUM_RMS + 1];

		rewriteTransactionLogItem(tm.transactionLogItem);

		// clear log item's working set to reduce overhead since you dont need to send it at this point?
		// tm.transactionLogItem.workingSet.clear();

		TransactionLogItem carRMTransactionLogItem = filterLogItemForRM(tm.transactionLogItem, "car");
		TransactionLogItem roomRMTransactionLogItem = filterLogItemForRM(tm.transactionLogItem, "room");
		TransactionLogItem flightRMTransactionLogItem = filterLogItemForRM(tm.transactionLogItem, "flight");
		
		TransactionLogItem middlewareTransactionLogItem = filterLogItemForRM(tm.transactionLogItem, "middleware");

		askToEnforceCrash("Crash middleware before sending vote request?", "middleware");
		// customers are a unique case. Middleware can just decide if they can or cannot be created by itself.
		// No need to ask RMs.
		//
		// if there are newcustomer/deletecustomer operations, ask middleware if valid first.
		if(middlewareTransactionLogItem.operationsInTransaction.size() != 0) {
			tm.participantResponses[3] = TransactionState.WAITING_VOTEREQ;
			middlewareTransactionLogItem.setTransactionState(TransactionState.SEND_VOTEREQ);
			middlewareTransactionLogItem = transfer2PCMessage(middlewareTransactionLogItem);

			// if not valid, simply cancel this transaction. No need to send anyone anything, since no one voted.
			// abort and log the transaction as ABORTED
			if(middlewareTransactionLogItem.currentState.equals(TransactionState.VOTE_NO)) {
				System.out.println("MIDDLEWARE DECIDED THAT CUSTOMER OPERATIONS CANNOT BE PERFORMED! ABORTING... (No need to send anything, no participant voted)");
				tm.transactionLogItem.setTransactionState(TransactionState.ABORTED);
				tm.transactionLogItem.endOfTransaction = true;
				rewriteTransactionLogItem(tm.transactionLogItem);
				abort(transactionId);
				return false;
			}
		}

		tm.transactionLogItem.setTransactionState(TransactionState.WAITING_VOTES);
		rewriteTransactionLogItem(tm.transactionLogItem);

		Future<TransactionLogItem> carRMResponseTask = null;
		Future<TransactionLogItem> roomRMResponseTask = null;
		Future<TransactionLogItem> flightRMResponseTask = null;

		TransactionLogItem carRMResponse = null;
		TransactionLogItem roomRMResponse = null;
		TransactionLogItem flightRMResponse = null;

		ExecutorService executor = Executors.newFixedThreadPool(3);

		// if we determined that there are car RM operations within the transaction, start a 
		// concurrent thread with the executorasking for its response, 
		if(carRMTransactionLogItem.operationsInTransaction.size() != 0) {
			tm.participantResponses[0] = TransactionState.WAITING_VOTEREQ;
			carRMTransactionLogItem.setTransactionState(TransactionState.SEND_VOTEREQ);
			carRMResponseTask = executor.submit(() -> {
				return MiddlewareServerImpl.rm_car.transfer2PCMessage(carRMTransactionLogItem);
			});
		}
		// similarly for room RM and flight RM
		if(roomRMTransactionLogItem.operationsInTransaction.size() != 0) {
			tm.participantResponses[1] = TransactionState.WAITING_VOTEREQ;
			roomRMTransactionLogItem.setTransactionState(TransactionState.SEND_VOTEREQ);
			roomRMResponseTask = executor.submit(() -> {
				return MiddlewareServerImpl.rm_room.transfer2PCMessage(roomRMTransactionLogItem);
			});
		}
		if(flightRMTransactionLogItem.operationsInTransaction.size() != 0) {
			tm.participantResponses[2] = TransactionState.WAITING_VOTEREQ;
			flightRMTransactionLogItem.setTransactionState(TransactionState.SEND_VOTEREQ);
			flightRMResponseTask = executor.submit(() -> {
				return MiddlewareServerImpl.rm_flight.transfer2PCMessage(flightRMTransactionLogItem);
			});
		}

		tm.transactionLogItem.setTransactionState(TransactionState.WAITING_VOTES);
		askToEnforceCrash("Crash middleware after sending vote request and before receiving any replies?", "middleware");		

		long startTime, currentTime;
		startTime = System.currentTimeMillis();

		boolean askedToCrash = false;

		// loop until either timeout, or all 3 are either null or a response is received
		// BE CAREFUL ABOUT LOOP CONDITION
		while(!((carRMResponseTask == null || carRMResponseTask.isDone()) && (roomRMResponseTask == null || roomRMResponseTask.isDone()) && (flightRMResponseTask == null || flightRMResponseTask.isDone()))) {
			currentTime = System.currentTimeMillis();

			if(!askedToCrash) {
				boolean[] responsesArrived = new boolean[3];
				int responsesCount = 0;
	
				if(carRMResponseTask != null) {
					responsesArrived[0] = carRMResponseTask.isDone();
				}
				if(roomRMResponseTask != null) {
					responsesArrived[1] = roomRMResponseTask.isDone();
				}
				if(flightRMResponseTask != null) {
					responsesArrived[2] = flightRMResponseTask.isDone();
				}
	
				for(int i = 0; i < responsesArrived.length; i++) {
					if(responsesArrived[i] == true) {
						responsesCount++;
					}
				}
				System.out.println("CARRM_RESPONDED = " + responsesArrived[0] + ", ROOMRM_RESPONDED = " + responsesArrived[1] + ", FLIGHTRM_RESPONDED = " + responsesArrived[2]);
	
				if(responsesCount > 0 && responsesCount < responsesArrived.length) {
					askedToCrash = true;
					askToEnforceCrash("Crash middleware after receiving some replies but not all?", "middleware");					
				}
			}

			if(currentTime - startTime > RESPONSETIMEOUT) {
				System.out.println("TIMEOUT OBSERVED");
				break;
			}
		}

		askToEnforceCrash("Crash middleware after receiving all replies but before deciding?", "middleware");

		try{
			// if car response task is neither null nor done, we havent received an answer within
			// the designated timeout threshold. that means we need to abort
			if(!(carRMResponseTask == null || carRMResponseTask.isDone())) {
				System.out.println("Car RM response expected but not received");
			}
			// we received an answer, so get it and store it in carRMResponse
			else if(carRMResponseTask != null) {
				carRMResponse = carRMResponseTask.get();
				System.out.println("Car RM's response VOTE = " + carRMResponse.currentState);

				tm.participantResponses[0] = carRMResponse.currentState;
			}
			else{
				System.out.println("Car RM is not accessed for this transaction");
			}

			// similarly for room RM and flight RM
			if(!(roomRMResponseTask == null || roomRMResponseTask.isDone())) {
				System.out.println("Room RM response expected but not received");
			}
			else if(roomRMResponseTask != null) {
				roomRMResponse = roomRMResponseTask.get();
				
				System.out.println("Room RM's response VOTE = " + roomRMResponse.currentState);

				tm.participantResponses[1] = roomRMResponse.currentState;
			}
			else{
				System.out.println("Room RM is not accessed for this transaction");
			}

			if(!(flightRMResponseTask == null || flightRMResponseTask.isDone())) {
				System.out.println("Flight RM response expected but not received");
			}
			else if(flightRMResponseTask != null) {
				flightRMResponse = flightRMResponseTask.get();
				
				System.out.println("Flight RM's response VOTE = " + flightRMResponse.currentState);

				tm.participantResponses[2] = flightRMResponse.currentState;
			}
			else{
				System.out.println("Flight RM is not accessed for this transaction");
			}

			if(middlewareTransactionLogItem.operationsInTransaction.size() != 0) {
				tm.participantResponses[3] = middlewareTransactionLogItem.currentState;
			}
		}
		catch(ExecutionException | InterruptedException e) {
		}

		boolean abort = false;
		// check if we will abort or not based on the responses received
		for(int i = 0; i < tm.participantResponses.length; i++) {
			if(tm.participantResponses[i] != null && !tm.participantResponses[i].equals(TransactionState.VOTE_YES)) {
				System.out.println("Participant[" + i + "] has response state = " + tm.participantResponses[i] + ". ABORTING");
				tm.transactionLogItem.setTransactionState(TransactionState.SEND_ABORT);
				abort = true;
			}
			System.out.println("Participant[" + i + "] has response state = " + tm.participantResponses[i]);				
		}


		if(abort) {
			tm.transactionLogItem.setTransactionState(TransactionState.ABORTED);
			rewriteTransactionLogItem(tm.transactionLogItem);

			askToEnforceCrash("Crash middleware after deciding but before sending decision?", "middleware");

			System.out.println("Sending SEND_ABORT to all yes voters!");
			tm.transactionLogItem.setTransactionState(TransactionState.SEND_ABORT);

			sendResponseToAllParticipants(tm.transactionLogItem);

			tm.transactionLogItem.setTransactionState(TransactionState.ABORTED);
			abort(transactionId);
			return false;
		}
		else{
			tm.transactionLogItem.setTransactionState(TransactionState.COMMITTED);
			rewriteTransactionLogItem(tm.transactionLogItem);

			askToEnforceCrash("Crash middleware after deciding but before sending decision?", "middleware");

			System.out.println("All participants have voted yes! Sending SEND_COMMIT to all participants");
			tm.transactionLogItem.setTransactionState(TransactionState.SEND_COMMIT);

			sendResponseToAllParticipants(tm.transactionLogItem);

			tm.transactionLogItem.setTransactionState(TransactionState.COMMITTED);
			return true;	
		}
	}

	public static void initTwoPhaseCommitRecovery() {
        File coordinatorLogFile = new File(MiddlewareServerImpl.instance.LOGDIRECTORY +  COORDINATORLOG_FILENAME);

        if(coordinatorLogFile.isFile()) {
			System.out.println("RECOVERING FROM PARTICIPATOR LOG FILE");
			System.out.println("Last recovered transaction Xid = " + numTransaction);
            // RECOVER!!!
            ArrayList<TransactionLogItem> transactionLogItems = returnLoggedTransactionItems();
			TransactionLogItem interruptedTransaction = null;

			if(transactionLogItems.size() == 0) {
				return;
			}
			else{
				interruptedTransaction = transactionLogItems.get(transactionLogItems.size() - 1);
			}

			System.out.println("Interrupted transaction had id: " + interruptedTransaction.transactionID);

			if (TransactionManager.activeTransMap==null)
			{
				TransactionManager.setActiveTransMap(new ConcurrentHashMap <Integer,TransactionManager>());
			}
			
			TransactionManager.getActiveTransMap().put(Integer.valueOf(interruptedTransaction.transactionID), new TransactionManager());

			ExecutorService executor = null;

			TransactionManager tm= getTransactionById(interruptedTransaction.transactionID);
			tm.transactionLogItem = interruptedTransaction;

			try{
				switch(interruptedTransaction.currentState) {
					case INITIAL:
						System.out.println("Sending SEND_ABORT to all yes voters!");
						tm.transactionLogItem.setTransactionState(TransactionState.SEND_ABORT);
		
						sendResponseToAllParticipants(tm.transactionLogItem);
			
						tm.transactionLogItem.setTransactionState(TransactionState.ABORTED);
						abort(interruptedTransaction.transactionID);
						rewriteTransactionLogItem(tm.transactionLogItem);
						break;
					case WAITING_VOTES:
						twoPhaseCommit(tm.transactionLogItem.transactionID);
						break;
					case COMMITTED:
						System.out.println("Sending SEND_COMMIT to all yes voters!");
						tm.transactionLogItem.setTransactionState(TransactionState.SEND_COMMIT);
		
						sendResponseToAllParticipants(tm.transactionLogItem);
			
						tm.transactionLogItem.setTransactionState(TransactionState.COMMITTED);
						break;
					case ABORTED:
						System.out.println("Sending SEND_ABORT to all yes voters!");
						tm.transactionLogItem.setTransactionState(TransactionState.SEND_ABORT);
		
						sendResponseToAllParticipants(tm.transactionLogItem);
			
						tm.transactionLogItem.setTransactionState(TransactionState.ABORTED);
						break;
					default:
						break;
				}
			}
			catch(Exception e) {
			}
			finally {
				numTransaction = interruptedTransaction.transactionID;
			}		
        }
        else{
            try{
                coordinatorLogFile.createNewFile();
            }
            catch(IOException e) {
            }
        }
    }
		
	public static TransactionManager getTransactionById(int xid){
		if(TransactionManager.activeTransMap!=null || !(TransactionManager.activeTransMap.isEmpty()) ){
			if (TransactionManager.activeTransMap.containsKey(Integer.valueOf(xid))){
				System.out.println("transaction found in activeTransMap "+ xid);
				return TransactionManager.activeTransMap.get(Integer.valueOf(xid));
			}
		}
			 
		return null;
	}

	// ASK MIDDLEWARE IF OPERATION IS POSSIBLE, SET TRANSACTION STATE TO EITHER VOTE_YES OR VOTE_NO
	// ACCORDING TO IF THE OPERATIONS ARE CORRECT (ex: if querycustomer on non existent customer, or new with already existant id, VOTE_NO)
    public static TransactionLogItem transfer2PCMessage(TransactionLogItem receivedLogItem) throws RemoteException {
		System.out.println("Middleware Server received log item with xid = " + receivedLogItem.transactionID + " and state = " + receivedLogItem.currentState + " and sender name = " + receivedLogItem.senderServerName);
		
		TransactionManager tm= getTransactionById(receivedLogItem.transactionID);
        TransactionLogItem response = new TransactionLogItem(receivedLogItem);
		
		try{
			switch(receivedLogItem.currentState) {
				case SEND_VOTEREQ:
					for(int i = 0; i < response.operationsInTransaction.size(); i++) {
						ObjectParam operation = response.operationsInTransaction.get(i);
						Boolean canBeExecuted = true;
	
						switch(operation.getMethodName().toLowerCase()){
							case "newcustomer":
								if(operation.getCid() == -1){
									canBeExecuted = true;
								}
								else{
									canBeExecuted = true;
								}
								break;
							case "deletecustomer":
								canBeExecuted = true;
								break;
							default:
								break;
						}
	
						if(!canBeExecuted) {
							System.out.println("Middleware Server: Operation " + operation.getMethodName() + " for transaction # " + receivedLogItem.transactionID + " cannot be performed! Returning VOTE_NO");
							response.setTransactionState(TransactionState.VOTE_NO);
							return response;
						}
					}
	
					System.out.println("Middleware Server: All operations for transaction # " + receivedLogItem.transactionID + " are valid. Returning VOTE_YES");
					response.setTransactionState(TransactionState.VOTE_YES);
					return response;
				case SEND_COMMIT:
					if(!receivedLogItem.workingSet.isEmpty()) {
						Set<String> workingSetKeys = receivedLogItem.workingSet.keySet();
						for(String key : workingSetKeys) {
							Customer item = (Customer) receivedLogItem.workingSet.get(key);
							if(item != null) {
								// effectively addFlight (with quantity = final number after reservations & price = latest updated price)
								MiddlewareServerImpl.instance.writeRMEntry(receivedLogItem.transactionID, item.getKey(), item);
								System.out.println("Updating middleware RM hashtable entry with key = " + item.getKey());
							}
							else{
								// effectively deleteFlight
								MiddlewareServerImpl.instance.removeRMEntry(receivedLogItem.transactionID, item.getKey());
								System.out.println("REMOVING middleware RM hashtable entry with key = " + item.getKey());
							}
						}
					}
					break;
				case SEND_ABORT:
					break;
				case VOTE_YES:
					// An RM IS TRYING TO RECOVER! DECIDE WHAT HAPPENED FOR THE SENT TRANSACTION and send it to the RM again
					System.out.println("Re-sending participant the outcome of the transaction with ID = " + receivedLogItem.transactionID);
					TransactionLogItem outcomeLogItem = getLogItemWithID(receivedLogItem.transactionID);

					if(outcomeLogItem == null) {
						System.out.println("OUTCOME NOT FOUND...");
					}
	
					outcomeLogItem = filterLogItemForRM(outcomeLogItem, receivedLogItem.senderServerName);
	
					System.out.println("Outcome was transaction with id = " + outcomeLogItem.transactionID + 
					", numOperations = " + outcomeLogItem.operationsInTransaction.size() + ", workingSet.size = " + 
					outcomeLogItem.workingSet.size() + " and state = " + outcomeLogItem.currentState);
	
					if(outcomeLogItem.currentState.equals(TransactionState.COMMITTED)) {
						outcomeLogItem.setTransactionState(TransactionState.SEND_COMMIT);
					}
					else if(outcomeLogItem.currentState.equals(TransactionState.ABORTED)) {
						outcomeLogItem.setTransactionState(TransactionState.SEND_ABORT);
					}
	
					return outcomeLogItem;
				case ABORTED:
					// An RM was trying to recover but decided to abort! Abort transaction
					System.out.println("Received ABORTED from RM. (An RM was trying to recover but decided to abort)! Abort transaction");
					TransactionLogItem outcomeTransaction = getLogItemWithID(receivedLogItem.transactionID);
	
					if(outcomeTransaction == null) {
						System.out.println("OUTCOME NOT FOUND...");
					}

					System.out.println("Outcome was transaction with id = " + outcomeTransaction.transactionID + 
					", numOperations = " + outcomeTransaction.operationsInTransaction.size() + ", workingSet.size = " + 
					outcomeTransaction.workingSet.size() + " and state = " + outcomeTransaction.currentState);
					
					outcomeTransaction.setTransactionState(TransactionState.ABORTED);
					rewriteTransactionLogItem(outcomeTransaction);
		
					System.out.println("Sending SEND_ABORT to all yes voters!");
					outcomeTransaction.setTransactionState(TransactionState.SEND_ABORT);
	
					sendResponseToAllParticipants(outcomeTransaction);
		
					outcomeTransaction.setTransactionState(TransactionState.ABORTED);
					try{
						abort(outcomeTransaction.transactionID);
					}
					catch(Exception e) {
					}
					break;
				default:
					break;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
        return null;
	}
	
	private static TransactionLogItem getLogItemWithID(int transactionID) {
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

	private static TransactionLogItem getLogItemWithFilteredOperationsForRM(TransactionLogItem transactionLogItem, String rmName) {
		TransactionLogItem filteredLogItem = new TransactionLogItem(transactionLogItem);
		filteredLogItem.operationsInTransaction = filterClassNameByString(filteredLogItem.operationsInTransaction, rmName);
		return filteredLogItem;
	}

	private static TransactionLogItem filterLogItemForRM(TransactionLogItem transactionLogItem, String rmName) {
		System.out.println("Received rmName = " + rmName);
		TransactionLogItem filteredLogItem = getLogItemWithFilteredOperationsForRM(transactionLogItem, rmName);
		filteredLogItem.workingSet.clear();

		Set<String> workingSetKeys = transactionLogItem.workingSet.keySet();
		for(String key : workingSetKeys) {
			switch(rmName.toLowerCase()) {
				case "car":
					if(key.startsWith(MiddlewareServerImpl.CAR)) {
						ReservableItem item = (ReservableItem) transactionLogItem.workingSet.get(key);
						filteredLogItem.workingSet.put(key, item);
					}
					break;
				case "room":
					if(key.startsWith(MiddlewareServerImpl.ROOM)) {
						ReservableItem item = (ReservableItem) transactionLogItem.workingSet.get(key);						
						filteredLogItem.workingSet.put(key, item);
					}
					break;
				case "flight":
					if(key.startsWith(MiddlewareServerImpl.FLIGHT)) {
						ReservableItem item = (ReservableItem) transactionLogItem.workingSet.get(key);
						filteredLogItem.workingSet.put(key, item);
					}
					break;
				case "middleware":
					if(key.startsWith(MiddlewareServerImpl.CUSTOMER)) {
						Customer item = (Customer) transactionLogItem.workingSet.get(key);
						filteredLogItem.workingSet.put(key, item);
					}
					break;
				default:
					break;
			}
		}
		return filteredLogItem;
	}

	private static ArrayList<ObjectParam> filterClassNameByString(ArrayList<ObjectParam> operationsList, String rmName) {
		return (ArrayList<ObjectParam>)operationsList.stream().filter(objParam -> objParam.getClassName().toLowerCase().equals(rmName)).collect(Collectors.toList());
	} 

	private static ArrayList<TransactionLogItem> returnLoggedTransactionItems() {
		return setAndReturnTransactionLogItems(null, (logItem -> true), true);
	}

	// get all the objects in the log file, write only the ones that have endOfTransaction == false
	private static void garbageCollectOnLogRecords(int currentTransactionId) {
		setAndReturnTransactionLogItems(null, (logItem -> logItem.endOfTransaction == false || logItem.transactionID >= currentTransactionId - 2), false);
	}

	// remove all log items in the log with the same transactionId as the parameter. write it with its new state
	private static void rewriteTransactionLogItem(TransactionLogItem transactionLogItem) {
		System.out.println("Writing transaction with id: " + transactionLogItem.transactionID + " to coordinator log");		
		setAndReturnTransactionLogItems(transactionLogItem, (logItem -> logItem.transactionID != transactionLogItem.transactionID), false);
	}

	// remove all log items in the log with the same transactionId as the parameter. write it with its new state
	private static ArrayList<TransactionLogItem> setAndReturnTransactionLogItems(TransactionLogItem transactionLogItemToWrite, 
		Predicate<TransactionLogItem> predicateFunction, boolean readonly) {
		ArrayList<TransactionLogItem> transactionLogItems = new ArrayList<TransactionLogItem>();
		
		File coordinatorLogFile = new File(MiddlewareServerImpl.instance.LOGDIRECTORY +  COORDINATORLOG_FILENAME);
		
		FileOutputStream fos;
		ObjectOutputStream oos;

		FileInputStream fis;
		ObjectInputStream ois;

		if(coordinatorLogFile.isFile()) {
			if(coordinatorLogFile.length() != 0) {
				try {
					System.out.println("Coordinator log file is NOT empty.");
					fis = new FileInputStream(coordinatorLogFile);
					ois = new ObjectInputStream(fis);
	
					// store all the objects into the list until EOF is reached
					transactionLogItems = (ArrayList<TransactionLogItem>)ois.readObject();
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
				System.out.println("Coordinator log file is empty.");
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
					fos = new FileOutputStream(coordinatorLogFile);
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

	private static void sendResponseToAllYesVoters(TransactionLogItem message) {
		TransactionManager tm = getTransactionById(message.transactionID);

		TransactionLogItem carRMTransactionLogItem = filterLogItemForRM(message, "car");
		TransactionLogItem roomRMTransactionLogItem = filterLogItemForRM(message, "room");
		TransactionLogItem flightRMTransactionLogItem = filterLogItemForRM(message, "flight");			
		
		TransactionLogItem middlewareTransactionLogItem = filterLogItemForRM(message, "middleware");		
		
		ExecutorService executor = Executors.newFixedThreadPool(3);

		if(tm.participantResponses[3] != null && tm.participantResponses[3].equals(TransactionState.VOTE_YES)) {
			try{
				transfer2PCMessage(middlewareTransactionLogItem);
			}
			catch(RemoteException e) {
				e.printStackTrace();
			}
		}

		// send the message in a separate thread to each participant that voted yes
		// message could inlcude commit or abort
		if(tm.participantResponses[0] != null && tm.participantResponses[0].equals(TransactionState.VOTE_YES)) {
			executor.submit(() -> {
				return MiddlewareServerImpl.rm_car.transfer2PCMessage(carRMTransactionLogItem);
			});
		}
		if(tm.participantResponses[1] != null && tm.participantResponses[1].equals(TransactionState.VOTE_YES)) {
			executor.submit(() -> {
				return MiddlewareServerImpl.rm_room.transfer2PCMessage(roomRMTransactionLogItem);
			});
		}
		if(tm.participantResponses[2] != null && tm.participantResponses[2].equals(TransactionState.VOTE_YES)) {
			executor.submit(() -> {
				return MiddlewareServerImpl.rm_flight.transfer2PCMessage(flightRMTransactionLogItem);
			});
		}
	}

	private static void sendResponseToAllParticipants(TransactionLogItem message) {
		TransactionManager tm = getTransactionById(message.transactionID);

		TransactionLogItem carRMTransactionLogItem = filterLogItemForRM(message, "car");
		TransactionLogItem roomRMTransactionLogItem = filterLogItemForRM(message, "room");
		TransactionLogItem flightRMTransactionLogItem = filterLogItemForRM(message, "flight");			
		
		TransactionLogItem middlewareTransactionLogItem = filterLogItemForRM(message, "middleware");		
		
		ExecutorService executor = Executors.newFixedThreadPool(3);
		ExecutorService crashExecutor = Executors.newFixedThreadPool(2);

		try{
			transfer2PCMessage(middlewareTransactionLogItem);
		}
		catch(RemoteException e) {
			e.printStackTrace();
		}

		// send the message in a separate thread to each participant that voted yes
		// message could inlcude commit or abort
		executor.submit(() -> {
			return MiddlewareServerImpl.rm_car.transfer2PCMessage(carRMTransactionLogItem);
		});
		if(ASKFORCRASH) {
			crashExecutor.submit(() -> {
				askToEnforceCrash("Crash middleware after sending some but not all decisions", "middleware");
			});
		}
		executor.submit(() -> {
			return MiddlewareServerImpl.rm_room.transfer2PCMessage(roomRMTransactionLogItem);
		});
		executor.submit(() -> {
			return MiddlewareServerImpl.rm_flight.transfer2PCMessage(flightRMTransactionLogItem);
		});

		if(ASKFORCRASH) {
			crashExecutor.submit(() -> {
				askToEnforceCrash("Crash middleware after having sent all decisions?", "middleware");
			});
		}
	}

	public static void askToEnforceCrash(String messageToDisplay, String crashingServerName) {
		if(ASKFORCRASH){
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
				try{
					MiddlewareServerImpl.instance.crash(crashingServerName);
				}
				catch(Exception e) {
				}
			}
		}
	}

}



