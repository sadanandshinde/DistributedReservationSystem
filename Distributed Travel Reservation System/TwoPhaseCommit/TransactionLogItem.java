package TwoPhaseCommit;

import java.io.*;
import common.*;

import object.ObjectParam;
import java.util.ArrayList;

public class TransactionLogItem implements Serializable {

    public int transactionID;
    public ArrayList<ObjectParam> operationsInTransaction = new ArrayList<ObjectParam>();
    public RMHashtable workingSet = new RMHashtable();
    public TransactionState currentState = TransactionState.INITIAL;
    public boolean endOfTransaction;
    public String senderServerName = "";

    public TransactionLogItem() {
    }

    public TransactionLogItem(int xid) {
        transactionID = xid;
    }

    public TransactionLogItem(int xid, String serverName) {
        transactionID = xid;
        senderServerName = serverName;
    }

    public TransactionLogItem(int xid, ArrayList<ObjectParam> operations) {
        transactionID = xid;
        operationsInTransaction = (ArrayList<ObjectParam>)operations.clone();
    }

    public TransactionLogItem(int xid, ArrayList<ObjectParam> operations, RMHashtable workSet) {
        transactionID = xid;
        operationsInTransaction = (ArrayList<ObjectParam>)operations.clone();
        workingSet = (RMHashtable)workSet.clone();
    }

    public TransactionLogItem(TransactionLogItem copy) {
        this.transactionID = copy.transactionID;
        this.operationsInTransaction = (ArrayList<ObjectParam>)copy.operationsInTransaction.clone();
        this.workingSet = (RMHashtable)copy.workingSet.clone();
        this.currentState = copy.currentState;
        this.endOfTransaction = copy.endOfTransaction;
        this.senderServerName = new String(copy.senderServerName);
    }

    public void setTransactionState(TransactionState state) {
        this.currentState = state;
    }

}