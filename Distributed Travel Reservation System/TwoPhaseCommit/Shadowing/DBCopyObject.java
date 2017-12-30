package TwoPhaseCommit.Shadowing;

import common.*;
import java.io.*;

public class DBCopyObject implements Serializable {

    public int currentTransactionID;
    public RMHashtable itemsInDBCopy;

    public DBCopyObject() {
    }
    
    public DBCopyObject(int transactionID, RMHashtable items) {
        currentTransactionID = transactionID;
        itemsInDBCopy = items;
    }
}