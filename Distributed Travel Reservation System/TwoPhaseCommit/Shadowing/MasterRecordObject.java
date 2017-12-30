package TwoPhaseCommit.Shadowing;

import java.io.*;

public class MasterRecordObject implements Serializable {

    public int lastValidTransactionID;
    public String lastCommitedVersionFileName;

    public MasterRecordObject() {
    }

    public MasterRecordObject(int transactionID, String fileName) {
        lastValidTransactionID = transactionID;
        lastCommitedVersionFileName = fileName;
    }
}