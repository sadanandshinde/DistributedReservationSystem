package TwoPhaseCommit.Shadowing;

import java.io.*;

public abstract class ShadowPager {

    public String LOGDIRECTORY;
    protected final String MASTERRECORDFILENAME = "masterRecord.txt";
	protected final String DBCOPY_A_FILENAME = "dbCopyA.txt";
    protected final String DBCOPY_B_FILENAME = "dbCopyB.txt";
    protected final String PARTICIPATORLOG_FILENAME = "participatorLog.txt";

    protected static final long RESPONSETIMEOUT = 30000;
    
    protected String currentWorkingDBCopyFileName;

    protected DBCopyObject initShadowpaging(String serverFolderName) {
        LOGDIRECTORY = new File("").getAbsolutePath() + "/servers/" + serverFolderName + "/log/";
        File masterRecordFile = new File(LOGDIRECTORY +  MASTERRECORDFILENAME);

        if(masterRecordFile.isFile()) {
            System.out.println("RECOVERING FROM MASTER FILE");
            return recoverFromMasterFile();
        }
        else{
            File dbCopyAFile = new File(LOGDIRECTORY + DBCOPY_A_FILENAME);
            File dbCopyBFile = new File(LOGDIRECTORY + DBCOPY_B_FILENAME);

            try{
                System.out.println("No log files found, initiating master log, two DB copy log files at file path: " + LOGDIRECTORY);
                masterRecordFile.createNewFile();
                dbCopyAFile.createNewFile();
                dbCopyBFile.createNewFile();
            }
            catch(IOException e) {
            }

            currentWorkingDBCopyFileName = DBCOPY_A_FILENAME;

            return new DBCopyObject();
        }
    }

    protected DBCopyObject recoverFromMasterFile() {
        FileInputStream fis;
        ObjectInputStream ois;

        try{
            System.out.println("Recovering from master file, located at path: " + LOGDIRECTORY +  MASTERRECORDFILENAME);
            File masterRecordFile = new File(LOGDIRECTORY +  MASTERRECORDFILENAME);
            
            if(masterRecordFile.length() != 0) {
                System.out.println("Master file not empty!");

                fis = new FileInputStream(LOGDIRECTORY +  MASTERRECORDFILENAME);
                ois = new ObjectInputStream(fis);
    
                MasterRecordObject masterRecord = (MasterRecordObject) ois.readObject();
    
                if(masterRecord != null) {
                    System.out.println("Last stable DB at file: " + masterRecord.lastCommitedVersionFileName);
                    System.out.println("Last Valid XID IS = " + masterRecord.lastValidTransactionID);
                    if(masterRecord.lastCommitedVersionFileName.equals(DBCOPY_A_FILENAME)) {
                        fis = new FileInputStream(LOGDIRECTORY +  DBCOPY_A_FILENAME);
                    }
                    else if(masterRecord.lastCommitedVersionFileName.equals(DBCOPY_B_FILENAME)) {
                        fis = new FileInputStream(LOGDIRECTORY +  DBCOPY_B_FILENAME);
                    }
                    else{
                        return null;
                    }
    
                    ois = new ObjectInputStream(fis);
                    DBCopyObject dbCopy = (DBCopyObject) ois.readObject();
    
                    if(dbCopy != null) {
                        currentWorkingDBCopyFileName = masterRecord.lastCommitedVersionFileName;
                        System.out.println("SHADOWING SUCCESS");
                    }
                    else{
                        currentWorkingDBCopyFileName = DBCOPY_A_FILENAME;
                    }
    
                    ois.close();
                    fis.close();
        
                    return dbCopy;
                }
                else{
                    currentWorkingDBCopyFileName = DBCOPY_A_FILENAME;
                }
            }
            else{
                System.out.println("Master file is empty, returning");
                currentWorkingDBCopyFileName = DBCOPY_A_FILENAME;
            }
        }
        catch(ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    protected void performShadowPaging(DBCopyObject dbCopy) {
        try{
            if(currentWorkingDBCopyFileName == null) {
                currentWorkingDBCopyFileName = DBCOPY_A_FILENAME;
            }
            String filePath = LOGDIRECTORY + currentWorkingDBCopyFileName;
            System.out.println("Writing new stable database state at file = " + filePath);
            FileOutputStream fos = new FileOutputStream(filePath);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            oos.writeObject(dbCopy);
            oos.close();
        }
        catch(FileNotFoundException e) {
            e.printStackTrace();
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        MasterRecordObject masterRecord = new MasterRecordObject(dbCopy.currentTransactionID, currentWorkingDBCopyFileName);

        try{
            String filePath = LOGDIRECTORY + MASTERRECORDFILENAME;
            System.out.println("FILEPATH = " + filePath);
            FileOutputStream fos = new FileOutputStream(filePath);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            oos.writeObject(masterRecord);
            oos.close();
            System.out.println("CLOSED");
        }
        catch(FileNotFoundException e) {
            e.printStackTrace();
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        if(currentWorkingDBCopyFileName.equals(DBCOPY_A_FILENAME)) {
            currentWorkingDBCopyFileName = DBCOPY_B_FILENAME;
        }
        else{
            currentWorkingDBCopyFileName = DBCOPY_A_FILENAME;
        }
    }

}