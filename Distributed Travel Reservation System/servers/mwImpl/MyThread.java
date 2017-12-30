package servers.mwImpl;

import java.rmi.RemoteException;
import java.util.Date;

import servers.mwImpl.lockManager.*;
//create a thread for checking the time to Live. //reset the time to leave each operation of the transaction
public class MyThread extends Thread{


	public MyThread(){

	}

	public void run () {
		
		while (true){

			if(null!=TransactionManager.getActiveTransMap()){
				
				//iterate over the actve trasnaction objects and check for time to live expiery
				for(TransactionManager tm:TransactionManager.getActiveTransMap().values()){

					long timeDiff=  (new Date().getTime() - tm.getTimeToLive().getTime() )/ (60 * 1000);
					//calculate time difference in Minute

					System.out.println("timeDiff "+ timeDiff +"for TranxId"+tm.getXid());
					//if the time diference is greater than 5 minute then abort the transaction
					
					if (timeDiff>5){
						try {
							System.out.println("time to live is expired for transaction id "+tm.getXid());

							TransactionManager.abort(tm.getXid());
							
						} catch (RemoteException | InvalidTransactionException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							
						}
					}
					try {
						this.sleep (20000);
					}
					catch (InterruptedException e) { }

				}

			}
		}
	}
}