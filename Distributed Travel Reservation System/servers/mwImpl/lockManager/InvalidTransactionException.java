package servers.mwImpl.lockManager;

public class InvalidTransactionException  extends Exception {
	
	protected String msg;
	
	
	public InvalidTransactionException(String Exmsg){
		super(Exmsg);
		
	}

}
