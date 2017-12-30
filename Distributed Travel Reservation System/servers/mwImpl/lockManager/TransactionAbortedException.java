package servers.mwImpl.lockManager;

public class TransactionAbortedException extends Exception{
	protected int xid = 0;
	
	public TransactionAbortedException(int transId, String msg){
		
		super(msg);
		this.xid = transId;
	}
	
	public int getXId() {
		return this.xid;
	}

}
