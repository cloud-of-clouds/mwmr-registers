package mwmr.client;

import java.util.List;
import java.util.Queue;

import mwmr.client.messages.Response;
import mwmr.client.messages.ResponseType;
import mwmr.clouds.IKVSDriver;
import mwmr.exceptions.StorageCloudException;
import mwmr.operations.IOperation;
import mwmr.operations.ListMWMROperation;
import mwmr.operations.ReadMWMROperation;
import mwmr.operations.WriteMWMROperation;

public class ExecutionThread extends Thread {

	private IKVSDriver conn;
	private IOperation op;
	private boolean terminate;
	private boolean executingOp;
	private Queue<ExecutionThread> threads;
	private OperationResponses responses;
	private int executionIndex;


	public ExecutionThread(Queue<ExecutionThread> threads) {
		this.executingOp = false;
		this.terminate = false;
		this.threads = threads;
	}

	@Override
	public void run() {
		Response current;
		while(!terminate){
			current = null;
			synchronized (this) {
				threads.offer(this);
				executingOp = false;
				try {
					this.wait();
				} catch (InterruptedException e) { e.printStackTrace();	}
				if(terminate)
					break;
				executingOp=true;
			}
			switch(op.getOperationType()){
			case LISTMWMR:
				current = listMWMR();
				break;
			case READMWMR:
				current = readMWMR();
				break;
			case WRITEMWMR:
				current = writeMWMR();
				break;
			default:
				break;
			}
			responses.addResponse(current);
			conn=null;
		}
	}

	private Response listMWMR(){
		ListMWMROperation listOp = (ListMWMROperation) op;
		DataUnit du = listOp.getDataUnit();
		try {
			List<String> list = conn.listContainer(listOp.getPrefix(), du.getContainerName(), null);
			//			for(String s : list)
			//				System.out.println("index: " + executionIndex + "   key:" + s);
			return new Response(ResponseType.OK, list);
		} catch (StorageCloudException e) {
			//			e.printStackTrace();
		}
		return new Response(ResponseType.FAIL);
	}

	private Response readMWMR(){	
		ReadMWMROperation readOp = (ReadMWMROperation) op;
		DataUnit du = readOp.getDataUnit();

		try {
			byte[] data = conn.readObjectData(du.getContainerName(), readOp.getVersion(), null);
			if(data != null){
				return new Response(ResponseType.OK, data);
			}else
				return new Response(ResponseType.NOT_EXISTS);
		} catch (StorageCloudException e) {
			e.printStackTrace();
		}
		return new Response(ResponseType.FAIL);
	}

	private Response writeMWMR(){
		WriteMWMROperation writeOp = (WriteMWMROperation) op;
		DataUnit du = writeOp.getDataUnit();
		try {
			//			System.out.println("Conn: " + executionIndex + "   writing data: " + writeOp.getData(executionIndex).length + "  writing ver: " + writeOp.getVersion());
			conn.writeObject(du.getContainerName(), writeOp.getVersion(), writeOp.getData(executionIndex), null);
			return new Response(ResponseType.OK, AMwmrRegister.getConnectionFullId(conn));
		} catch (StorageCloudException e) {
			e.printStackTrace();
		}
		return new Response(ResponseType.FAIL);
	}


	public void terminateThread(){
		terminate=true;
		synchronized (this) {
			if(!executingOp){
				this.notify();
			}
		}
	}

	/**
	 * 
	 * @param conn
	 * @param op
	 * @param responses 
	 * @param responseIndex 
	 * @requires (conn!=null && op!=null);
	 */
	public void executeOp(IKVSDriver conn, IOperation op, int executionIndex, OperationResponses resp){
		synchronized (this) {
			if(!executingOp){
				this.conn = conn;
				this.op=op;
				this.responses = resp;
				this.executionIndex = executionIndex;
				this.notify();
			}
		}
	}

}
