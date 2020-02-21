package mwmr.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import exception.UnsuccessfulDecodeException;
import jec.ReedSolInfo;
import jec.ReedSolomon;
import mwmr.client.messages.Response;
import mwmr.clouds.IKVSDriver;
import mwmr.clouds.LocalCloudSimulator;
import mwmr.clouds.amazon.AmazonS3Driver;
import mwmr.clouds.google.GoogleStorageDriver;
import mwmr.clouds.rackspace.RackSpaceDriver;
import mwmr.clouds.softlayer.SoftLayerDriver;
import mwmr.exceptions.ClientServiceException;
import mwmr.exceptions.StorageCloudException;
import mwmr.operations.IOperation;
import mwmr.util.Constants;
import mwmr.util.integrity.IntegrityManager;

public abstract class AMwmrRegister {


	protected IKVSDriver [] connections;
	protected int clientId;

	protected final int N;
	protected final int F;
	protected final int EC_K;
	protected final int EC_M;

	protected final int READ_QUORUM;
	protected final int WRITE_QUORUM;
	protected final int LIST_QUORUM;

	private LinkedBlockingQueue<ExecutionThread> threads;
	protected Boolean destroyed;

	protected AMwmrRegister(int clientId, int numberOfThreads, String configFilePath, List<String[][]> accessCredentials, int n, int f, int ecK, int ecM) throws StorageCloudException {
		this.clientId = clientId;

		this.N = n;
		this.F = f;
		this.EC_K = ecK;
		this.EC_M  = ecM;

		this.READ_QUORUM = N-F;
		this.WRITE_QUORUM = N-F;
		this.LIST_QUORUM = N-F;

		//init connections
		connections = initConnections(accessCredentials, configFilePath);

		threads = new LinkedBlockingQueue<ExecutionThread>(numberOfThreads);
		for(int i=0 ; i < numberOfThreads ; i++ )
			new ExecutionThread(threads).start();

		this.destroyed = false;
	}


	/*************************************************************************************************************************/
	/********************************                     PUBLIC METHODS                    **********************************/
	/*************************************************************************************************************************/

	
	public abstract void write(DataUnit dataUnit, byte[] data ) throws StorageCloudException;
	
	public abstract byte[] read(DataUnit dataUnit) throws StorageCloudException;



	/*************************************************************************************************************************/
	/********************************                 METHOD IMPLEMENTATIONS                 **********************************/
	/**
	 * @throws StorageCloudException ***********************************************************************************************************************/
	
	protected String getMaxVer(Response[] respProcessed) throws StorageCloudException{
		// find the max version
		List<String> temp_list = null;
		int maxTs = 0;
		String key = "";
		int c_ID = -1;
		int cloud = 0, count = 0;
		while(count < 2){
			for(int i = 0; i < respProcessed.length; i++){
				if(respProcessed[i] != null){
					temp_list = respProcessed[i].getListNames();
					if(temp_list!=null && temp_list.size() > 0)
						for (String ver : temp_list) {
							if(ver.startsWith("pow")){
								ver = ver.substring(4, ver.length());
							}
							if(Integer.parseInt(ver.split(",")[0]) > maxTs){
								maxTs = Integer.parseInt(ver.split(",")[0]);
								c_ID = Integer.parseInt(ver.split(",")[1]);
								key = ver;
								cloud = i;
							}else if(Integer.parseInt(ver.split(",")[0]) == maxTs && Integer.parseInt(ver.split(",")[1]) > c_ID){
								maxTs = Integer.parseInt(ver.split(",")[0]);
								c_ID = Integer.parseInt(ver.split(",")[1]);
								key = ver;
								cloud = i;
							}
						}
				}
			}
			if(maxTs > 0){
				boolean verify = verifySig(key.split("-")[0], toByteArray(key.split("-")[1]));
				if(!verify && count == 1){
					throw new StorageCloudException("Full Replication Regular List: (list responses) two KVSs are byzantine");
				}else if(!verify){
					respProcessed[cloud] = null;
					maxTs = 0;
					count++;
				}else{
					count = 2;
				}
			}else{
				count = 2;
			}
		}
		return key;
	}

	protected byte[] getOriginalValue(List<byte[]> blocks, String hash, int dataSize) throws ClientServiceException{
		byte[][] b2 = new byte[2][];
		//		System.out.println("datasize: " + dataSize);
		for(int i = 0; i < blocks.size(); i++){
			for(int j = 0; j < blocks.size(); j++){
				if(j > i){
					ReedSolInfo ecInfo = new ReedSolInfo(dataSize, this.EC_K, this.EC_M, Constants.EC_W);
					try {
						//						System.out.println("BLOCK 1 -> " + blocks.get(i).length);
						//						System.out.println("BLOCK 2 -> " + blocks.get(j).length);
						b2[0] = blocks.get(i);
						b2[1] = blocks.get(j);
						byte[]result = ReedSolomon.decode(b2, ecInfo);
						if(hash.equals(IntegrityManager.getHexHash(result))){
							//							System.out.println("VALID VALUE");
							return result;
						}
					} catch (UnsuccessfulDecodeException e) {
						e.printStackTrace();
						throw new ClientServiceException("Read: Error Decoding (EC).");
					}
				}
			}
		}
		return null;
	}

	protected void removeKeyFromListResult(Response[] respProcessed, String key){
		//List<Response> result = new ArrayList<Response>();

		for(int i = 0; i < respProcessed.length; i++){
			if(respProcessed[i]==null || respProcessed[i].getListNames() == null)
				continue;

			if(respProcessed[i].getListNames().contains(key))
				respProcessed[i].getListNames().remove(key);
		}

		//return respProcessed;
	}


	public void garbageCollect(DataUnit dataUnit, int numVersionToKeep) throws ClientServiceException {
		if(destroyed)
			throw new ClientServiceException("DepSky Client already destroyed.");

		//TODO: IMPLEMENT
	}

	
	
	private IKVSDriver[] initConnections(List<String[][]> accessCredentials , String fileName) throws StorageCloudException {
		List<String[][]> credentials = accessCredentials;
		try {
			if(accessCredentials==null)
				credentials = readCredentials(fileName);

		} catch (FileNotFoundException e) {
			System.out.println("accounts.properties file dosen't exist!");
			e.printStackTrace();
		} catch (ParseException e) {
			System.out.println("accounts.properties misconfigured!");		
			e.printStackTrace();
		}

		List<IKVSDriver> drivers = new ArrayList<IKVSDriver>();

		String type = null, driverId = null, accessKey = null, secretKey = null;
		for(int i = 0 ; i < credentials.size(); i++){
			for(String[] pair : credentials.get(i)){
				if(pair[0].equalsIgnoreCase("driver.type")){
					type = pair[1];
				}else if(pair[0].equalsIgnoreCase("driver.id")){
					driverId = pair[1];
				}else if(pair[0].equalsIgnoreCase("accessKey")){
					accessKey = pair[1];
				}else if(pair[0].equalsIgnoreCase("secretKey")){
					secretKey = pair[1];
				}
			}
			drivers.add(getDriver(type, driverId, accessKey, secretKey));
		}

		if(drivers.size() == this.N)
			return drivers.toArray(new IKVSDriver[this.N]);

		if(drivers.size() < this.N)
			throw new ClientServiceException("Invalid access credentials.");

		if(drivers.size() > this.N)
			System.out.println("Warning: number of configured driver is greater than N.");

		IKVSDriver[] result = new IKVSDriver[this.N];
		for(int i=0 ; i<result.length ; i++)
			result[i] = drivers.get(i);

		return result;
	}


	/*************************************************************************************************************************/
	/********************************                  AUXILIAR FUNCTIONS                   **********************************/
	/*************************************************************************************************************************/




	protected OperationResponses executeOperation(IOperation op, int quorum, int numOfValidResponses ) {
		Semaphore sem = new Semaphore(1); 
		OperationResponses responses = new OperationResponses(op.getOperationType(), sem, quorum, numOfValidResponses, this.N , this.F);			//responses will be here after the execution
		performOp(op, responses); 				// perform the operation
		sem.acquireUninterruptibly();
		return responses;
	}

	private void performOp(IOperation op, OperationResponses responses) {
		ExecutionThread current = null;
		for(int i = 0 ; i < this.N ; i++){
			do{
				try {
					current = threads.poll(Constants.DEFAULT_TIME_TO_WAIT, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					current=null;
				}
			}while(current==null);

			current.executeOp(connections[i], op, i, responses);
		}
	}


	static String getConnectionFullId(IKVSDriver drv){
		String res = drv.getDriverId();
		if(drv.getUserDefinedId() != null && !drv.getUserDefinedId().equals(""))
			res = res.concat("-").concat(drv.getUserDefinedId());
		return res;
	}


	/**
	 * Read the credentials of drivers accounts
	 * @param filename 
	 */
	private List<String[][]> readCredentials(String filename) throws FileNotFoundException, ParseException{
		Scanner sc=new Scanner(new File(filename==null ? "config"+File.separator+"accounts.properties" : filename));
		String line;
		String [] splitLine;
		LinkedList<String[][]> list = new LinkedList<String[][]>();
		int lineNum =-1;
		LinkedList<String[]> l2 = new LinkedList<String[]>();
		boolean firstTime = true;
		while(sc.hasNext()){
			lineNum++;
			line = sc.nextLine();
			if(line.startsWith("#") || line.equals(""))
				continue;
			else{
				splitLine = line.split("=", 2);
				if(splitLine.length!=2){
					sc.close();
					throw new ParseException("Bad formated accounts.properties file.", lineNum);
				}else{
					if(splitLine[0].equals("driver.type")){
						if(!firstTime){
							String[][] array= new String[l2.size()][2];
							for(int i = 0;i<array.length;i++)
								array[i] = l2.get(i);
							list.add(array);
							l2 = new LinkedList<String[]>();
						}else
							firstTime = false;
					}
					l2.add(splitLine);
				}
			}
		}
		String[][] array= new String[l2.size()][2];
		for(int i = 0;i<array.length;i++)
			array[i] = l2.get(i);
		list.add(array);
		sc.close();
		return list;
	}

	public static String toHexString(byte[] array) {
		return Hexadecimal.toHexStringFromBytes(array);
	}

	public static byte[] toByteArray(String s) {
		return Hexadecimal.toBytesFromHex(s);
	}

	public static boolean verifySig(String toSign, byte[] sign){
		return IntegrityManager.verifySignature(-1, toSign.getBytes(), sign);
	}

	public static byte[] sign(String toSign, int clientId){
		return IntegrityManager.getSignature(clientId, toSign.getBytes());
	}

	public void destroy(){
		this.destroyed = true;
		for(ExecutionThread thread : threads)
			thread.terminateThread();
	
		for(ExecutionThread thread : threads){
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	
		if(connections!=null){
			for(int i = 0 ; i< connections.length ; i++)
				connections[i]=null;
			connections = null;
		}
	
		System.gc();
	
	}

	private IKVSDriver getDriver(String type, String driverId, String accessKey, String secretKey) throws StorageCloudException {
		if(type.equals(AmazonS3Driver.getDriverType()))
			return new AmazonS3Driver(clientId, driverId, accessKey, secretKey);

		if(type.equals(GoogleStorageDriver.getDriverType()))
			return new GoogleStorageDriver(clientId, driverId, accessKey, secretKey);

//		if(type.equals(WindowsAzureDriver.getDriverType()))
//			return new WindowsAzureDriver(clientId, driverId, accessKey, secretKey);

		if(type.equals(RackSpaceDriver.getDriverType()))
			return new RackSpaceDriver(clientId, driverId, accessKey, secretKey);

		if(type.equals(SoftLayerDriver.getDriverType()))
			return new SoftLayerDriver(clientId, driverId, accessKey, secretKey);

		if(type.equals(LocalCloudSimulator.getDriverType()))
			return new LocalCloudSimulator(driverId);

		throw new ClientServiceException("invalid Driver Type: " + type);
	}

}
