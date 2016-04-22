package mwmr.client;

import java.util.List;

import mwmr.client.messages.Response;
import mwmr.client.messages.ResponseType;
import mwmr.exceptions.ClientServiceException;
import mwmr.exceptions.StorageCloudException;
import mwmr.operations.ListMWMROperation;
import mwmr.operations.ReadMWMROperation;
import mwmr.operations.WriteMWMROperation;
import mwmr.util.integrity.IntegrityManager;

public class TwoStepFR extends AMwmrRegister{
	
	private static final int DEFAULT_NUMBER_OF_THREADS = 8;
	private static final int DEFAULT_N = 4;
	private static final int DEFAULT_F = 1;
	
	

	public TwoStepFR(int clientId) throws StorageCloudException{
		super(clientId, DEFAULT_NUMBER_OF_THREADS, null, null, DEFAULT_N, DEFAULT_F, -1, -1);
	}

	public TwoStepFR(int clientId,  int numOfThreads) throws StorageCloudException{
		super(clientId,  numOfThreads, null, null, DEFAULT_N, DEFAULT_F, -1, -1);
	}

	public TwoStepFR(int clientId, int numOfThreads,  List<String[][]> accessCredentials) throws StorageCloudException{
		super(clientId, numOfThreads, null, accessCredentials, DEFAULT_N, DEFAULT_F, -1, -1);
	}

	public TwoStepFR(int clientId, int numOfThreads, String configFilePath) throws StorageCloudException{
		this(clientId, numOfThreads, configFilePath, DEFAULT_N, DEFAULT_F);
	}
	
	protected TwoStepFR(int clientId, int numOfThreads, String configFilePath, int n, int f) throws StorageCloudException{
		super(clientId, numOfThreads, configFilePath, null, n, f, -1, -1);
	}

	protected TwoStepFR(int clientId, int numOfThreads, String configFilePath, int n, int f, int ecK, int ecM) throws StorageCloudException{
		super(clientId, numOfThreads, configFilePath, null, n, f, ecK, ecM);
	}

	
	
	@Override
	public void write(DataUnit dataUnit, byte[] data) throws StorageCloudException {
		if(destroyed)
			throw new ClientServiceException("DepSky Client already destroyed.");

		if(dataUnit == null || dataUnit.getContainerName()==null || data == null )
			throw new ClientServiceException("write(): a required argument is null");

		//		List versions
		OperationResponses responses = executeOperation(new ListMWMROperation(dataUnit, ""), this.LIST_QUORUM, 1);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Full Replication Regular Write: (list) No quorum achieved");
		}
		Response[] respProcessed = responses.getProcessedMWMRResponses();
		String maxKey = getMaxVer(respProcessed);

		int newTs = 0;
		if(!maxKey.equals(""))
			newTs = Integer.parseInt(maxKey.split(",")[0])+1;
		else
			newTs = 1;
		String hash = IntegrityManager.getHexHash(data);
		String newVer = newTs + "," + clientId + "," + hash;

		String newKey = newVer + "-" + toHexString(sign(newVer, -1));

		byte[][] value = new byte[1][data.length];
		value[0] = data;

		//		write data
		responses = executeOperation(new WriteMWMROperation(dataUnit, newKey, value, false), this.WRITE_QUORUM, this.WRITE_QUORUM);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Write : No quorum achieved");
		}
	}

	@Override
	public byte[] read(DataUnit dataUnit) throws StorageCloudException {
		if(destroyed)
			throw new ClientServiceException("DepSky Client already destroyed.");

		if(dataUnit == null || dataUnit.getContainerName()==null)
			throw new ClientServiceException("read(): a required argument is null");

		//		List versions
		OperationResponses responses = executeOperation(new ListMWMROperation(dataUnit, ""), this.LIST_QUORUM, 1);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Full Replication Regular Read: (list) No quorum achieved");
		}
		Response[] respProcessed = responses.getProcessedMWMRResponses();

		String hash = null;
		int numReadsHappened = 0;
		while(true){
			String maxKey = getMaxVer(respProcessed);
			numReadsHappened ++;
			if(!maxKey.equals("")){
				OperationResponses Readresponses = executeOperation(new ReadMWMROperation(dataUnit, maxKey), this.READ_QUORUM, 1);
				if(Readresponses.getNumberOfFailResponses() > (this.N - this.READ_QUORUM)){
					throw new StorageCloudException("Read: No quorum achieved");
				}
				Response[] ReadrespProcessed = Readresponses.getProcessedMWMRResponses();
				int count = 0;
				while(count < 3){
					if(count > 0){
						ReadrespProcessed = new Response[0];
						while(ReadrespProcessed.length == 0){
							ReadrespProcessed = Readresponses.getExtraResponses();
							try {
								System.out.println("I WILL SLEEP!!!!!!!!!!!!!!");
								Thread.sleep(50);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
					for(int i = 0; i < ReadrespProcessed.length; i++){
						hash = maxKey.split("-")[0].split(",")[2];
						if(!ReadrespProcessed[i].isFailResponse() && ReadrespProcessed[i].getResponseType() != ResponseType.NOT_EXISTS)
							if(hash.equals(IntegrityManager.getHexHash(ReadrespProcessed[i].getData()))){
								//								System.out.println("VALID VALUE");
								System.out.println("NUM OF VERSION READ FOR THIS OPERATION = " + numReadsHappened);
								return ReadrespProcessed[i].getData();
							}
						count++;
					}
				}

				removeKeyFromListResult(respProcessed, maxKey);
			}else{
				throw new StorageCloudException("Read: No version available!");
			}
		}
	}
	
}
