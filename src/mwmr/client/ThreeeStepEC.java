package mwmr.client;

import java.util.LinkedList;
import java.util.List;

import jec.EncodeResult;
import jec.ReedSolomon;
import mwmr.client.messages.Response;
import mwmr.client.messages.ResponseType;
import mwmr.exceptions.ClientServiceException;
import mwmr.exceptions.StorageCloudException;
import mwmr.operations.ListMWMROperation;
import mwmr.operations.ReadMWMROperation;
import mwmr.operations.WriteMWMROperation;
import mwmr.util.Constants;
import mwmr.util.integrity.IntegrityManager;

public class ThreeeStepEC extends AMwmrRegister {

	private static final int DEFAULT_NUMBER_OF_THREADS = 8;
	private static final int DEFAULT_N = 4;
	private static final int DEFAULT_F = 1;
	private static final int DEFAULT_EC_K = 2;
	private static final int DEFAULT_EC_M = 2;
	
	

	public ThreeeStepEC(int clientId) throws StorageCloudException{
		super(clientId, DEFAULT_NUMBER_OF_THREADS, null, null, DEFAULT_N, DEFAULT_F, DEFAULT_EC_K, DEFAULT_EC_M);
	}

	public ThreeeStepEC(int clientId,  int numOfThreads) throws StorageCloudException{
		super(clientId,  numOfThreads, null, null, DEFAULT_N, DEFAULT_F, DEFAULT_EC_K, DEFAULT_EC_M);
	}

	public ThreeeStepEC(int clientId, int numOfThreads,  List<String[][]> accessCredentials) throws StorageCloudException{
		super(clientId, numOfThreads, null, accessCredentials, DEFAULT_N, DEFAULT_F, DEFAULT_EC_K, DEFAULT_EC_M);
	}

	public ThreeeStepEC(int clientId, int numOfThreads, String configFilePath) throws StorageCloudException{
		super(clientId, numOfThreads, configFilePath, null, DEFAULT_N, DEFAULT_F, DEFAULT_EC_K, DEFAULT_EC_M);
	}

	public ThreeeStepEC(int clientId, int numOfThreads, String configFilePath, int n, int f, int ecK, int ecM) throws StorageCloudException{
		super(clientId, numOfThreads, configFilePath, null, n, f, ecK, ecM);
	}
	
	
	
	@Override
	public void write(DataUnit dataUnit, byte[] data) throws StorageCloudException {
		if(destroyed)
			throw new ClientServiceException("DepSky Client already destroyed.");

		if(dataUnit == null || dataUnit.getContainerName()==null || data == null )
			throw new ClientServiceException("write(): a required argument is null");

		//			List versions
		OperationResponses responses = executeOperation(new ListMWMROperation(dataUnit, "pow"), this.LIST_QUORUM, 1);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Full Replication Regular Write: (list) No quorum achieved");
		}

		Response[] respProcessed = responses.getProcessedMWMRResponses();
		String maxKey = getMaxVer(respProcessed);

		//erasure-codes
		EncodeResult encodeRes = ReedSolomon.encode(data, this.EC_K, this.EC_M, Constants.EC_W);
		byte[][] blocks = encodeRes.getBlocks();

		int newTs = 0;
		if(!maxKey.equals(""))
			newTs = Integer.parseInt(maxKey.split(",")[0])+1;
		else
			newTs = 1;
		int dataSize = encodeRes.getInfo().getSize();
		String hash = IntegrityManager.getHexHash(data);
		String dataKey = newTs + "," + clientId;
		String newVer = dataKey + "," + hash + "," + dataSize;
		String proof = "pow-" + newVer + "-" + toHexString(sign(newVer, -1));

		//			write data
		responses = executeOperation(new WriteMWMROperation(dataUnit, dataKey, blocks, true), this.WRITE_QUORUM, this.WRITE_QUORUM);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Write : No quorum achieved");
		}

		blocks = new byte[1][];
		blocks[0] = "".getBytes();

		//			write proof
		responses = executeOperation(new WriteMWMROperation(dataUnit, proof, blocks, false), this.WRITE_QUORUM, this.WRITE_QUORUM);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Write : No quorum achieved");
		}
	}
	
	@Override
	public byte[] read(DataUnit dataUnit) throws StorageCloudException {
		if(destroyed)
			throw new ClientServiceException("DepSky Client already destroyed.");

		if(dataUnit == null || dataUnit.getContainerName()==null)
			throw new ClientServiceException("write(): a required argument is null");

		//			List versions

		OperationResponses responses = executeOperation(new ListMWMROperation(dataUnit, "pow"), this.LIST_QUORUM, 1);
		if(responses.getNumberOfFailResponses() > this.F){
			throw new StorageCloudException("Full Replication Regular Write: (list) No quorum achieved");
		}
		Response[] respProcessed = responses.getProcessedMWMRResponses();
		String maxKey = getMaxVer(respProcessed);

		if(maxKey.equals(""))
			return null;

		String[] keyFields = maxKey.split(",");
		String dataKey = keyFields[0] + "," + keyFields[1];
		OperationResponses Readresponses = executeOperation(new ReadMWMROperation(dataUnit, dataKey), this.READ_QUORUM, 2);
		if(Readresponses.getNumberOfFailResponses() > (this.N - this.READ_QUORUM)){
			throw new StorageCloudException("Read: No quorum achieved");
		}
		Response[] ReadrespProcessed = Readresponses.getProcessedMWMRResponses();
		String hash = null;
		int dataSize = 0;
		byte[] data = null;
		int count = 0;
		List<byte[]> blocks = new LinkedList<byte[]>();
		while(count < 4){
			if(count > 0){
				ReadrespProcessed = new Response[0];
				while(ReadrespProcessed.length == 0){
					ReadrespProcessed = Readresponses.getExtraResponses();
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			hash = maxKey.split("-")[0].split(",")[2];
			dataSize = Integer.parseInt(maxKey.split("-")[0].split(",")[3]); 
			for(int i = 0; i < ReadrespProcessed.length; i++){
				if(!ReadrespProcessed[i].isFailResponse() && ReadrespProcessed[i].getResponseType() != ResponseType.NOT_EXISTS)
					blocks.add(ReadrespProcessed[i].getData());
				if(blocks.size() > 1){
					data = getOriginalValue(blocks, hash, dataSize);
					if(data != null)
						return data;
				}
				count++;
			}
		}
		throw new StorageCloudException("Read: There are more than F clouds with a Byzantine behaviour!");
	}
	
	
}
