package mwmr.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import mwmr.client.messages.Response;
import mwmr.client.messages.ResponseType;
import mwmr.operations.OperationType;

public class OperationResponses {

	private Response[] responses;
	private int index;
	private int failResponses;
	private Semaphore semaphore;
	private OperationType opType;
	private int quorum;
	private int notExists;
	private int numValidResponses;
	private boolean released;
	private Response[] processedMetadata;
	private List<Response> extraResponses;
	private int n;
	private int f;

	public OperationResponses(OperationType opType, Semaphore semaphore, int quorum, int numValidResponses, int n, int f) {
		this.n = n;
		this.f = f;
		responses = new Response[this.n];
		this.index = 0;
		this.opType = opType;
		this.semaphore = semaphore;
		this.semaphore.acquireUninterruptibly();

		this.failResponses = 0;
		this.notExists = 0;

		this.quorum = quorum;
		this.numValidResponses = numValidResponses;

		// FIXME: client may change this array. BE CAREFULL
		this.processedMetadata = null;
		this.released = false;

		this.extraResponses = new ArrayList<Response>(this.n-quorum);
	}

	public void addResponse(Response res) {
		synchronized (this) {
			if(released){
				extraResponses.add(res);
				return;
			}else
				responses[index++] = res;

			switch(opType){
			case WRITEMWMR:
			case LISTMWMR:
				if(res.isFailResponse()){ //if it is a valid response, release one permit on the semaphore.
					failResponses++;
				}
				if(failResponses > this.f){
					release();
					return;
				}
				if(index-failResponses >= quorum){
					release();
					return;
				}
				break;
			case READMWMR:
				if(res.isFailResponse()){ //if it is a valid response, release one permit on the semaphore.
					failResponses++;
				}else if (res.getResponseType() == ResponseType.NOT_EXISTS) {
					notExists++;
				}
				//have enough blocks
				if(index-(failResponses+notExists) >= numValidResponses){
					release();
					return;
				}
				//there is no enough blocks
				if(index >= quorum){
					release();
					return;
				}
				break;
			default:

				if(res.isFailResponse()){ //if it is a valid response, release one permit on the semaphore.
					failResponses++;
				}else if (res.getResponseType() == ResponseType.NOT_EXISTS) {
					notExists++;
				}

				//more than F null responses has arrived.
				if(failResponses > this.f){
					release();
					return;
				}

				// we have a quorum of NOT EXIST responses
				if(notExists >= quorum){
					processedMetadata = processMetadataResponses(responses);
					release();
					return;
				}

				// all responses has already arrived
				if(index==responses.length){
					processedMetadata = processMetadataResponses(responses);
					release();
					return;
				}

				// we have a quorum of OK responses
				if(index-(failResponses+notExists) >= quorum){
					processedMetadata = processMetadataResponses(responses);
					// we have
					if(processedMetadata != null && processedMetadata.length >= numValidResponses)
						release();
				}
				break;
			}
		}
	}

	public boolean allResponsesArrived() {
		synchronized (this) {
			return index == responses.length && extraResponses.isEmpty();
		}
	}

	public boolean allResponsesMWMRArrived() {
		synchronized (this) {
			return index == responses.length + extraResponses.size();
		}
	}

	private void reprocessReponsesWithExtra(){
		if(processedMetadata==null)
			processedMetadata = new Response[0];

		Response[] res = new Response[processedMetadata.length + extraResponses.size()];

		int index = 0;
		for(int i = 0 ; i < processedMetadata.length ; i++)
			res[index++] = processedMetadata[i];

		for(int i = 0 ; i < extraResponses.size() ; i++)
			res[index++] = extraResponses.get(i);

		this.processedMetadata = processMetadataResponses(res);
		this.index += extraResponses.size();
		this.extraResponses = new ArrayList<Response>();
	}

	public int getNumberOfFailResponses(){
		synchronized (this) {
			return failResponses;
		}
	}

	public Response[] getProcessedResponses(){
		synchronized (this) {
			if(extraResponses.size()>0)
				reprocessReponsesWithExtra();

			return processedMetadata;
		}
	}
	public Response[] getProcessedMWMRResponses(){
		synchronized (this) {
			List<Response> result = new ArrayList<Response>();
			for(Response res: responses)
				if(res != null)
					result.add(res);

			if(extraResponses.size()>0){
				for(int i = 0; i < extraResponses.size(); i++){
					result.add(extraResponses.get(i));

				}
				extraResponses = new ArrayList<>();
			}
			return result.toArray(new Response[result.size()]);
		}
	}
	public Response[] getExtraResponses(){
		synchronized (this) {
			List<Response> result = new ArrayList<Response>();		
			if(extraResponses.size()>0){
				for(int i = 0; i < extraResponses.size(); i++){
					result.add(extraResponses.get(i));

				}
				extraResponses.clear();
			}			
			return result.toArray(new Response[result.size()]);
		}
	}
	private Response[] processMetadataResponses(Response[] responses){
		if(failResponses+notExists > this.f)
			return null;

		List<Response> result = new ArrayList<Response>();

		// OK but no metadata ??
		for(int i = 0 ; i<responses.length ; i++){
			if(responses[i] != null && responses[i].getResponseType() == ResponseType.OK && responses[i].getMetadata() == null)
				result.add(responses[i]);
		}

		if(result.size()>=quorum){
			return result.toArray(new Response[result.size()]);
		}


		result=null;
		// is ok with metadata - see what the most up-to-date version.
		for(int i = 0 ; i<responses.length ; i++){
			if(responses[i] == null || responses[i].getResponseType() != ResponseType.OK || responses[i].getMetadata() == null)
				continue;

			if(result==null || responses[i].getMetadata().getVersionInteger() > result.get(0).getMetadata().getVersionInteger()){
				result=new ArrayList<Response>();
				result.add(responses[i]);
			}else if(responses[i].getMetadata().getVersionInteger() == result.get(0).getMetadata().getVersionInteger()){
				result.add(responses[i]);
			}
		}

		if(result!=null && result.size()>0){
			return result.toArray(new Response[result.size()]);
		}

		//no quorum achieved.
		return null;

	}

	public Response[] getResponses(){
		return responses;
	}

	private void release(){
		released = true;
		semaphore.release();
	}

	public int getNumberOfNotExistsResponses() {
		synchronized (this) {
			return notExists;
		}
	}


}
