package mwmr.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mwmr.util.Pair;

public class CloudsCredentials {
	
	private Map<String, String[]> credentials;
	
	public CloudsCredentials() {
		this.credentials = new HashMap<String, String[]>();
	}
	
	public CloudsCredentials(List<Pair<String,String[]>> cannonicalIds) {
		this.credentials = new HashMap<String, String[]>();
		insertCredentials(cannonicalIds);
	}
	
	public boolean containsCredentials(String driverId){
		return credentials.containsKey(driverId);
	}
	
	public String[] getCredentials(String driverId){
		return credentials.get(driverId);
	}
	
	public void insertCredentials(String driverId, String[] credentials){
		this.credentials.put(driverId, credentials);
	}

	public void insertCredentials(List<Pair<String,String[]>> cannonicalIds){
		for(Pair<String, String[]> p : cannonicalIds)
			credentials.put(p.getKey(), p.getValue());
	}
	
	public List<Pair<String,String[]>> getAsList(){
		List<Pair<String,String[]>> cannonicalIds =new ArrayList<Pair<String,String[]>>(credentials.size());
		
		for(Entry<String, String[]> e : credentials.entrySet())
			cannonicalIds.add(new Pair<String,String[]>(e.getKey(), e.getValue()));
		
		return cannonicalIds;
	}
}
