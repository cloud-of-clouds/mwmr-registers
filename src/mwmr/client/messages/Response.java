package mwmr.client.messages;

import java.util.List;

import mwmr.client.messages.metadata.MetadataObject;
import mwmr.util.Pair;


public class Response {
	private ResponseType resType;
	private byte[] data;
	private MetadataObject metadata;
	private Pair<String, String[]> credentials;
	private List<String> listNames;
	private int version;
	private String versionICStore;
	
	public Response(ResponseType resType) {
		this.resType = resType;
	}
	
	public Response(ResponseType resType, List<String> listNames){
		this.resType = resType;
		this.listNames = listNames;
	}
	
	public Response(ResponseType resType, String connectionId) {
		this(resType, null, null, connectionId);
	}
	
	public Response(ResponseType resType, byte[] data, MetadataObject metadata, String connectionId){
		this.resType = resType;
		this.data=data;
		this.metadata = metadata;
	}
	
	public Response(ResponseType resType, Pair<String, String[]> credentials ){
		this.resType = resType;
		this.credentials = credentials;
	}
	
	public Response(ResponseType resType, MetadataObject metadata, String connectionId){
		this(resType, null, metadata, connectionId);
	}

	public Response(ResponseType resType, byte[] data ){
		this(resType, data, null, null);
	}
	
	public Response(ResponseType resType, byte[] data, int version ){
		this.resType = resType;
		this.data=data;
		this.version = version;
	}
	
	public Response(ResponseType resType, byte[] data, String version ){
		this.resType = resType;
		this.data=data;
		this.versionICStore = version;
	}

	public Pair<String, String[]> getCredentials() {
		return credentials;
	}
	
	public ResponseType getResponseType() {
		return resType;
	}
	
	public MetadataObject getMetadata() {
		return metadata;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public List<String> getListNames(){
		return listNames;
	}
	
	public boolean isFailResponse(){
		return resType==ResponseType.FAIL;
	}
	
	public int getVersion() {
		return version;
	}	
	
	public String getVersionICStore() {
		return versionICStore;
	}
	
	@Override
	public String toString() {
		String res = resType.name();
		res = res.concat("data = " + data==null ? "null" : "not_null");
		res = res.concat("metadata = " + metadata==null ? "null" : "not_null");
		return res;
	}
	
}
