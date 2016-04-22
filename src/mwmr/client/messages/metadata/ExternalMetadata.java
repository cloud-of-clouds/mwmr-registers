package mwmr.client.messages.metadata;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import jec.ReedSolInfo;
import mwmr.util.Pair;
import pvss.PublicInfo;

public class ExternalMetadata implements Externalizable {

	private List<Pair<String, String>> blockHashs;
	private String version;
	private String wholeDataHash;
	private boolean usingEC;
	private boolean usingPVSS;

	private PublicInfo pvssInfo;
	private ReedSolInfo ecInfo;
	
	public ExternalMetadata() {}

	public ExternalMetadata(int writerId, boolean usingEC, boolean usingPVSS, List<Pair<String, String>> blockHashs, String wholeDataHash, int version) {
		this.blockHashs = blockHashs;
		this.version = writerId + "." + version;
		this.wholeDataHash = wholeDataHash;
		this.usingEC = usingEC;
		this.usingPVSS = usingPVSS;

		this.pvssInfo = null;
		this.ecInfo = null;
	}

	public List<Pair<String, String>> getBlockHashs() {
		return blockHashs;
	}

	public ReedSolInfo getEcInfo() {
		return ecInfo;
	}

	public PublicInfo getPvssInfo() {
		return pvssInfo;
	}

	public String getVersion() {
		return version;
	}

	public String getWholeDataHash() {
		return wholeDataHash;
	}
	
	public void setDataHash(String wholeDataHash) {
		this.wholeDataHash = wholeDataHash;
	}

	public boolean isUsingEC() {
		return usingEC;
	}

	public boolean isUsingPVSS() {
		return usingPVSS;
	}

	public void setBlockHashs(List<Pair<String, String>> blockHashs) {
		this.blockHashs = blockHashs;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		version = in.readUTF();

		int size = in.readInt();
		if(size>=0){
			blockHashs = new ArrayList<Pair<String, String>>(size);
			for(int i = 0 ; i < size ; i++){
				String key = in.readUTF();
				String value = in.readUTF();
				blockHashs.add(new Pair<String, String>(key, value));
			}
		}else{
			blockHashs = null;
		}

		wholeDataHash = in.readUTF();
		usingEC = in.readBoolean();
		usingPVSS = in.readBoolean();

		if(in.readInt()>=0)
			pvssInfo = (PublicInfo) in.readObject();
		if(in.readInt()>=0)
			ecInfo = (ReedSolInfo) in.readObject();

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(version);

		if(blockHashs==null){
			out.writeInt(-1);
		}else{
			out.writeInt(blockHashs.size());
			for(Pair<String, String> p : blockHashs){
				out.writeUTF(p.getKey());
				out.writeUTF(p.getValue());
			}
		}

		out.writeUTF(wholeDataHash);
		out.writeBoolean(usingEC);
		out.writeBoolean(usingPVSS);

		if(pvssInfo == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			out.writeObject(pvssInfo);
		}
		if(ecInfo == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			out.writeObject(ecInfo);
		}
		out.flush();
	}
	
	public int getNextVersion(){
		return Integer.parseInt(version.split("\\.")[1])+1;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		ExternalMetadata res = new ExternalMetadata();
		
		res.blockHashs = new ArrayList<Pair <String, String>>(blockHashs);
		res.version = new String(version);
		res.wholeDataHash = new String(wholeDataHash);
		res.usingEC = usingEC;
		res.usingPVSS = usingPVSS;

		res.pvssInfo = pvssInfo;
		res.ecInfo = ecInfo;
		
		return res;
	}

	public void setPvssInfo(PublicInfo pvssPublicInfo) {
		pvssInfo = pvssPublicInfo;
		
	}

	public void setEcInfo(ReedSolInfo info) {
		ecInfo = info;
	}

	public void incrementVersion(int writerId) {
		this.version = writerId + "." + getNextVersion();
	}

	public int getWriterId() {
		return Integer.parseInt(version.split("\\.")[0]);
	}
	
	@Override
	public String toString() {
		String res = "v = " + version;
		res = res.concat("WholeDataHash = " + wholeDataHash);
		res = res.concat("usingEC = " + usingEC);
		res = res.concat("usingPVSS = " + usingPVSS);
		
		res = res.concat("blockHashes  = {" );
		for(Pair<String, String> p : blockHashs){
			res = res.concat("<" + p.getKey() +" , " + p.getValue() +">,");
		}
		res = res.concat("}");
		
		return res;
	}

	public int getVersionInteger() {
		return Integer.parseInt(version.split("\\.")[1]);
	}
}
