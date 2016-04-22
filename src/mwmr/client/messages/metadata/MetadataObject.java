package mwmr.client.messages.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import jec.ReedSolInfo;
import mwmr.util.IOUtil;
import mwmr.util.integrity.IntegrityManager;
import pvss.PublicInfo;

public class MetadataObject implements Externalizable {

	private String version;
	private String blockHash;
	private String wholeDataHash;
	
	private DataHashesHistory history;
	
	private boolean usingEC;
	private boolean usingPVSS;

	private PublicInfo pvssInfo;
	private ReedSolInfo ecInfo;
	private int writerId;

	private byte[] signature;
	
	public MetadataObject() {	}

	public MetadataObject(int writerId, boolean usingEC, boolean usingPVSS, String blockHash, String wholeDataHash) {
		this(writerId, usingEC, usingPVSS, blockHash, wholeDataHash, 0);
	}
	
	public MetadataObject(MetadataObject other, String blockHash, String wholeDataHash, String version) {
		this.writerId = other.writerId;
		this.version = version;
		this.blockHash = blockHash;
		this.wholeDataHash = wholeDataHash;
		this.usingEC = other.usingEC;
		this.usingPVSS = other.usingPVSS;
		this.pvssInfo = other.pvssInfo;
		this.ecInfo = other.ecInfo;
		this.history = new DataHashesHistory();
	}


	public MetadataObject(int writerId,  boolean usingEC, boolean usingPVSS, String blockHash, String wholeDataHash, int version) {
		this.writerId = writerId;
		this.version = writerId + "." + version;
		this.blockHash = blockHash;
		this.wholeDataHash = wholeDataHash;
		this.usingEC = usingEC;
		this.usingPVSS = usingPVSS;
		this.pvssInfo = null;
		this.ecInfo = null;
		this.history = new DataHashesHistory();
	}

	public String getVersion(){
		return version;
	}
	
	public String getVersion(String wholeDataHash){
		return history.getVersion(wholeDataHash);
	}
	
	public int getVersionInteger(){
		return Integer.parseInt(version.split("\\.")[1]);
	}

	public String getBlockHash() {
		return blockHash;
	}
	
	public String getBlockHash(String wholeDataHash, String connectionId) {
		return history.getBlockHash(wholeDataHash, connectionId);
	}

	public String getWholeDataHash() {
		return wholeDataHash;
	}

	public PublicInfo getPVSSInfo(){
		return pvssInfo;
	}
	
	public DataHashesHistory getHistory() {
		return history;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		version = in.readUTF();
		writerId = in.readInt();
		blockHash = in.readUTF();
		wholeDataHash = in.readUTF();
		usingEC = in.readBoolean();
		usingPVSS = in.readBoolean();

		if(in.readInt()>=0)
			pvssInfo = (PublicInfo) in.readObject();
		if(in.readInt()>=0)
			ecInfo = (ReedSolInfo) in.readObject();
		
		int size = in.readInt();
		if(size>=0){
			signature = new byte[size];
			IOUtil.readFromOIS(in, signature);
		}
		if(in.readInt()>=0){
			history = new DataHashesHistory();
			history.readExternal(in);
		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(version);
		out.writeInt(writerId);
		out.writeUTF(blockHash);
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
		
		if(signature == null){
			out.writeInt(-1);
		}else{
			out.writeInt(signature.length);
			IOUtil.writeToOOS(out, signature);
		}
		
		if(history == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			history.writeExternal(out);
		}
		out.flush();
	}


	public int getNextVersion(){
		return Integer.parseInt(version.split("\\.")[1])+1;
	}

	public boolean isUsingEC() {
		return usingEC;
	}

	public boolean isUsingPVSS() {
		return usingPVSS;
	}
	
	public ReedSolInfo getEcInfo() {
		return ecInfo;
	}
	
	public void setEcInfo(ReedSolInfo ecInfo) {
		this.ecInfo = ecInfo;
//		this.signature = IntegrityManager.getSignature(writerId, getBytesToSign());
	}
	
	public void setPvssInfo(PublicInfo pvssInfo) {
		this.pvssInfo = pvssInfo;
//		this.signature = IntegrityManager.getSignature(writerId, getBytesToSign());
	}

	public PublicInfo getPvssInfo() {
		return pvssInfo;
	}

	public void setBlockHash(String hexHash) {
		this.blockHash = hexHash;
//		this.signature = IntegrityManager.getSignature(writerId, getBytesToSign());
	}
	
	
	public void sign(){
		this.signature = IntegrityManager.getSignature(writerId, getBytesToSign());
	}
	
	
	public boolean isSignatureValid(){
		try{
			int writerId = Integer.parseInt(version.split("\\.")[0]);
			byte[] bytes = getBytesToSign();
			if(signature == null || bytes==null)
				return false;
			return IntegrityManager.verifySignature(writerId, getBytesToSign(), signature);
		}catch(Exception e){
			return false;
		}
	}
	
	private byte[] getBytesToSign(){
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(version);
			oos.writeInt(writerId);
			oos.writeUTF(blockHash);
			oos.writeUTF(wholeDataHash);
			oos.writeBoolean(usingEC);
			oos.writeBoolean(usingPVSS);
			if(pvssInfo!=null)
				oos.writeObject(pvssInfo);
			if(ecInfo!=null)
				oos.writeObject(ecInfo);
			
			if(history!=null)
				history.writeExternal(oos);

			IOUtil.closeStream(oos);
			IOUtil.closeStream(baos);

			return baos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return null;
	}
	
	/**
	 * 
	 * @param buf
	 * @requires buf!=null;
	 * @return
	 */
	public static MetadataObject getMetadataFromBytes(byte[] buf){

		MetadataObject res = new MetadataObject();
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			ObjectInputStream ois = new ObjectInputStream(bais);

			res.readExternal(ois);

			IOUtil.closeStream(bais);
			IOUtil.closeStream(ois);
		} catch (IOException e) {
			res = null;
		} catch (ClassNotFoundException e) {
			res = null;
			e.printStackTrace();
		}
		return res;
	}

	public byte[] toBytes(){
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);


			this.writeExternal(oos);

			IOUtil.closeStream(oos);
			IOUtil.closeStream(baos);

			return baos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}


	@Override
	public String toString() {
		String res = "v = " + version;
		res = res.concat("\nwriterId = " + writerId);
		res = res.concat("\nblockHash = " + blockHash);
		res = res.concat("\nWholeDataHash = " + wholeDataHash);
		res = res.concat("\nusingEC = " + usingEC);
		res = res.concat("\nusingPVSS = " + usingPVSS);
		
		return res;
	}
	
	public int getWriterId() {
		return writerId;
	}

	public void setDataHistory(DataHashesHistory history) {
		this.history = history;
	}

	
}
