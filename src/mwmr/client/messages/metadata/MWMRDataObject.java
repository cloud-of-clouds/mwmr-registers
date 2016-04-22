package mwmr.client.messages.metadata;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import mwmr.client.AMwmrRegister;
import mwmr.util.IOUtil;

public class MWMRDataObject implements Externalizable{
	
	private byte[] data;
	private String hash;
	private byte[] signature;
	
	public MWMRDataObject(byte[] data, String hash){
		this.data = data;
		this.hash = hash;
	}
	
	public byte[] getData(){
		return data;
	}
	
	public String getHash(){
		return hash;
	}
	public void sign(){
		this.signature = AMwmrRegister.sign(hash, -1);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(hash);
		if(data!=null){
			out.writeInt(data.length);
			IOUtil.writeToOOS(out, data);
		}else{
			out.writeInt(-1);
		}
		if(signature!=null){
			out.writeInt(signature.length);
			IOUtil.writeToOOS(out, signature);
		}else{
			out.writeInt(-1);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		hash = in.readUTF();
		int size = in.readInt();
		if(size>=0){
			data=new byte[size];
			IOUtil.readFromOIS(in, data);
		}
		size = in.readInt();
		if(size>=0){
			signature=new byte[size];
			IOUtil.readFromOIS(in, signature);
		}
	}
}
