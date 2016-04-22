package mwmr.client.messages;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import mwmr.util.IOUtil;
import pvss.Share;

public class DataObject implements Externalizable{

	private byte[] data;
	private Share keyShare;
	
	public DataObject(byte[] data, Share keyShare) {
		this.data = data;
		this.keyShare = keyShare;
	}
	
	public DataObject() {	}
	
	public byte[] getData() {
		return data;
	}
	
	public Share getKeyShare() {
		return keyShare;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		int size = in.readInt();
		if(size>=0){
			data=new byte[size];
			IOUtil.readFromOIS(in, data);
		}
		size = in.readInt();
		if(size!=-1)
			keyShare = (Share) in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if(data!=null){
			out.writeInt(data.length);
			IOUtil.writeToOOS(out, data);
		}else{
			out.writeInt(-1);
		}
		
		if(keyShare!=null){
			out.writeInt(0);
			out.writeObject(keyShare);
		}else{
			out.writeInt(-1);
		}
		out.flush();
	}

}
