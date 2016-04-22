package mwmr.operations;

import mwmr.client.DataUnit;

public class WriteMWMROperation extends AOperation{

	private String version;
	private byte[][] data;
	private boolean isEC;

	public WriteMWMROperation(DataUnit du, String version, byte[][] data, boolean isEC) {
		super(du, OperationType.WRITEMWMR);
		this.version = version;
		this.data = data;
		this.isEC = isEC;
	}

	public String getVersion(){
		return version;
	}

	public byte[] getData(int blockIndex){
		if(isEC)
			return data[blockIndex];
		else
			return data[0];
	}
	public boolean getIsEC(){
		return isEC;
	}
}
