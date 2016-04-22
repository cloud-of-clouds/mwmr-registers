package mwmr.operations;

import mwmr.client.DataUnit;

public class ReadMWMROperation extends AOperation{
	
	private String version;
	
	public ReadMWMROperation(DataUnit du, String version) {
		super(du, OperationType.READMWMR);
		this.version = version;
	}

	public String getVersion(){
		return version;
	}
	
}
