package mwmr.operations;

import mwmr.client.DataUnit;

public class ListMWMROperation extends AOperation{
	
	private String prefix;
	
	public ListMWMROperation(DataUnit du, String prefix) {
		super(du, OperationType.LISTMWMR);		
		this.prefix = prefix;
	}
	
	public String getPrefix(){
		return this.prefix;
	}
}
