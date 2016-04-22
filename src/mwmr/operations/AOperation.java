package mwmr.operations;

import mwmr.client.DataUnit;

public abstract class AOperation implements IOperation {

	private DataUnit du;
	private OperationType opType;
	
	public AOperation(DataUnit du, OperationType opType) {
		this.du = du;
		this.opType = opType;
	}
	
	@Override
	public DataUnit getDataUnit() {
		return du;
	}
	
	@Override
	public OperationType getOperationType() {
		return opType;
	}
	
}
