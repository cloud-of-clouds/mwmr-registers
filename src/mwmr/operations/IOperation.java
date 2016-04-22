package mwmr.operations;

import mwmr.client.DataUnit;

public interface IOperation {

	OperationType getOperationType();
	
	DataUnit getDataUnit();
	
}
