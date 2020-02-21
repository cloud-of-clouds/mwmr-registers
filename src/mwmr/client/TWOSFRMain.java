package mwmr.client;

import mwmr.exceptions.StorageCloudException;

public class TWOSFRMain {

	public static void main(String[] args) throws StorageCloudException {

		TwoStepFR stf = new TwoStepFR(0);
		DataUnit dataUnit = new DataUnit("container", "object");
		stf.write(dataUnit, "test".getBytes());
		byte[] data = stf.read(dataUnit);
		System.out.println("--> " + new String(data));
	}

}
