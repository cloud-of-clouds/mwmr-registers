package mwmr.clouds;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mwmr.exceptions.StorageCloudException;

public class LocalCloudSimulator implements IKVSDriver {

	private String userDefinedID;

	public LocalCloudSimulator(String id) {
		this.userDefinedID = id;
	}
	
	@Override
	public void writeObject(String bucket, String id, byte[] data,
			String[] uploadToAnotherAccount) {
		sleepSomeTime();
		
		String bucketName = bucket + this.userDefinedID;
		File f = new File(bucketName);
		if(!f.exists())
			while(!f.mkdirs());
		f = new File(bucketName + File.separator + id);
		try {
			while(!f.exists()){
				while(!f.createNewFile());
			}

			FileOutputStream fos = new FileOutputStream(f);
			fos.write(data);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public byte[] readObjectData(String bucket, String id,
			String[] uploadToAnotherAccount) {
		sleepSomeTime();

		String bucketName = bucket + this.userDefinedID;
		File f = new File(bucketName);
		if(!f.exists())
			return null;

		f = new File(bucketName + File.separator + id);
		if(!f.exists())
			return null;

		try {
			FileInputStream fis = new FileInputStream(f);
			byte[] data = new byte[(int) f.length()];
			fis.read(data);
			fis.close();
			return data;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;


	}

	@Override
	public void deleteObject(String bucketName, String id,
			String[] uploadToAnotherAccount) {
		sleepSomeTime();
	}

	@Override
	public void deleteObjects(String bucketName, String[] ids,
			String[] uploadToAnotherAccount) throws StorageCloudException {
		sleepSomeTime();
	}

	@Override
	public void deleteContainer(String bucketName,
			String[] uploadToAnotherAccount) {
		sleepSomeTime();
	}

	@Override
	public List<String> listContainer(String prefix, String sid,
			String[] uploadToAnotherAccount) {
		sleepSomeTime();
		return new ArrayList<String>();
	}

	private void sleepSomeTime(){
		int minTime = 300;
		int rand = new Random().nextInt(800);

		try {
			Thread.sleep(minTime+rand);
		} catch (InterruptedException e) {		}
	}

	@Override
	public String getDriverId() {
		return "LOCAL";
	}

	@Override
	public String getUserDefinedId() {
		return userDefinedID;
	}

	public static String getDriverType() {
		return "LOCAL";
	}


}
