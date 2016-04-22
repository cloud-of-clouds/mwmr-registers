package mwmr.clouds.softlayer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.lang.text.StrTokenizer;
import org.restlet.data.MediaType;
import org.restlet.representation.InputRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

import mwmr.clouds.IKVSDriver;
import mwmr.exceptions.StorageCloudException;


public class SoftLayerDriver implements IKVSDriver {

	private static final String DRIVER_ID = "SOFTLAYER";

	private Client conn;

	private String userDefinedId;

	private Hashtable<String, String> authParams;

	@SuppressWarnings("unused")
	private int clientId;

	public SoftLayerDriver(int clientId, String driverId, String username, String password) throws StorageCloudException {

		// user = SLOS531003-2:SL531003
		// pass = 1ce7330edf1923c1972a3b3afc7e80b5b994d2c9147a71426f689cc48d0cd624

		// curl -i https://par01.objectstorage.softlayer.net/auth/v1.0 -H 'x-auth-user: SLOS531003-2:SL531003' -H 'x-auth-key: 1ce7330edf1923c1972a3b3afc7e80b5b994d2c9147a71426f689cc48d0cd624'

		this.clientId = clientId;
		this.userDefinedId = driverId;
		String baseUrl = "https://"+Location.LONDON.getTag()+".objectstorage.softlayer.net";
		try {
			this.conn = new Client(baseUrl, username, password, true);
			this.authParams = conn.createAuthParams();
		} catch (IOException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}

	@Override
	public void writeObject(String bucketName, String id, byte[] data,	String[] uploadToAnotherAccount) throws StorageCloudException {
		if (conn.isValidName(id)) {
			String uName;
			try {
				uName = conn.saferUrlEncode(bucketName);
				String fName = conn.saferUrlEncode(id);
				Representation representation = new InputRepresentation(new ByteArrayInputStream(data), MediaType.ALL);
				try{
					conn.put(authParams, representation, conn.getStorageUrl() + "/" + uName + "/" + fName);
				}catch(IOException e){
					if(createBucket(bucketName, e)){
						representation = new InputRepresentation(new ByteArrayInputStream(data), MediaType.ALL);
						conn.put(authParams, representation, conn.getStorageUrl() + "/" + uName + "/" + fName);
					}
				}
				//			return head.getFirstValue("Etag");
			} catch (EncoderException e) {
				e.printStackTrace();
				throw new StorageCloudException(e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				throw new StorageCloudException(e.getMessage());
			}
		} else {
			throw new StorageCloudException("invalid file name");
		}
	}

	@Override
	public byte[] readObjectData(String bucketName, String id,
			String[] uploadToAnotherAccount) throws StorageCloudException {
		try {
			URLCodec ucode = new URLCodec();
			String uName = ucode.encode(bucketName).replaceAll("\\+", "%20");
			String fName = ucode.encode(id).replaceAll("\\+", "%20");
			ClientResource clientRes = conn.get(authParams, conn.getStorageUrl() + "/" + uName + "/" + fName);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

			clientRes.getResponseEntity().write(outputStream);

			outputStream.close();
			return outputStream.toByteArray();
		}catch(IOException e){
			if(isNotFound(e)){
				return null;
			}
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		} catch (EncoderException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}

	@Override
	public void deleteObject(String bucketName, String id,
			String[] uploadToAnotherAccount) throws StorageCloudException {

		try {
			String uName = conn.saferUrlEncode(bucketName);
			String fName = conn.saferUrlEncode(id);
			conn.delete(authParams, conn.getStorageUrl() + "/" + uName + "/" + fName);
		}catch(IOException e){
			if(isNotFound(e)){
				return;
			}
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		} catch (EncoderException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}

	@Override
	public void deleteObjects(String bucketName, String[] ids,
			String[] uploadToAnotherAccount) {

		for(String id : ids){
			try {
				deleteObject(bucketName, id, uploadToAnotherAccount);
			} catch (StorageCloudException e) {}
		}
	}

	@Override
	public void deleteContainer(String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException {
		try {
			String uName = conn.saferUrlEncode(bucketName);
			conn.delete(authParams, conn.getStorageUrl() + "/" + uName);
		} catch (IOException e) {
			if(isNotFound(e)){
				return;
			}
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		} catch (EncoderException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}

	@Override
	public List<String> listContainer(String prefix, String bucketName,
			String[] uploadToAnotherAccount) throws StorageCloudException {

		try {
			String uName = conn.saferUrlEncode(bucketName);
			ClientResource clientRes = conn.get(authParams, conn.getStorageUrl() + "/" + uName);
			Representation entity = clientRes.getResponseEntity();
			String containers = entity.getText();
			StrTokenizer tokenize = new StrTokenizer(containers);
			tokenize.setDelimiterString("\n");
			String[] obj = tokenize.getTokenArray();
			List<String> objs = new ArrayList<String>();
			for (String token : obj) {
				objs.add(token);
			}

			return objs;
		} catch (IOException e) {
			if(isNotFound(e)){
				return new ArrayList<String>();
			}
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		} catch (EncoderException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}


	@Override
	public String getUserDefinedId() {
		return userDefinedId;
	}

	@Override
	public String getDriverId() {
		return DRIVER_ID;
	}

	public static Object getDriverType() {
		return DRIVER_ID;
	}

	private boolean createBucket(String bucketName, IOException e) throws IOException, EncoderException{
		if(isNotFound(e)){
			if (conn.isValidName(bucketName)) {
				String uName = conn.saferUrlEncode(bucketName);
				conn.put(authParams, null, conn.getStorageUrl() + "/" + uName);
				return true;
			} else {
				throw new EncoderException("Invalid Container Name");
			}
		}
		return false;
	}

	private boolean isNotFound(IOException e){
		if(e.getCause()!= null && e.getCause() instanceof ResourceException){
			ResourceException rExc = (ResourceException) e.getCause();
			if(rExc.getStatus().getCode() == 404){
				return true;
			}
		}
		return false;
	}

	public List<String> listBuckets() throws StorageCloudException{
		try {
			ClientResource res = conn.get(authParams, conn.getStorageUrl());
			Representation entity = res.getResponseEntity();
			String containers = entity.getText();
			StrTokenizer tokenize = new StrTokenizer(containers);
			tokenize.setDelimiterString("\n");
			String[] obj = tokenize.getTokenArray();
			List<String> objs = new ArrayList<String>();
			for (String token : obj) {
				objs.add(token);
			}

			return objs;
		} catch (IOException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}

}
