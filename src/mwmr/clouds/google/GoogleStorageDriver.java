package mwmr.clouds.google;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.jets3t.service.ServiceException;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.acl.gs.GSAccessControlList;
import org.jets3t.service.acl.gs.UserByEmailAddressGrantee;
import org.jets3t.service.impl.rest.httpclient.GoogleStorageService;
import org.jets3t.service.model.GSObject;
import org.jets3t.service.security.GSCredentials;

import mwmr.clouds.IKVSDriver;
import mwmr.exceptions.ServiceSiteException;
import mwmr.exceptions.StorageCloudException;

/**
 * Class that interact directly with google storage api
 * @author tiago oliveira
 *
 */
public class GoogleStorageDriver implements IKVSDriver {

	private static final String DRIVER_ID = "GOOGLE-STORAGE";
	private GoogleStorageService gsService;
	private String userDefinedId;
	private String defaultBacketName = "depskys";
	private String accessKey;
	private String secretKey;

	public GoogleStorageDriver(int clientId, String driverID, String accessKey, String secretKey) throws StorageCloudException{
		this.userDefinedId = driverID;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		try {
			getBucketName();
		} catch (FileNotFoundException e) {
			System.out.println("Problem with bucket_name.properties file!");
		}

		initSession(clientId);
	}

	/*************************************************************************************************************************/
	/********************************                     PUBLIC METHODS                    **********************************/
	/*************************************************************************************************************************/

	@Override
	public void writeObject(String bucketName, String id, byte[] data, String[] uploadToAnotherAccount) throws StorageCloudException {
		try {
			GSObject object = new GSObject(id);
			ByteArrayInputStream in = new ByteArrayInputStream(data);
			object.setDataInputStream(in);
			object.setContentLength(data.length);

			String bucketToWrite = (bucketName == null) ? defaultBacketName : bucketName;

			// associate an ACL with the object
			if(uploadToAnotherAccount !=null){
				GSAccessControlList acl = new GSAccessControlList();
				for(int i = 0; i < uploadToAnotherAccount.length; i++){
					acl.grantPermission(new UserByEmailAddressGrantee(uploadToAnotherAccount[i]), Permission.PERMISSION_READ);
				}
				object.setAcl(acl);
			}


			try {
				gsService.putObject(bucketToWrite, object);
			} catch (ServiceException e1) {
				if(isNotExistsException(e1) || gsService.checkBucketStatus(bucketToWrite)==1){
					gsService.createBucket(bucketToWrite);
					object = new GSObject(id);
					in = new ByteArrayInputStream(data);
					object.setDataInputStream(in);
					object.setContentLength(data.length);
					gsService.putObject(bucketToWrite, object);
				}else
					throw e1;
			}
		} catch (ServiceException e1) {
			throw new ServiceSiteException("GoogleStorageException::" + e1.getMessage());
		} catch (Exception e2) {
			throw new StorageCloudException("GoogleStorageException::" + e2.getMessage());
		}
	}

	@Override
	public byte[] readObjectData(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {
		String bucketToRead = (bucketName == null) ? defaultBacketName : bucketName;

		try {
			GSObject objectComplete = gsService.getObject(bucketToRead, id);
			InputStream in = objectComplete.getDataInputStream();
			byte[] array = getBytesFromInputStream(in);

			return array;
		} catch (ServiceException e) {
			if(isNotExistsException(e))
				return null;
			throw new ServiceSiteException("GoogleStorageException::" + e.getMessage());
		} catch (IOException e) {
			throw new StorageCloudException("GoogleStorageException::" + e.getMessage());
		} catch (Exception e) {
			throw new StorageCloudException("GoogleStorageException::" + e.getMessage());
		}
	}

	@Override
	public List<String> listContainer(String prefix, String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException {

		LinkedList<String> find = new LinkedList<String>();
		String bucketToUse = (bucketName == null) ? defaultBacketName : bucketName;
		try {
			GSObject[] objectListing = gsService.listObjects(bucketToUse, prefix, null);
			for(GSObject obj : objectListing)
				find.add(obj.getName());

			return find;
		} catch (ServiceException e1) {
			try {
				if(isNotExistsException(e1) || gsService.checkBucketStatus(bucketToUse)==1)
					return find;
			} catch (ServiceException e) {
				e.printStackTrace();
			}
			throw new ServiceSiteException("GoogleStorageException::" + e1.getMessage());
		} catch (Exception e2) {
			throw new StorageCloudException("GoogleStorageException::" + e2.getMessage());
		}

	}

	@Override
	public void deleteObjects(String bucketName, String[] ids, String[] uploadToAnotherAccount) throws StorageCloudException {
		String bucketToUse = bucketName == null ? defaultBacketName : bucketName;
		for(String str : ids){
			try {
				gsService.deleteObject(bucketToUse, str);
			} catch (ServiceException e) {
				if(!isNoSuckKeyException(e))
					throw new ServiceSiteException("GoogleStorageException::" + e.getMessage());
			}
		}
	}

	@Override
	public void deleteObject(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {
		String bucketToUse = bucketName == null ? defaultBacketName : bucketName;
		try {
			gsService.deleteObject(bucketToUse, id);
		} catch (ServiceException e) {
			if(!isNoSuckKeyException(e))
				throw new ServiceSiteException("GoogleStorageException::" + e.getMessage());
		}
	}

	@Override
	public void deleteContainer(String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException {
		List<String> names = listContainer("", bucketName, uploadToAnotherAccount);
		deleteObjects(bucketName, names.toArray(new String[names.size()]), uploadToAnotherAccount);

		try {
			gsService.deleteBucket(bucketName);
		} catch (ServiceException e) {
			if(!isNoSuckBucketException(e))
				throw new ServiceSiteException("GoogleStorageException::" + e.getMessage());
		}
	}

	public String[] setAcl(String bucketNameToShare, String[] canonicalId, String permission) throws StorageCloudException {
		try {
			boolean withRead = false;
			if(bucketNameToShare != null){
				gsService.getOrCreateBucket(bucketNameToShare);
			}else{
				return null;
			}

			GSAccessControlList acl = gsService.getBucketAcl(bucketNameToShare);
			for(int i = 0; i < canonicalId.length; i++){
				UserByEmailAddressGrantee user = new UserByEmailAddressGrantee(canonicalId[i]);
				if(permission.equals("rw")){
					//TODO: Nao pode ser full control. Arranjar solucao para dar leitura e escrita em separado
					acl.grantPermission(user, Permission.PERMISSION_FULL_CONTROL);
					//acl.grantPermission(user, Permission.PERMISSION_READ);
					withRead = true;
				}else if(permission.equals("r")){
					acl.grantPermission(new UserByEmailAddressGrantee(canonicalId[i]), Permission.PERMISSION_READ);
					withRead = true;
				}else if(permission.equals("w"))
					acl.grantPermission(new UserByEmailAddressGrantee(canonicalId[i]), Permission.PERMISSION_WRITE);
			}

			if(withRead){
				//StorageOwner bucketOwner = acl.getOwner();
				//System.out.println("-- " + bucketOwner.getId());
				GSObject[] objects = gsService.listObjects(bucketNameToShare);
				GSAccessControlList aclKeys = null;
				for(GSObject elem: objects) {
					aclKeys = (GSAccessControlList) gsService.getObjectAcl(bucketNameToShare, elem.getName());
					for(int i = 0; i < canonicalId.length; i++){
						aclKeys.grantPermission(new UserByEmailAddressGrantee(canonicalId[i]), Permission.PERMISSION_READ);
					}
					gsService.putObjectAcl(bucketNameToShare, elem.getKey(), aclKeys);
				}
			}
			gsService.putBucketAcl(bucketNameToShare, acl);

			return canonicalId;
		} catch (ServiceException e1) {
			//e.printStackTrace();
			throw new ServiceSiteException("GoogleStorageException::" + e1.getMessage());
		} catch (Exception e2) {
			throw new StorageCloudException("GoogleStorageException::" + e2.getMessage());
		}
	}


	@Override
	public String getDriverId() {
		return DRIVER_ID;
	}


	@Override
	public String getUserDefinedId() {
		return userDefinedId;
	}

	public static String getDriverType() {
		return DRIVER_ID;
	}


	/*************************************************************************************************************************/
	/********************************                    PRIVATE METHODS                    **********************************/
	/*************************************************************************************************************************/

	private void initSession(int clientId) throws StorageCloudException {
		try {
			GSCredentials gsCredentials = new GSCredentials(accessKey, secretKey);
			gsService = new GoogleStorageService(gsCredentials);

		} catch (ServiceException e) {
			throw new StorageCloudException("GoogleStorageException::" + StorageCloudException.INVALID_SESSION);
		}

		//		try {
		//			gsService.getOrCreateBucket(defaultBacketName);
		//		} catch (ServiceException e) {}
	}

	private boolean isNotExistsException(ServiceException e) {
		return (isNoSuckBucketException(e) || isNoSuckKeyException(e));
	}

	private boolean isNoSuckBucketException(ServiceException e){
		return (e.getErrorCode().equals("NoSuchBucket"));
	}

	private boolean isNoSuckKeyException(ServiceException e){
		return (e.getErrorCode().equals("NoSuchKey"));
	}

	private static byte[] getBytesFromInputStream(InputStream is) throws IOException {

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int nRead;
		byte[] data = new byte[16384];

		while ((nRead = is.read(data, 0, data.length)) != -1) {
			buffer.write(data, 0, nRead);
		}

		buffer.flush();

		return buffer.toByteArray();
	}

	private void getBucketName() throws FileNotFoundException{

		String path = "config" + File.separator + "bucket_name.properties";
		FileInputStream fis;
		try {
			fis = new FileInputStream(path);
			Properties props = new Properties();  
			props.load(fis);  
			fis.close();  
			String name = props.getProperty("bucketname");
			if(name.length() == 0){
				char[] randname = new char[10];
				for(int i = 0; i < 10; i++){
					char rand = (char)(Math.random() * 26 + 'a');
					randname[i] = rand;
				}
				defaultBacketName = defaultBacketName.concat(new String(randname));
				props.setProperty("bucketname", defaultBacketName);
				props.store(new FileOutputStream(path),"change");
			}else{
				defaultBacketName = name;
			}

		}catch(IOException e){  
			e.printStackTrace();  
		} 
	}

}
