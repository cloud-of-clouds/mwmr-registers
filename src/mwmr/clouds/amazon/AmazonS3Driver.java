package mwmr.clouds.amazon;


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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import mwmr.clouds.IKVSDriver;
import mwmr.exceptions.ClientServiceException;
import mwmr.exceptions.ServiceSiteException;
import mwmr.exceptions.StorageCloudException;


public class AmazonS3Driver implements IKVSDriver {

	private static final String DRIVER_ID = "AMAZON-S3";
	private AmazonS3Client conn = null;
	private String defaultBucketName = "mwmr";
	private String accessKey, secretKey;
	private String userDefinedId;
	private Region region = null;

	public AmazonS3Driver(int clientId, String driverId, String accessKey, String secretKey) throws StorageCloudException {
		this(clientId, driverId, accessKey, secretKey, null);
	}

	/**
	 * Class that interact directly with amazon s3 api 
	 *
	 * @author tiago oliveira
	 * @author Ricardo Mendes
	 * @throws StorageCloudException 
	 */
	public AmazonS3Driver(int clientId, String driverId, String accessKey, String secretKey, Region region) throws StorageCloudException {
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.userDefinedId = driverId;

		try {
			getBucketName();
		} catch (FileNotFoundException e) {
			System.out.println("Problem with bucket_name.properties file!");
		}

		defaultBucketName = defaultBucketName.concat("-" + userDefinedId);
		if(region == null)
			this.region=Region.EU_Ireland;
		else
			this.region = region;

		initSession();
	}

	/*************************************************************************************************************************/
	/********************************                     PUBLIC METHODS                    **********************************/
	/*************************************************************************************************************************/

	/**
	 * writes the value 'data' in the file 'id'
	 */
	@Override
	public void writeObject(String bucketName, String id, byte[] data, String[] canonicalIDs) throws StorageCloudException {
		String container = getContainerName(bucketName);

		try {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(data.length);
			ByteArrayInputStream in = new ByteArrayInputStream(data);

			PutObjectRequest request = new PutObjectRequest(container, id, in, metadata);

			// associate an ACL to the object
			if(canonicalIDs !=null){
				AccessControlList acl = new AccessControlList();
				for(int i = 0; i < canonicalIDs.length; i++){
					acl.grantPermission(new CanonicalGrantee(canonicalIDs[i]), Permission.Read);
				}
				request = request.withAccessControlList(acl);	
			}

			try{				
				conn.putObject(request);	
			}catch (AmazonS3Exception e){
				if(isNotExistsException(e) || !conn.doesBucketExist(container)){
					conn.createBucket(container, region);
					conn.putObject(request);
				}else{
					in.close();
					throw e;
				}
			}
			in.close();
		} catch (AmazonServiceException e1) {
			throw new ServiceSiteException("AWSS3Exception::" + e1.getMessage());
		} catch (AmazonClientException e2) {
			throw new ClientServiceException("AWSS3Exception::" + e2.getMessage());
		} catch (IOException e3) {
			throw new StorageCloudException("AWSS3Exception::" + e3.getMessage());
		}
	}

	/**
	 * download the content of the file 'id'
	 */
	@Override
	public byte[] readObjectData(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {

		String container = getContainerName(bucketName);
		try {
			S3Object object = conn.getObject(new GetObjectRequest(container, id));
			byte[] array = getBytesFromInputStream(object.getObjectContent());

			object.getObjectContent().close();
			return array;
		} catch (AmazonServiceException e1) {
			if(isNotExistsException(e1))
				return null;
			throw new ServiceSiteException("AWSS3Exception::" + e1.getMessage());
		} catch (AmazonClientException e2) {
			throw new ClientServiceException("AWSS3Exception::" + e2.getMessage());
		} catch (IOException e3) {
			e3.printStackTrace();
			throw new StorageCloudException("AWSS3Exception::" + e3.getMessage());
		}
	}

	@Override

	public List<String> listContainer(String prefix, String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException {
		LinkedList<String> find = new LinkedList<String>();
		String container = getContainerName(bucketName);
		boolean moreToList = true;
		try{
			String marker = null;
			while(moreToList){
				ObjectListing objectListing = conn.listObjects(new ListObjectsRequest()
						.withBucketName(container).withPrefix(prefix).withMarker(marker));
				moreToList = objectListing.isTruncated();
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) 
					find.add(objectSummary.getKey());

				marker = find.getLast();
			}
		} catch (AmazonServiceException e1) {
			if(isNotExistsException(e1))
				return find;
			throw new ServiceSiteException("AWSS3Exception::" + e1.getMessage());
		} catch (AmazonClientException e2) {
			throw new ClientServiceException("AWSS3Exception::" + e2.getMessage());
		}

		return find;
	}

	/**
	 * delete the file 'id'
	 */
	@Override
	public void deleteObject(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = getContainerName(bucketName);

		try {	
			conn.deleteObject(container, id);
		} catch (AmazonServiceException e1) {
			if(isNotExistsException(e1))
				return;
			throw new ServiceSiteException("AWSS3Exception::" + e1.getMessage());
		} catch (AmazonClientException e2) {
			throw new ClientServiceException("AWSS3Exception::" + e2.getMessage());
		}
	}


	@Override
	public void deleteObjects(String bucketName, String[] ids, String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = getContainerName(bucketName);
		for(String fileId : ids){
			try {		
				conn.deleteObject(container, fileId);
			} catch (AmazonServiceException e1) {
				if(isNoSuckKeyException(e1))
					continue;
				throw new ServiceSiteException("AWSS3Exception::" + e1.getMessage());
			} catch (AmazonClientException e2) {
				throw new ClientServiceException("AWSS3Exception::" + e2.getMessage());
			}
		}
	}

	@Override
	public void deleteContainer(String bucketName,	String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = getContainerName(bucketName);

		List<String> names = listContainer("", container, uploadToAnotherAccount);
		try {	
			while(names.size()>0){
				deleteObjects(container, names.toArray(new String[names.size()]), uploadToAnotherAccount);
				names = listContainer("", container, uploadToAnotherAccount);
			}
			conn.deleteBucket(container);
		} catch (AmazonServiceException e1) {
			if(isNotExistsException(e1))
				return;
			throw new ServiceSiteException("AWSS3Exception::" + e1.getMessage());
		} catch (AmazonClientException e2) {
			throw new ClientServiceException("AWSS3Exception::" + e2.getMessage());
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


	public static String getDriverType() {
		return DRIVER_ID;
	}


	/*************************************************************************************************************************/
	/********************************                    PRIVATE METHODS                    **********************************/
	/*************************************************************************************************************************/

	/**
	 * Make the connection with Amazon S3
	 */
	private void initSession() throws StorageCloudException {
		try {
			String mprops = "accessKey=" + accessKey + "\r\n"
					+ "secretKey = " + secretKey;
			PropertiesCredentials b = new PropertiesCredentials( new ByteArrayInputStream(mprops.getBytes()));

			conn = new AmazonS3Client(b);		
			conn.setEndpoint("http://s3.amazonaws.com"); //for working in Virtual Box

			//			if(!conn.doesBucketExist(defaultBucketName)){
			//				conn.createBucket(defaultBucketName, region);
			//			}

		} catch (IOException e) {
			//System.out.println("Cannot connect with Amazon S3.");
			//e.printStackTrace();
			throw new StorageCloudException(StorageCloudException.INVALID_SESSION);
		}
	}

	private String getContainerName(String bucketName) {
		return (bucketName == null) ? defaultBucketName : bucketName.concat("-" + userDefinedId);
	}

	private boolean isNoSuckKeyException(AmazonServiceException e){
		return (e.getErrorCode().equals("NoSuchKey"));
	}

	private boolean isNotExistsException(AmazonServiceException e) {
		return (isNoSuckBucketException(e) || isNoSuckKeyException(e));
	}

	private boolean isNoSuckBucketException(AmazonServiceException e){
		return (e.getErrorCode().equals("NoSuchBucket"));
	}

	private byte[] getBytesFromInputStream(InputStream is)
			throws IOException {

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int nRead;
		byte[] data = new byte[16384];

		while ((nRead = is.read(data, 0, data.length)) != -1) {
			buffer.write(data, 0, nRead);
		}

		buffer.flush();

		return buffer.toByteArray();
	}

	private String getBucketName() throws FileNotFoundException{
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
				defaultBucketName = defaultBucketName.concat(new String(randname));
				props.setProperty("bucketname", defaultBucketName);
				props.store(new FileOutputStream(path),"change");
			}else{
				defaultBucketName = name;
			}
		}catch(IOException e){  
			e.printStackTrace();  
		} 
		return null;
	}

}
