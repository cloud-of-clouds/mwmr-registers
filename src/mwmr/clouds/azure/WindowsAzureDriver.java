//package mwmr.clouds.azure;
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.security.InvalidKeyException;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.NoSuchElementException;
//import java.util.Properties;
//
//import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
//import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
//import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
//import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
//import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
//import com.microsoft.windowsazure.services.core.storage.StorageException;
//
//import mwmr.clouds.IKVSDriver;
//import mwmr.exceptions.ServiceSiteException;
//import mwmr.exceptions.StorageCloudException;
//
//
//public class WindowsAzureDriver implements IKVSDriver {
//
//	private static final String DRIVER_ID = "WINDOWS-AZURE";
//
//	private String userDefinedId;
//	private String accessKey, secretKey;
//	private String defaultBucketName = "depskys";
//	private CloudBlobClient blobClient;
//
//	public WindowsAzureDriver(int clientId, String driverId, String accessKey, String secretKey) throws StorageCloudException{
//
//		this.userDefinedId = driverId;
//		this.accessKey = accessKey;
//		this.secretKey = secretKey;
//
//		try {
//			getBucketName();
//		} catch (FileNotFoundException e) {
//			System.out.println("Problem with bucket_name.properties file!");
//		}
//
//		initSession(clientId);
//	}
//
//	/*************************************************************************************************************************/
//	/********************************                     PUBLIC METHODS                    **********************************/
//	/*************************************************************************************************************************/
//
//	@Override
//	public void writeObject(String bucketName, String id, byte[] data, String[] uploadToAnotherAccount) throws StorageCloudException {
//
//		String bucketToWrite = (bucketName == null) ? defaultBucketName : bucketName;
//		try {
//			CloudBlobContainer container = null;
//			if(uploadToAnotherAccount == null)
//				container = blobClient.getContainerReference(bucketToWrite);
//			else{
//				//upload via SAS
//				String[] sharedAccess = uploadToAnotherAccount[0].split("/"+bucketToWrite);
//				CloudBlobClient blobclient = new CloudBlobClient(new URI(sharedAccess[0]));
//
//				URI uri = new URI(blobclient.getEndpoint().toString()+"/"+bucketToWrite+"?"+sharedAccess[1]);
//				container = new CloudBlobContainer(uri, blobclient);
//			}
//
//			CloudBlockBlob blob = container.getBlockBlobReference(id);
//			try{
//				blob.upload(new ByteArrayInputStream(data), data.length);
//			} catch (StorageException e) {
//				if(isNotExistsException(e)){
//					container.createIfNotExist();
//					blob.upload(new ByteArrayInputStream(data), data.length);
//				}else
//					throw e;
//			}
//		} catch (URISyntaxException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		} catch (StorageException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		} catch (IOException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		}
//	}
//
//	@Override
//	public byte[] readObjectData(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {
//
//		String bucketToWrite = (bucketName == null) ? defaultBucketName : bucketName;
//
//		try {
//			CloudBlobContainer container = null;
//			if(uploadToAnotherAccount == null)
//				container = blobClient.getContainerReference(bucketToWrite);
//			else{
//				String[] sharedAccess = uploadToAnotherAccount[0].split("/"+bucketToWrite);
//				CloudBlobClient blobclient = new CloudBlobClient(new URI(sharedAccess[0]));
//
//				URI uri = new URI(blobclient.getEndpoint().toString()+"/"+bucketToWrite+"?"+sharedAccess[1]);
//				container = new CloudBlobContainer(uri, blobclient);
//			}
//
//			CloudBlockBlob blob = container.getBlockBlobReference(id);
//			ByteArrayOutputStream out = new ByteArrayOutputStream();
//			blob.download(out);
//			return out.toByteArray();
//
//		} catch (StorageException e) {
//			if(isNotExistsException(e))
//				return null;
//			else{
//				System.out.println("e.getErrorCode() = " + e.getErrorCode());
//				throw new StorageCloudException("AzureStorageException::" + e.getMessage());
//			}
//		} catch (URISyntaxException e) {
//			throw new StorageCloudException("AzureStorageException::" + e.getMessage());
//		} catch (IOException e) {
//			throw new StorageCloudException("AzureStorageException::" + e.getMessage());
//		}
//
//	}
//
//	@Override
//	public List<String> listContainer(String prefix, String bucketName,	String[] uploadToAnotherAccount) throws StorageCloudException {
//		String bucketToWrite = (bucketName == null) ? defaultBucketName : bucketName;
//		LinkedList<String> find = new LinkedList<String>();
//
//		try {
//			if(uploadToAnotherAccount != null){
//				String[] sharedAccess = uploadToAnotherAccount[0].split("/"+bucketToWrite);
//				URI baseuri = new URI(sharedAccess[0]);
//				CloudBlobClient blobclient = new CloudBlobClient(baseuri);
//				return listViaSAS(bucketToWrite, prefix, sharedAccess[1], blobclient);
//			}
//
//			CloudBlobContainer container = blobClient.getContainerReference(bucketToWrite);
//			Iterable<ListBlobItem> listOfNames = container.listBlobs(prefix);
//			try{
//				for(ListBlobItem item : listOfNames){
//					String[] name = item.getUri().getPath().split("/");
//					find.add(name[name.length-1]);
//				}
//			}catch(NoSuchElementException e){
//
//			}
//			return find;
//
//		} catch (StorageException e1) {
//			if(isNotExistsException(e1))
//				return find;
//			throw new ServiceSiteException("AzureStorageException::" + e1.getMessage());
//		} catch (URISyntaxException e2) {
//			throw new ServiceSiteException("AzureStorageException::" + e2.getMessage());
//		}
//	}
//
//	@Override
//	public void deleteObjects(String bucketName, String[] ids, String[] uploadToAnotherAccount) throws StorageCloudException {
//		String bucketToWrite = (bucketName == null) ? defaultBucketName : bucketName;
//
//		for(String str : ids)
//			deleteObject(bucketToWrite, str, uploadToAnotherAccount);
//	}
//
//	@Override
//	public void deleteObject(String bucketName, String fileId, String[] uploadToAnotherAccount) throws StorageCloudException {
//		String bucketToWrite = (bucketName == null) ? defaultBucketName : bucketName;
//		try {
//			if(uploadToAnotherAccount != null){
//				String[] sharedAccess = uploadToAnotherAccount[0].split("/"+bucketToWrite);
//				URI baseuri = new URI(sharedAccess[0]);
//				CloudBlobClient blobclient = new CloudBlobClient(baseuri);
//				deleteViaSAS(bucketToWrite, fileId, sharedAccess[1], blobclient);
//			}else{
//				CloudBlobContainer container = blobClient.getContainerReference(bucketToWrite);
//				CloudBlockBlob blob = container.getBlockBlobReference(fileId);
//				blob.deleteIfExists();
//			}
//		} catch (StorageException e) {
//			if(isNotExistsException(e))
//				return;
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		} catch (URISyntaxException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		} catch (FileNotFoundException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		} catch (IOException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		}
//	}
//
//	@Override
//	public void deleteContainer(String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException {
//
//		String bucketToWrite = (bucketName == null) ? defaultBucketName : bucketName;
//
//		List<String> names = listContainer("", bucketToWrite, uploadToAnotherAccount);
//		deleteObjects(bucketToWrite, names.toArray(new String[names.size()]), uploadToAnotherAccount);
//
//		try {
//			CloudBlobContainer container = blobClient.getContainerReference(bucketToWrite);
//			container.deleteIfExists();
//		} catch (URISyntaxException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		} catch (StorageException e) {
//			throw new ServiceSiteException("AzureStorageException::" + e.getMessage());
//		}
//	}
//
//
//	@Override
//	public String getDriverId() {
//		return DRIVER_ID;
//	}
//
//	@Override
//	public String getUserDefinedId() {
//		return userDefinedId;
//	}
//
//
//	public static String getDriverType() {
//		return DRIVER_ID;
//	}
//
//	/*************************************************************************************************************************/
//	/********************************                    PRIVATE METHODS                    **********************************/
//	/*************************************************************************************************************************/
//
//	private void initSession(int clientId) throws StorageCloudException {
//
//		String storageConnectionString = 
//				"DefaultEndpointsProtocol=https;" + 
//						"AccountName=" + accessKey + ";" + 
//						"AccountKey=" + secretKey + ";";
//
//		try {
//			// Retrieve storage account from connection-string
//			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
//			blobClient = storageAccount.createCloudBlobClient();
//			//			CloudBlobContainer container = blobClient.getContainerReference(defaultBucketName);
//			//			container.createIfNotExist();
//
//		} catch (URISyntaxException e) {
//			throw new StorageCloudException("AzureStorageException::" + e.getMessage());
//			//		} catch (StorageException e) {
//			//			throw new StorageCloudException("AzureStorageException::" + e.getMessage());
//		} catch (InvalidKeyException e) {
//			throw new StorageCloudException("AzureStorageException::" + e.getMessage());
//		}	
//
//	}
//
//	private boolean isNotExistsException(StorageException e) {
//		return (isNoSuckBucketException(e) || isNoSuckKeyException(e)); 
//	}
//
//	private boolean isNoSuckBucketException(StorageException e){
//		return (e.getErrorCode().equals("ContainerNotFound"));
//	}
//
//	private boolean isNoSuckKeyException(StorageException e){
//		return (e.getErrorCode().equals("BlobNotFound"));
//	}
//
//	private boolean deleteViaSAS(String bucketName, String fileId, String sas, CloudBlobClient blobclient)
//			throws URISyntaxException, FileNotFoundException, IOException, StorageException{
//
//		URI uri = new URI(blobclient.getEndpoint().toString()+"/"+bucketName+"/"+fileId+"?"+sas);
//		CloudBlockBlob sasBlob = new CloudBlockBlob(uri, blobclient);
//		sasBlob.deleteIfExists();
//		return true;
//	} 
//
//	private LinkedList<String> listViaSAS(String bucketName, String prefix, String sas, CloudBlobClient blobclient) 
//			throws URISyntaxException, StorageException{
//
//		LinkedList<String> find = new LinkedList<String>();
//		URI uri = new URI(blobclient.getEndpoint().toString()+"/"+bucketName+"?"+sas);
//		CloudBlobContainer container = new CloudBlobContainer(uri, blobclient);
//		Iterable<ListBlobItem> listOfNames = container.listBlobs(prefix);
//		for(ListBlobItem item : listOfNames){
//			String[] name = item.getUri().getPath().split("/");
//			find.add(name[name.length-1]);
//		}
//
//		return find;
//	}
//
//	private String getBucketName() throws FileNotFoundException{
//		String path = "config" + File.separator + "bucket_name.properties";
//		FileInputStream fis;
//		try {
//			fis = new FileInputStream(path);
//			Properties props = new Properties();  
//			props.load(fis);  
//			fis.close();  
//			String name = props.getProperty("bucketname");
//			if(name.length() == 0){
//				char[] randname = new char[10];
//				for(int i = 0; i < 10; i++){
//					char rand = (char)(Math.random() * 26 + 'a');
//					randname[i] = rand;
//				}
//				defaultBucketName = defaultBucketName.concat(new String(randname));
//				props.setProperty("bucketname", defaultBucketName);
//				props.store(new FileOutputStream(path),"change");
//			}else{
//				defaultBucketName = name;
//			}
//		}catch(IOException e){  
//			e.printStackTrace();  
//		} 
//		return null;
//	}
//
//}
