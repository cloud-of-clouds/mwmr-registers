package mwmr.clouds;

import java.util.List;

import mwmr.exceptions.StorageCloudException;

/**
 * This interface represents the contract that drivers must fulfill
 * @author rmendes
 */
public interface IKVSDriver {

	/**
	 * Method to obtain an unique identifier of this Driver.
	 * @return String with the identifier.
	 */
	public String getUserDefinedId();
	
    /**
     * @param sid
     * @param cid
     * @param data
     * @param id
     * @return
     * @throws Exception
     */
    public void writeObject(String bucketName, String id, byte[] data, String[] uploadToAnotherAccount) throws StorageCloudException;

    /**
     * Download file from cloud
     * @param sid
     * @param cid
     * @param id
     * @return data
     */
    public byte[] readObjectData(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException;

    /**
     * Delete file from cloud
     * @param sid
     * @param cid
     * @param id
     * @return success?
     */
    public void deleteObject(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException;

 
    /**
     * Delete drop/bucket/folder
     * @param sid
     * @param cid
     * @return success?
     */
    public void deleteObjects(String bucketName, String[] ids, String[] uploadToAnotherAccount) throws StorageCloudException;

    public void deleteContainer(String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException;
    /**
     * Load cloud storage driver and initiate session
     * @param sessionProperties
     * @return session id
     */
    
    public List<String> listContainer(String prefix, String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException;
    

	public String getDriverId();
	
}
