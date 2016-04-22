package mwmr.client;

public class DataUnit {

	private String containerName;
	private String objectName;
	private CloudsCredentials credentialToOtherAccount;

	public DataUnit(String containerName) {
		this(containerName, null, null);
	}

	public DataUnit(String containerName, String objectName) {
		this(containerName, objectName, null);
	}

	public DataUnit(String containerName, String objectName, CloudsCredentials thirdPartyDUCredentials) {
		this.containerName = containerName;
		this.objectName = objectName;
		this.credentialToOtherAccount = thirdPartyDUCredentials;
	}

	public String getContainerName() {
		return containerName;
	}

	public String getObjectName() {
		return objectName;
	}

	public CloudsCredentials getCloudsCredentials() {
		return credentialToOtherAccount;
	}

	public String[] getCloudCredential(String drvId) {
		if(credentialToOtherAccount==null)
			return null;
		return credentialToOtherAccount.getCredentials(drvId);
	}
}
