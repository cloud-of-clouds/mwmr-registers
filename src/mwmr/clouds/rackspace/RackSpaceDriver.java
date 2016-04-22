package mwmr.clouds.rackspace;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import mwmr.clouds.IKVSDriver;
import mwmr.exceptions.ServiceSiteException;
import mwmr.exceptions.StorageCloudException;

public class RackSpaceDriver implements IKVSDriver {

	private static final String DRIVER_ID = "RACKSPACE";

	private String userDefinedId;
	protected String defaultBucketName = "depskys";
	private String accessKey;
	private String secretKey;
	private final String accessURL = "https://lon.identity.api.rackspacecloud.com/v2.0/";
	private final String getToken = "tokens";
	private final String addUsers = "users";
	protected String tokenId;
	protected String operationURL;
	protected CloseableHttpClient client;
	protected HashMap<String, String[]> tokensToOthersAccounts;

	public RackSpaceDriver(int clientId, String driverID, String accessKey, String secretKey) throws StorageCloudException{
		this.userDefinedId = driverID;
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.tokensToOthersAccounts = new HashMap<String, String[]>();
		try {
			getBucketName();
		} catch (FileNotFoundException e) {
			System.out.println("Problem with bucket_name.properties file!");
		}

		initSession(clientId);
	}

	@Override
	public void writeObject(String bucketName, String id, byte[] data, String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = bucketName == null ? defaultBucketName : bucketName;
		CloseableHttpResponse response;
		try {
			if(uploadToAnotherAccount == null){
				HttpPut put;
				put = new HttpPut(operationURL+"/"+container+"/"+id);
				put.addHeader("X-Auth-Token", tokenId);
				put.addHeader("Accept", "application/json");
				HttpEntity entity = new ByteArrayEntity(data);
				put.setEntity(entity);
				response = client.execute(put);
				response.close();
				if(response.getStatusLine().getStatusCode() == 404){
					//					EntityUtils.consume(response.getEntity());

					HttpPut putCont = new HttpPut(operationURL+"/"+container);
					putCont.addHeader("X-Auth-Token", tokenId);
					putCont.addHeader("Accept", "application/json");	
					response = client.execute(putCont);
					//					EntityUtils.consume(response.getEntity());
					response.close();
					response = client.execute(put);
					response.close();
				}
			}else{
				authenticateAsSubAccount(uploadToAnotherAccount[0], uploadToAnotherAccount[1]);
				String[] acc = tokensToOthersAccounts.get(uploadToAnotherAccount[0]);
				HttpPut put = new HttpPut(acc[1]+"/"+container+"/"+id);
				put.addHeader("X-Auth-Token", acc[0]);
				put.addHeader("Accept", "application/json");
				HttpEntity entity = new ByteArrayEntity(data);
				put.setEntity(entity);
				response = client.execute(put);
				response.close();
				//				System.out.println(response);
				//				System.out.println(response.getStatusLine());
			}

			//			EntityUtils.consume(response.getEntity());
			if(response.getStatusLine().getStatusCode() == 404){
				throw new ServiceSiteException("RackSpaceException::" + "NoSuchBucket");
			}

		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		}
	}

	//uploadToAnotherAccount -> 1' position -> username, 2' position -> password (to access other account)
	@Override
	public byte[] readObjectData(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = bucketName == null ? defaultBucketName : bucketName;
		try {
			HttpGet get = null;
			if(uploadToAnotherAccount == null){
				get = new HttpGet(operationURL+"/"+container+"/"+id);
				get.addHeader("X-Auth-Token", tokenId);
				get.addHeader("Accept", "application/json");
			}else{
				authenticateAsSubAccount(uploadToAnotherAccount[0], uploadToAnotherAccount[1]);
				String[] acc = tokensToOthersAccounts.get(uploadToAnotherAccount[0]);
				get = new HttpGet(acc[1]+"/"+container+"/"+id);
				get.addHeader("X-Auth-Token", acc[0]);
				get.addHeader("Accept", "application/json");
			}
			CloseableHttpResponse response = client.execute(get);
			if(response.getStatusLine().getStatusCode() == 404){
				response.close();
				return null;
			}


			byte[] bytes = getBytesFromInputStream(response.getEntity().getContent());
			response.close();
			return bytes;
		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		}
	}

	@Override
	public List<String> listContainer(String prefix, String bucketName, String[] uploadToAnotherAccount) throws StorageCloudException {

		String container = bucketName == null ? defaultBucketName : bucketName;
		//String url = operationURL+"/"+container;
		if(prefix!=null)
			prefix = "?prefix="+prefix;
		else
			prefix = "";

		HttpGet get = null;
		if(uploadToAnotherAccount == null){
			get = new HttpGet(operationURL+"/"+container+prefix);
			get.addHeader("X-Auth-Token", tokenId);
			get.addHeader("Accept", "application/json");
		}else{
			authenticateAsSubAccount(uploadToAnotherAccount[0], uploadToAnotherAccount[1]);
			String[] acc = tokensToOthersAccounts.get(uploadToAnotherAccount[0]);
			get = new HttpGet(acc[1]+ "/" + container+prefix);
			get.addHeader("X-Auth-Token", acc[0]);
			get.addHeader("Accept", "application/json");
		}
		LinkedList<String> l = new LinkedList<String>();
		CloseableHttpResponse response = null;
		try {
			response = client.execute(get);
			if(response.getStatusLine().getStatusCode() == 404){
				response.close();
				return l;
				//				throw new ServiceSiteException("RackSpaceException::" + "NoSuchBucket");
			}
			JsonFactory f = new JsonFactory();
			HttpEntity entity = response.getEntity();
			if(entity == null || entity.getContent() == null){
				response.close();
				return l;
			}
			JsonParser jp = f.createJsonParser(entity.getContent());
			boolean next=false;
			JsonToken token;
			while((token = jp.nextToken()) != null){
				if(token == JsonToken.FIELD_NAME){
					if(jp.getCurrentName().equals("name"))
						next=true;
				}
				if(token == JsonToken.VALUE_STRING){
					if(next){
						l.add(jp.getText());
						next=false;
					}
				}
			}
		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		}
		try {
			if(response != null)
				response.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		}
		return l;
	}


	@Override
	public void deleteObject(String bucketName, String id, String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = bucketName == null ? defaultBucketName : bucketName;
		try {
			HttpDelete delete = null;
			if(uploadToAnotherAccount == null){
				delete = new HttpDelete(operationURL+"/"+container+"/"+id);
				delete.addHeader("X-Auth-Token", tokenId);
				delete.addHeader("Accept", "application/json");
			}else{
				authenticateAsSubAccount(uploadToAnotherAccount[0], uploadToAnotherAccount[1]);
				String[] acc = tokensToOthersAccounts.get(uploadToAnotherAccount[0]);
				delete = new HttpDelete(acc[1]+"/"+container+"/"+id);
				delete.addHeader("X-Auth-Token", acc[0]);
				delete.addHeader("Accept", "application/json");
			}
			CloseableHttpResponse response = client.execute(delete);
			response.close();

			//			boolean result = response.getStatusLine().getStatusCode() == 204 || response.getStatusLine().getStatusCode() == 404;
		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		}

	}
	@Override
	public void deleteObjects(String bucketName, String[] ids, String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = bucketName == null ? defaultBucketName : bucketName;
		HttpDelete delete = null;
		CloseableHttpResponse response;
		try {
			for(String name : ids){
				if(uploadToAnotherAccount == null){
					delete = new HttpDelete(operationURL+"/"+container+"/"+name);
					delete.addHeader("X-Auth-Token", tokenId);
					delete.addHeader("Accept", "application/json");
				}else{
					authenticateAsSubAccount(uploadToAnotherAccount[0], uploadToAnotherAccount[1]);
					String[] acc = tokensToOthersAccounts.get(uploadToAnotherAccount[0]);
					delete = new HttpDelete(acc[1]+"/"+container+"/"+name);
					delete.addHeader("X-Auth-Token", acc[0]);
					delete.addHeader("Accept", "application/json");
				}
				response = client.execute(delete);
				response.close();
			}
		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		}
	}

	@Override
	public void deleteContainer(String bucketName,
			String[] uploadToAnotherAccount) throws StorageCloudException {
		String container = bucketName == null ? defaultBucketName : bucketName;

		try {
			HttpDelete delete = new HttpDelete(operationURL+"/"+container+"/");
			delete.addHeader("X-Auth-Token", tokenId);
			delete.addHeader("Accept", "application/json");
			CloseableHttpResponse response;
			response = client.execute(delete);
			response.close();
		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		}
	}

	public String[] setAcl(String bucketNameToShare, String[] canonicalId, String permission) throws StorageCloudException {
		try {
			HttpPut put = new HttpPut(operationURL+"/"+bucketNameToShare);
			put.addHeader("X-Auth-Token", tokenId);
			put.addHeader("Accept", "application/json");	
			CloseableHttpResponse response = client.execute(put);
			response.close();
			String[] usersToAdd = addUser(canonicalId);
			HttpPost post = null;
			System.out.println();
			String names = "";
			for(int i = 0; i < usersToAdd.length; i+=2){
				if(i+2 < usersToAdd.length)
					names = names.concat(usersToAdd[i]+",");
				else
					names = names.concat(usersToAdd[i]);
			}

			HashMap<String, String> oldGrantess = getOldGrantees(bucketNameToShare);
			post = new HttpPost(operationURL+"/"+bucketNameToShare);
			post.addHeader("X-Auth-Token", tokenId);
			post.addHeader("Accept", "application/json");
			if(permission.equals("rw")){
				if(oldGrantess == null){
					post.addHeader("x-container-read", names);
					post.addHeader("x-container-write", names);
				}else{
					if(oldGrantess.containsKey("r"))
						post.addHeader("x-container-read", names+","+oldGrantess.get("r"));
					if(oldGrantess.containsKey("w"))
						post.addHeader("x-container-write", names+","+oldGrantess.get("w"));
				}
			}else if(permission.equals("r")){
				if(oldGrantess != null && oldGrantess.containsKey("r"))
					post.addHeader("x-container-read", names+","+oldGrantess.get("r"));
				else
					post.addHeader("x-container-read", names);
			}else if(permission.equals("w")){
				if(oldGrantess != null && oldGrantess.containsKey("w"))
					post.addHeader("x-container-write", names+","+oldGrantess.get("w"));
				else
					post.addHeader("x-container-write", names);
			}
			response = client.execute(post);
			response.close();
			if(response.getStatusLine().getStatusCode() == 404){
				response.close();
				throw new ServiceSiteException("RackSpaceException::" + "NoSuchBucket");
			}
			return usersToAdd;

		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		}

	}

	public String getDriverId() {
		return DRIVER_ID;
	}

	@Override
	public String getUserDefinedId() {
		return userDefinedId;
	}

	public static Object getDriverType() {
		return DRIVER_ID;
	}

	private void initSession(int clientId) throws StorageCloudException {

		try {
			String content = "{"+
					"\"auth\": {"+
					"\"RAX-KSKEY:apiKeyCredentials\": {"+
					"\"username\": \""+accessKey+"\","+
					"\"apiKey\": \""+ secretKey+ "\"" +
					"}}}";
			client = HttpClients.createDefault();
			//authenticate
			HttpPost post = new HttpPost(accessURL+getToken);
			post.addHeader("Content-Type", "application/json");
			post.addHeader("Accept", "application/json");
			HttpEntity entity;
			entity = new StringEntity(content);
			post.setEntity(entity);
			CloseableHttpResponse response = client.execute(post);

			//get token and operationURL using response
			JsonFactory f = new JsonFactory();
			JsonParser jp = f.createJsonParser(response.getEntity().getContent());

			JsonToken token;
			boolean tokenTag = false;
			boolean idTag = false;
			boolean nameTag = false;
			boolean isCloudFiles = false;
			boolean publicUrlTag = false;

			while((token = jp.nextToken()) != null){
				if(token == JsonToken.FIELD_NAME){
					if(jp.getCurrentName().equals("token"))
						tokenTag = true;
					else if(tokenTag && jp.getCurrentName().equals("id"))
						idTag = true;
					else if(jp.getCurrentName().equals("name"))
						nameTag = true;
					else if(isCloudFiles && jp.getCurrentName().equals("publicURL"))
						publicUrlTag = true;
				}
				if(token == JsonToken.VALUE_STRING){
					if(tokenTag && idTag){
						tokenId = jp.getText();
						tokenTag = idTag = false;
					}else if(nameTag && jp.getText().equals("cloudFiles")){
						isCloudFiles = true;
					}else if(publicUrlTag){
						operationURL = jp.getText();
						publicUrlTag = isCloudFiles = nameTag = false;
					}
				}
			}
			response.close();

			//create deafult container
			//			HttpPut put = new HttpPut(operationURL+"/"+defaultBucketName);
			//			put.addHeader("X-Auth-Token", tokenId);
			//			put.addHeader("Accept", "application/json");	
			//			
			//			response = client.execute(put);
			//			response.close();


		} catch (UnsupportedEncodingException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (ClientProtocolException e) {
			throw new ServiceSiteException("RackSpaceException::" + e.getMessage());
		} catch (IOException e) {
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		} catch (Exception e) {
			throw new StorageCloudException("RackSpaceException::" + e.getMessage());
		}

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
				defaultBucketName = defaultBucketName.concat(new String(randname));
				props.setProperty("bucketname", defaultBucketName);
				props.store(new FileOutputStream(path),"change");
			}else{
				defaultBucketName = name;
			}

		}catch(IOException e){  
			e.printStackTrace();  
		} 
	}

	//TODO: THOSE PRIVATE METHODS - handle mwmr.exceptions!!!!
	private String thisUserExists(String name, String email){

		try {

			HttpGet get = new HttpGet(accessURL+addUsers+"/?"+name+","+email+"");
			get.addHeader("X-Auth-Token", tokenId);
			get.addHeader("Accept", "application/json");
			CloseableHttpResponse response;
			response = client.execute(get);
			response.close();
			JsonFactory f = new JsonFactory();
			JsonParser jp = f.createJsonParser(response.getEntity().getContent());
			boolean idValue = false, emailValue = false;
			String userID = "";
			JsonToken token;
			while((token = jp.nextToken()) != null){
				if(token == JsonToken.FIELD_NAME){
					//					if(jp.getCurrentName().equals("username"))
					//						userNameValue=true;
					if(jp.getCurrentName().equals("email"))
						emailValue = true;
					if(jp.getCurrentName().equals("id"))
						idValue = true;
				}

				if(token == JsonToken.VALUE_STRING){
					//					if(userNameValue){
					//						if(name.equals(jp.getText())){
					//							correctName = true;
					//						}
					//						userNameValue = false;
					//					}
					if(emailValue){
						if(email.equals(jp.getText())){
							return userID;
						}
						emailValue = false;
					}
					if(idValue){
						userID = jp.getText();
						idValue = false;
					}
				}
			}

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	//user and password
	private void authenticateAsSubAccount(String username, String password){

		try {
			if(!tokensToOthersAccounts.containsKey(username)){
				String content = "{"+
						"\"auth\":{"+
						"\"RAX-KSKEY:apiKeyCredentials\": {"+
						"\"username\": \""+username+"\","+
						"\"apiKey\": \""+password+"\""+
						"}}}";

				HttpPost post = new HttpPost(accessURL+getToken);
				post.addHeader("Content-Type", "application/json");
				post.addHeader("Accept", "application/json");
				HttpEntity entity = new StringEntity(content);
				post.setEntity(entity);
				CloseableHttpResponse response = client.execute(post);
				//get token and operationURL using response
				//				System.out.println(response);

				//				BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
				//
				//				String output;
				//				System.out.println("Output from Server .... \n");
				//				while ((output = br.readLine()) != null) {
				//					System.out.println(output);
				//
				//				}

				JsonFactory f = new JsonFactory();
				JsonParser jp = f.createJsonParser(response.getEntity().getContent());

				JsonToken token;
				boolean tokenTag = false;
				boolean idTag = false;
				boolean nameTag = false;
				boolean isCloudFiles = false;
				boolean publicUrlTag = false;

				String tok = "";
				String URL = "";
				while((token = jp.nextToken()) != null){
					if(token == JsonToken.FIELD_NAME){
						if(jp.getCurrentName().equals("token"))
							tokenTag = true;
						else if(tokenTag && jp.getCurrentName().equals("id"))
							idTag = true;
						else if(jp.getCurrentName().equals("name"))
							nameTag = true;
						else if(isCloudFiles && jp.getCurrentName().equals("publicURL"))
							publicUrlTag = true;
					}
					if(token == JsonToken.VALUE_STRING){
						if(tokenTag && idTag){
							tok = jp.getText();
							tokenTag = idTag = false;
						}else if(nameTag && jp.getText().equals("cloudFiles")){
							isCloudFiles = true;
						}else if(publicUrlTag){
							URL = jp.getText();
							publicUrlTag = isCloudFiles = nameTag = false;
						}
					}
				}
				System.out.println("url: " + URL);
				System.out.println();
				System.out.println("token: " + tok);
				//				EntityUtils.consume(response.getEntity());
				String[] acc = {tok,URL};
				tokensToOthersAccounts.put(username, acc);
				response.close();
			}


		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String[] getCredOfSubUser(String userId){
		String[] creds = new String[2];
		try {
			HttpGet get = new HttpGet(accessURL+addUsers+"/"+userId+"/OS-KSADM/credentials");
			get.addHeader("X-Auth-Token", tokenId);
			get.addHeader("Accept", "application/json");
			CloseableHttpResponse response = client.execute(get);
			response.close();
			//							BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
			//			
			//							String output;
			//							System.out.println("Output from Server .... \n");
			//							while ((output = br.readLine()) != null) {
			//								System.out.println(output);
			//			
			//							}

			JsonFactory f = new JsonFactory();
			JsonParser jp = f.createJsonParser(response.getEntity().getContent());
			JsonToken token;
			boolean isName = false, isPass = false;
			while((token = jp.nextToken()) != null){
				if(token == JsonToken.FIELD_NAME){
					if(jp.getCurrentName().equals("username")){
						isName = true;
					}
					if(jp.getCurrentName().equals("apiKey")){
						isPass = true;
					}
				}

				if(token == JsonToken.VALUE_STRING){
					if(isName){
						creds[0] = jp.getText();
						isName = false;
					}
					if(isPass){
						creds[1] = jp.getText();
						isPass = false;
						return creds;
					}
				}
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private String[] addUser(String[] names){
		String name = "";
		String[] userPass = new String[names.length];
		try {
			for(int i = 0; i < names.length; i+=2){
				char[] randname = new char[10];
				for(int j = 0; j < 10; j++){
					char rand = (char)(Math.random() * 26 + 'a');
					randname[j] = rand;
				}
				//name = names[i].concat("-subuser-".concat(new String(randname)));
				name = names[i];
				String content = "{"+
						"\"user\": {"+
						"\"username\": \""+name+"\","+
						"\"email\": \""+names[i+1]+"\","+
						"\"enabled\": true"+
						"}"+
						"}";
				String userId = thisUserExists(name, names[i+1]);
				if(userId != null){
					String[] cred = getCredOfSubUser(userId);
					userPass[i] = cred[0];
					userPass[i+1] = cred[1];
				}else{
					HttpPost post = new HttpPost(accessURL+addUsers);
					post.addHeader("X-Auth-Token", tokenId);
					post.addHeader("Content-Type", "application/json");
					post.addHeader("Accept", "application/json");
					HttpEntity entity;
					entity = new StringEntity(content);
					post.setEntity(entity);
					CloseableHttpResponse response = client.execute(post);
					response.close();
					//									BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));
					//					
					//									String output;
					//									System.out.println("Output from Server .... \n");
					//									while ((output = br.readLine()) != null) {
					//										System.out.println(output);
					//					
					//									}


					//get token and operationURL using response
					JsonFactory f = new JsonFactory();
					JsonParser jp = f.createJsonParser(response.getEntity().getContent());

					JsonToken token;
					boolean passTag = false;
					while((token = jp.nextToken()) != null){
						if(token == JsonToken.FIELD_NAME){
							if(jp.getCurrentName().equals("id"))
								passTag = true;
						}
						if(token == JsonToken.VALUE_STRING){
							if(passTag){
								userPass[i+1] = getCredOfSubUser(jp.getText())[1];
								passTag = false;
							}
						}
					}	
					userPass[i] = name;
				}
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return userPass;
	}

	private HashMap<String, String> getOldGrantees(String container){

		HashMap<String, String> toRet = new HashMap<String, String>();
		try {
			HttpGet get = new HttpGet(operationURL+"/"+container);
			get.addHeader("X-Auth-Token", tokenId);
			get.addHeader("Accept", "application/json");
			CloseableHttpResponse response = client.execute(get);
			response.close();
			Header[] headers = response.getAllHeaders();
			for(Header h : headers){
				if(h.getName().equals("X-Container-Read")){
					toRet.put("r", h.getValue());
				}
				if(h.getName().equals("X-Container-Write")){
					toRet.put("w", h.getValue());
				}
			}
			if(toRet.size() > 0)
				return toRet;
			else 
				return null;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
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

	public List<String> listBuckets() throws StorageCloudException {
		HttpGet get = new HttpGet(operationURL+"/");
		get.addHeader("X-Auth-Token", tokenId);
		get.addHeader("Accept", "application/json");

		try {
			CloseableHttpResponse response = client.execute(get);
			HttpEntity entity = response.getEntity();

			List<String> l = new ArrayList<String>();
			if(entity == null || entity.getContent() == null){
				response.close();
			}
			JsonFactory f = new JsonFactory();
			JsonParser jp = f.createJsonParser(entity.getContent());
			boolean next=false;
			JsonToken to;
			while((to = jp.nextToken()) != null){
				if(to == JsonToken.FIELD_NAME){
					if(jp.getCurrentName().equals("name"))
						next=true;
				}
				if(to == JsonToken.VALUE_STRING){
					if(next){
						l.add(jp.getText());
						next=false;
					}
				}
			}
			response.close();
			return l;
		} catch (IOException e) {
			e.printStackTrace();
			throw new StorageCloudException(e.getMessage());
		}
	}

}
