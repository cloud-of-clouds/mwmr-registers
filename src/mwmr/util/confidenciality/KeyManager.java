package mwmr.util.confidenciality;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import mwmr.util.Constants;
import pvss.ErrorDecryptingException;
import pvss.InvalidVSSScheme;
import pvss.PVSSEngine;
import pvss.PublicInfo;
import pvss.PublishedShares;
import pvss.Share;

public class KeyManager {
	/**
	 * Loads a public key
	 *
	 * @return the PrivateKey loaded from config/keys/publickey<id>
	 * @throws Exception problems reading or parsing the key
	 */
	public static PublicKey loadPublicKey(int id) throws Exception {
		ObjectInputStream ois = null;
		if(id == -1)
			ois = new ObjectInputStream(new FileInputStream(Constants.PUBLIC_KEYS_PATH + File.separator +"mwmr.pub"));
		else
			ois = new ObjectInputStream(new FileInputStream(Constants.PUBLIC_KEYS_PATH + File.separator +"publickey" + id));
		PublicKey puk = (PublicKey) ois.readObject();

		ois.close();
		return puk;
	}

	/**
	 * Loads a private key
	 *
	 * @return the PrivateKey loaded from config/keys/publickey<id>
	 * @throws Exception problems reading or parsing the key
	 */
	public static PrivateKey loadPrivateKey(int id) throws Exception {
		ObjectInputStream ois = null;
		if(id == -1)
			ois = new ObjectInputStream(new FileInputStream(Constants.PRIVATE_KEYS_PATH + File.separator + "mwmr.priv"));
		else
			ois = new ObjectInputStream(new FileInputStream(Constants.PRIVATE_KEYS_PATH + File.separator + "privatekey" + id));
		PrivateKey prk = (PrivateKey) ois.readObject();
		ois.close();
		return prk;
	}

	public static Share[] getKeyShares(SecretKey key, int n, int t) throws InvalidVSSScheme, RuntimeException  {

		byte[] secretkey = key.getEncoded();

		PVSSEngine pvssEngine = PVSSEngine.getInstance(n, t, Constants.KEY_NUM_BITS);

		BigInteger[] secretKeys = pvssEngine.generateSecretKeys();
		BigInteger[] publicKeys = new BigInteger[n];
		for (int i = 0; i < n; i++) {
			publicKeys[i] = pvssEngine.generatePublicKey(secretKeys[i]);
		}
		PublishedShares publishedShares = pvssEngine.generalPublishShares(
				secretkey, publicKeys, 1);//generate shares
		Share[] shares = new Share[n];
		for (int i = 0; i < n; i++) {
			shares[i] = publishedShares.getShare(i, secretKeys[i], pvssEngine.getPublicInfo(), publicKeys);
		}


		return shares;
	}

	public static SecretKey recombineSecretKeyShares(PublicInfo info, Share[] shares, int n) throws ErrorDecryptingException {
		try {
			PVSSEngine engine = PVSSEngine.getInstance(info);
			Share[] orderedShares = new Share[n];
			//share ordering for recombination to process or else it fails
			for (int i = 0; i < shares.length; i++) {
				Share s = shares[i];
				if (s == null) {
					continue;
				}
				orderedShares[s.getIndex()] = s;
			}

			return new SecretKeySpec(engine.generalCombineShares(orderedShares), "AES");
		} catch (InvalidVSSScheme e) {
			e.printStackTrace();
		}
		return null;
	}

	public static PublicInfo getPvssPublicInfo(int n, int t) throws InvalidVSSScheme  {
		return PVSSEngine.getInstance(n, t, Constants.KEY_NUM_BITS).getPublicInfo();
	}




}
