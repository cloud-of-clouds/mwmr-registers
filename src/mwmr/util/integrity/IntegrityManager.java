package mwmr.util.integrity;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;

import mwmr.util.confidenciality.KeyManager;

public class IntegrityManager {

	public static boolean verifySignature(int clientId, byte[] v, byte[] signature) {
		try {
			Signature sig = Signature.getInstance("SHA1withRSA");
			sig.initVerify(KeyManager.loadPublicKey(clientId));
			sig.update(v);
			return sig.verify(signature);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}
	
	public static byte[] getSignature(int clientId, byte[] v) {
		try {
			Signature sig = Signature.getInstance("SHA1withRSA");
			sig.initSign(KeyManager.loadPrivateKey(clientId));
			sig.update(v);
			return sig.sign();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Compute a hash for a given byte array
	 * 
	 * @param o the byte array to be hashed
	 * @return the hash of the byte array
	 */
	public static String getHexHash(byte[] v)  {
			try {
				return getHexString(MessageDigest.getInstance("SHA-1").digest(v));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			return null;
	}
	
	public static String getHexString(byte[] raw) {
		byte[] hex = new byte[2 * raw.length];
		int index = 0;
		for (byte b : raw) {
			int v = b & 0xFF;
			hex[index++] = HEX_CHAR_TABLE[v >>> 4];
			hex[index++] = HEX_CHAR_TABLE[v & 0xF];
		}
		try {
			return new String(hex, "ASCII");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	//base16 char table (aux in getHexString)
		private static final byte[] HEX_CHAR_TABLE = {
			(byte) '0', (byte) '1', (byte) '2', (byte) '3',
			(byte) '4', (byte) '5', (byte) '6', (byte) '7',
			(byte) '8', (byte) '9', (byte) 'a', (byte) 'b',
			(byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
		};
	
}
