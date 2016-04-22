package mwmr.util.confidenciality;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author miguel
 */
public class KeyGenerator {

	public static void main(String args[]) {
		try {
			String filename = "mwmr";
			//int keySize = Integer.parseInt(args[1]);
			int keySize = 512;
			String keyPath= "config/keys/";
			KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
			kpg.initialize(keySize);
			KeyPair kp = kpg.genKeyPair();
			Key publicKey = kp.getPublic();
			Key privateKey = kp.getPrivate();
			saveToFile(keyPath+filename+".priv", (PrivateKey) privateKey);
			saveToFile(keyPath+filename+".pub", (PublicKey) publicKey);
//			System.out.println("Private and public key generated size="+keySize);
//			KeyFactory fact = KeyFactory.getInstance("RSA");
//			RSAPublicKeySpec pub = fact.getKeySpec(publicKey, RSAPublicKeySpec.class);
//			RSAPrivateKeySpec priv = fact.getKeySpec(privateKey, RSAPrivateKeySpec.class);
//			saveToFile(keyPath+filename+".pub", pub.getModulus(), pub.getPublicExponent());
//			System.out.println(keyPath+filename+".pub saved to file");
//			saveToFile(keyPath+filename+".priv", priv.getModulus(), priv.getPrivateExponent());
//			System.out.println(keyPath+filename+".priv saved to file");
		} catch (Exception ex) {
			Logger.getLogger(KeyGenerator.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	public static void saveToFile(String fileName, Key key) throws IOException {
		ObjectOutputStream oout = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
		oout.writeObject(key);
		oout.close();

	}

}