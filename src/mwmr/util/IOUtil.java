package mwmr.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class IOUtil {

	public static void readFromOIS(ObjectInput ois , byte[] toFill) throws IOException{
		for(int i = 0 ; i< toFill.length ; i++)
			toFill[i] = ois.readByte();
	}

	public static void writeToOOS(ObjectOutput out , byte[] towrite) throws IOException{
		out.write(towrite);
		out.flush();
	}

	public static void closeStream(Closeable stream){
		try {
			stream.close();
		} catch (IOException e) {
			//ignore close execption.
		}
	}


}
