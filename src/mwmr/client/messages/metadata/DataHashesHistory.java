package mwmr.client.messages.metadata;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mwmr.util.Pair;

import java.util.TreeMap;

public class DataHashesHistory implements Externalizable {

	//			<wholeDataHash, <version, blockHash>;
	private Map <String, VersionMetadata> history;

	public DataHashesHistory() {
		history = new TreeMap<String, VersionMetadata>(); 
	}

	public DataHashesHistory(DataHashesHistory old) {
		if(old!=null)
			history = new TreeMap<String, VersionMetadata>(old.history);
		else
			history = new TreeMap<String, VersionMetadata>();
	}

	public String getVersion(String wholeDataHash){
		synchronized (history) {
			VersionMetadata versionMeta = history.get(wholeDataHash);
			if(versionMeta == null)
				return null;

			return versionMeta.getVersion();
		}
	}

	public String getBlockHash(String wholeDataHash, String connectionId){
		synchronized (history) {
			VersionMetadata versionMeta = history.get(wholeDataHash);
			if(versionMeta == null)
				return null;

			return versionMeta.getBlockHash(connectionId);
		}
	}

	public void garbageCollect(int numberOfVersionsToMaintain){
		synchronized (history) {
			if(numberOfVersionsToMaintain >= history.size())
				return;

			Map<String, VersionMetadata> aux = new TreeMap<String, VersionMetadata>();

			for(int i = 0 ; i<numberOfVersionsToMaintain ; i++) {
				int max = -1;
				String maxKey = null;
				for(Entry<String, VersionMetadata> e : history.entrySet()){
					int tempMax = getVersionInteger(e.getValue().getVersion());
					if(tempMax > max){
						max = tempMax;
						maxKey = e.getKey();
					}
					aux.put(maxKey, history.get(maxKey));
					history.remove(maxKey);
				}
			}
			history = aux;
		}
	}

	public void putMetadata( String wholeDataHash, String version, String connectionId, String blockHash){
		VersionMetadata vMeta = history.get(wholeDataHash);
		if(vMeta == null){
			vMeta = new VersionMetadata(version);
			vMeta.putMetadata(connectionId, blockHash);
			history.put(wholeDataHash, vMeta);
		}else{
			vMeta.putMetadata(connectionId, blockHash);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		synchronized (history) {
			int size = in.readInt();
			history = new TreeMap<String, VersionMetadata>();
			for(int i = 0 ; i<size ; i++){
				String key = in.readUTF();
				VersionMetadata vMeta = new VersionMetadata();
				vMeta.readExternal(in);
				history.put(key, vMeta);
			}
		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		synchronized (history) {
			out.writeInt(history.size());

			for(String key : history.keySet()){
				out.writeUTF(key);
				history.get(key).writeExternal(out);
			}

			out.flush();
		}
	}

	private int getVersionInteger(String version){
		return Integer.parseInt(version.split("\\.")[1]);
	}

	private class VersionMetadata implements Externalizable{

		private String version;
		
		//      <connectionId, blockHash>
		private List<Pair<String, String>> hashs;


		public VersionMetadata() {
			hashs = new ArrayList<Pair<String, String>>();
		}

		public VersionMetadata(String version) {
			this();
			this.version = version;
		}

		public String getVersion() {
			return version;
		}

		public String getBlockHash(String connectionId){
			for(Pair<String, String> p : hashs){
				if(p.getKey().equals(connectionId)){
					return p.getValue();
				}
			}
			return null;
		}

		public void putMetadata(String connectionId, String blockHash){
			for(Pair<String, String> p : hashs){
				if(p.getKey().equals(connectionId)){
					p.setValue(blockHash);
					return;
				}
			}
			hashs.add(new Pair<String, String>(connectionId, blockHash));
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
		ClassNotFoundException {
			version = in.readUTF();

			int size = in.readInt();
			for(int i = 0 ; i< size ; i++){
				Pair<String, String> p = new Pair<String, String>();

				p.readExternal(in);
				hashs.add(p);
			}
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeUTF(version);

			out.writeInt(hashs.size());
			for(Pair<String, String> p : hashs)
				p.writeExternal(out);

		}
		
		@Override
		public String toString() {
			String res = version;
			res = res.concat("\n{");
			for(Pair<String, String> p : hashs)
				res = res.concat(p.getKey() + " , " + p.getValue());
			res = res.concat("}");
			return res;
		}

	}

}
