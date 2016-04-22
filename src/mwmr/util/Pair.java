package mwmr.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public class Pair<K extends Serializable,V extends Serializable> implements Externalizable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6359507770831785117L;
	private K key;
	private V value;

	public Pair() {}
	
	public Pair(K k, V v) {
		this.key = k;
		this.value = v;
	}

	public V getValue(){
		return value;
	}

	public K getKey(){
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public void setValue(V value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[key=" + key + ", value=" + value + "]";
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readExternal(ObjectInput in) throws IOException,
	ClassNotFoundException {
		if(in.readInt()>=0){
			key = (K) in.readObject();
		}else{
			key = null;
		}

		if(in.readInt()>=0){
			value = (V) in.readObject();
		}else{
			value = null;
		}

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if(key == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			out.writeObject(key);
		}
		if(value == null){
			out.writeInt(-1);
		}else{
			out.writeInt(0);
			out.writeObject(value);
		}
		out.flush();
	}




}
