package imsem.felix.rethinksimd;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by felix on 17.05.16.
 */
public class LinearProbing {
	
	public static class Bucket implements Serializable{
		double key;
		ByteBuffer payload;
		
		public Bucket (double key, ByteBuffer payload) {
			this.key = key;
			this.payload = payload;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof Bucket))
				return false;
			if (obj == this)
				return true;

			Bucket b = (Bucket) obj;
			
			return (this.key == b.key && this.payload.equals(b.payload));
		}
	}
	
	public static class HashTable extends Hashtable<Integer, Bucket>{
		int size;
		
		public HashTable(int size) {
			super();
			this.size = size;
		}

		@Override
		public String toString() {
			String s = "";
			for (Map.Entry<Integer,Bucket> e: this.entrySet()) {
				try {
					s += "bucket_ID: " + e.getKey() + " key: " + e.getValue().key + " row: " + Utils.bufferToString(e.getValue().payload, 1);
				} catch (Exception e1) {
					e1.printStackTrace();
				} 
			}
			return s;
		}
	}

	public static void probeScalar(double [] sKeys, ByteBuffer sPayloads, HashTable T) {
		int row_byte_size = 387;
		sPayloads.position(0);
		
		int j = 0;
		
		double k;
		byte [] v = new byte[row_byte_size];
		int h;

		ByteBuffer RS_R_payloads = ByteBuffer.allocate(sKeys.length);
		ByteBuffer RS_S_payloads = ByteBuffer.allocate(sKeys.length);
		DoubleBuffer RS_keys = DoubleBuffer.allocate(sKeys.length);

		for (int i = 0; i < sKeys.length; i++) { // outer (probing) relation
			k = sKeys[i];
			sPayloads.get(v);
			h = Double.hashCode(k) % T.size;
			while (T.containsKey(h)) { //until empty bucket
				if (k == T.get(h).key) {
					RS_R_payloads.put(T.get(h).payload.array()); //inner payloads
					RS_S_payloads.put(v); // outer payloads
					RS_keys.put(k); // join keys
				}
				h = h + 1; // next bucket
				if (h == T.size) { // reset if last bucket
					h = 0; 
				}
			}
		}
	}
	
	public static HashTable buildScalar(double [] rKeys, ByteBuffer rPayloads, int hashTableSize) {
		double k;
		int h;
		rPayloads.position(0);

		HashTable T = new HashTable(hashTableSize);

		int row_byte_size = 387;
		byte [] row = new byte[row_byte_size];
		
		for (int i = 0; i < rKeys.length; i++) {
			k = rKeys[i];
			h = Double.hashCode(k) % T.size;
			while (T.containsKey(h)) { //until empty bucket
				h = h + 1;
				if (h == T.size) { // reset if last bucket
					h = 0;
				}
			}
			rPayloads.get(row);
			T.put(h, new Bucket(k, ByteBuffer.allocate(row_byte_size).put(row))); // set empty bucket
		}
		
		return  T;
	}
}
