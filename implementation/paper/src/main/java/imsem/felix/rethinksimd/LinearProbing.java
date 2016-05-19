package imsem.felix.rethinksimd;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Hashtable;

/**
 * Created by felix on 17.05.16.
 */
public class LinearProbing {
	
	public class Bucket implements Serializable{
		int key;
		ByteBuffer payload;
		
		public Bucket (int key, ByteBuffer payload) {
			this.key = key;
			this.payload = payload;
		}
	}

	public static void probeScalar(int [] sKeys, ByteBuffer sPayloads) {
		int row_byte_size = 387;
		sPayloads.position(0);
		
		int hashTableSize = 5;
		
		int j = 0;
		
		int k;
		byte [] v = new byte[row_byte_size];
		int h;

		ByteBuffer RS_R_payloads = ByteBuffer.allocate(sKeys.length);
		ByteBuffer RS_S_payloads = ByteBuffer.allocate(sKeys.length);
		IntBuffer RS_keys = IntBuffer.allocate(sKeys.length);

		Hashtable<Integer, Bucket> T = new Hashtable<Integer, Bucket>();
		
		for (int i = 0; i < sKeys.length; i++) { // outer (probing) relation
			k = sKeys[i];
			sPayloads.get(v);
			h = Integer.hashCode(k) % hashTableSize;//
			while (T.containsKey(h)) { //until empty bucket
				if (k == T.get(h).key) {
					RS_R_payloads.put(T.get(h).payload.array()); //inner payloads
					RS_S_payloads.put(v); // outer payloads
					RS_keys.put(k); // join keys
				}
				h = h + 1; // next bucket
				if (h == hashTableSize) { // reset if last bucket
					h = 0; 
				}
			}
		}
	}
}
