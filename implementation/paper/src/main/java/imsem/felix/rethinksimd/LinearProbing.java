package imsem.felix.rethinksimd;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.BitSet;
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
		
		public Double [] getKeys(int [] indices) {
			Double [] keys = new Double[indices.length];
			for (int i = 0; i < indices.length; i++) {
				Bucket b = this.get(indices[i]);
				if (b != null) {
					keys[i] = b.key;
				} else {
					keys[i] = null;
				}
			}
			return keys;
		}

		public void put(int [] indices, Double [] keys, ByteBuffer payloads, BitSet m) {
			int row_byte_size = 387;
			byte [] row = new byte [387];
			ByteBuffer rBuffer = ByteBuffer.allocate(row_byte_size);
			
			
			for (int i = 0; i < indices.length; i++) {
				
				if (m.get(i)) {
					payloads.position(0);
					payloads.get(row, i * row_byte_size, row_byte_size);
					rBuffer.put(row);
					rBuffer.position(0);
					
					this.put(indices[i], new Bucket(keys[i], rBuffer));
				}
			}
		}

		
		public ByteBuffer getPayloads(int [] indices) {
			int row_byte_size = 387;
			byte [] row = new byte [387];
				
			ByteBuffer payloads = ByteBuffer.allocate(indices.length * row_byte_size);
			payloads.position(0);
			for (int i = 0; i < indices.length; i++) {
				Bucket b = this.get(indices[i]);
				if (b != null) {
					payloads.put(b.payload);
				} else {
					payloads.put(row); //insert empty row
				}
			}
			return payloads;
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
	
	public static int [] hashVector(Double [] k, int hashTableSize) {
		int [] h = new int [k.length];
		for (int i = 0; i < k.length; i++) {
			h[i] = Double.hashCode(k[i]) % hashTableSize;
		}
		return h;
	}
	
	public static int [] add (int [] a, int [] b) {
		int [] sum = new int[a.length];
		for (int i = 0; i < a.length; i++) {
			sum[i] = a[i] + b[i];
		}
		return sum;
	}
	
	public static BitSet compare(Double [] a, Double [] b) {
		BitSet m = new BitSet(a.length);
		for (int i = 0; i < a.length; i++) {
			m.set(i, (a[i] == b[i]));
		}
		return m;
	}

	public static BitSet isEmpty(Double [] a) {
		BitSet m = new BitSet(a.length);
		for (int i = 0; i < a.length; i++) {
			m.set(i, (a[i] == null));
		}
		return m;
	}

	public static int [] incrementOrResetOffsets(int [] o, BitSet m) {
		for (int i = 0; i < o.length; i++) {
			if (m.get(i)) {
				o[i] = 0;		 //reset offset
			} else {
				o[i] = o[i] + 1; //increment offset
			}
		}
		return o;
	}

	public static void probeVector(int W, Double [] sKeys, ByteBuffer sPayloads, HashTable T) {
		int row_byte_size = 387;
		sPayloads.position(0);

		int i = 0;
		int [] o = new int[W];
		for (int t = 0; t < W; t++) {
			o[t] = 0;
		}

		Double [] k = new Double[W];
		ByteBuffer v = ByteBuffer.allocate(row_byte_size * W);
		v.position(0);
		int [] h;
		
		Double [] kT = new Double[W];
		ByteBuffer vT = ByteBuffer.allocate(W * row_byte_size);

		ByteBuffer RS_R_payloads = ByteBuffer.allocate(sKeys.length * row_byte_size);
		ByteBuffer RS_S_payloads = ByteBuffer.allocate(sKeys.length * row_byte_size);
		DoubleBuffer RS_keys = DoubleBuffer.allocate(sKeys.length);

		BitSet m = new BitSet(W);
		m.set(0, m.length()); //boolean vector register

		while (i + W <= sKeys.length) { // W : # of vector lanes
			k = FundamentalOperations.selectiveLoad(Double.class, k, sKeys, i, m);
			v = FundamentalOperations.selectiveLoad(sPayloads, v, m);

			i = i + m.cardinality();
			
			h = hashVector(k, T.size);
			h = add(h,o);
			
			kT = T.getKeys(h); //gather buckets
			vT = T.getPayloads(h);
			
			m = compare(kT, k);

			RS_keys = FundamentalOperations.selectiveStore(k, m, RS_keys); //selectively store matching tuples
			RS_S_payloads = FundamentalOperations.selectiveStore(v, m, RS_S_payloads);
			RS_R_payloads = FundamentalOperations.selectiveStore(vT, m, RS_R_payloads);

			m = isEmpty(kT); // discard finished tuples
			
			o = incrementOrResetOffsets(o, m); //increment or reset offsets
		}
	}
	
	public static HashTable buildScalar(Double [] rKeys, ByteBuffer rPayloads, int hashTableSize) {
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

	public static HashTable buildVector(int W, Double [] rKeys, ByteBuffer rPayloads, int hashTableSize) {
		int row_byte_size = 387;
		rPayloads.position(0);
		
		int [] l = new int[W];//any vector with unique values per lane
		for (int t = 0; t < W; t++) {
			l[t] = t;
		}

		int [] o = new int[W];
		for (int t = 0; t < W; t++) {
			o[t] = 0;
		}

		BitSet m = new BitSet(W);
		m.set(0, m.length()); //boolean vector register
		
		int i = 0;

		ByteBuffer v = ByteBuffer.allocate(row_byte_size * W);

		Double [] k = new Double[W];

		int [] h;

		Double [] kT = new Double[W];
		
		HashTable T = new HashTable(hashTableSize);

		while (i + W <= rKeys.length) { // W : # of vector lanes
			k = FundamentalOperations.selectiveLoad(Double.class, k, rKeys, i, m);
			v = FundamentalOperations.selectiveLoad(rPayloads, v, m);

			i = i + m.cardinality();

			h = hashVector(k, T.size); //multiplicative hashing
			h = add(h,o);

			kT = T.getKeys(h); //gather buckets

			m = isEmpty(kT); // find empty buckets
			
			//TODO: some stuff I don't understand
			
			T.put(h, k, v, m); //scatter to buckets

			o = incrementOrResetOffsets(o, m); //increment or reset offsets
		}
		return T;
	}
}
