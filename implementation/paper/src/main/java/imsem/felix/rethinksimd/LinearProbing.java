package imsem.felix.rethinksimd;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.*;

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

		@Override
		public String toString() {
			String s = "key: " + key + " row: " + payload;
			return s;
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

		public Double [] getKeys(int [] indices, BitSet m) {
			Double [] keys = new Double[indices.length];
			for (int i = 0; i < indices.length; i++) {
				Bucket b = this.get(indices[i]);
				if (m.get(i)) {
					keys[i] = b.key;
				}
			}
			return keys;
		}

		public void put(int [] indices, Double [] keys, BitSet m) {
			for (int i = 0; i < indices.length; i++) {
				if (m.get(i)) {
					System.out.println("h: " + indices[i] + " -> " + keys[i]);
					this.put(indices[i], new Bucket(keys[i], null));
				}
			}
		}

		public void put(int [] indices, Double [] keys, ByteBuffer payloads, BitSet m) {
			int row_byte_size = 387;
			byte [] row = new byte [row_byte_size];
			payloads.position(0);
			int limit = payloads.limit();


			for (int i = 0; i < indices.length; i++) {
				if (m.get(i)) {
					if (keys[i] != null) {

						payloads.position(i * row_byte_size);
						payloads.limit((i+1) * row_byte_size);
						payloads.get(row);

						//this.put(indices[i], new Bucket(keys[i], payloads.slice()));
						this.put(indices[i], new Bucket(keys[i], ByteBuffer.wrap(row)));
					}
				}
			}
			payloads.limit(limit);
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
					if (e.getValue() != null) {
						s += "bucket_ID: " + e.getKey() + " key: " + e.getValue().key + " row: " + Utils.bufferToString(e.getValue().payload, 1);
					} else {
						s += "bucket_ID: " + e.getKey() + "-> null";
					}
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
			return s;
		}
	}

	public static ByteBuffer probeScalar(Double [] sKeys, ByteBuffer sPayloads, HashTable T) {
		int row_byte_size = 387;
		sPayloads.position(0);

		int j = 0;

		Double k;
		byte [] v = new byte[row_byte_size];
		int h;

		ByteBuffer RS_R_payloads = ByteBuffer.allocate(T.size * sKeys.length * row_byte_size);
		ByteBuffer RS_S_payloads = ByteBuffer.allocate(T.size * sKeys.length * row_byte_size);
		DoubleBuffer RS_keys = DoubleBuffer.allocate(T.size * sKeys.length);

		RS_R_payloads.position(0);
		RS_S_payloads.position(0);
		RS_keys.position(0);


		for (int i = 0; i < sKeys.length; i++) { // outer (probing) relation
			k = sKeys[i];
			sPayloads.get(v);
			h = Double.hashCode(k) % T.size;
			while (T.containsKey(h)) { //until empty bucket
				if (k == T.get(h).key) {
					RS_R_payloads.put(T.get(h).payload); //inner payloads
					RS_S_payloads.put(v); // outer payloads
					RS_keys.put(k); // join keys
				}
				h = h + 1; // next bucket
				if (h == T.size) { // reset if last bucket
					h = 0;
				}
			}
		}

		RS_R_payloads.limit(RS_R_payloads.position());
		RS_S_payloads.limit(RS_S_payloads.position());
		RS_keys.limit(RS_keys.position());

		return RS_R_payloads;
	}

	public static int [] hashVector(Double [] k, int hashTableSize) {
		int [] h = new int [k.length];
		for (int i = 0; i < k.length; i++) {
			if (k[i] != null) {
				h[i] = Double.hashCode(k[i]) % hashTableSize;
			}
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
			System.out.println("m: i: " + i + " -> " + "a: " + a[i] + " b: " + b[i]);
			m.set(i, (b[i].equals(a[i])));
			System.out.println("m: i: " + i + " -> " + (b[i].equals(a[i])) + "a: " + a[i] + " b: " + b[i]);
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

	public static ByteBuffer probeVector(int W, Double [] sKeys, ByteBuffer sPayloads, HashTable T) {
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

		ByteBuffer RS_R_payloads = ByteBuffer.allocate(T.size * sKeys.length * row_byte_size);
		ByteBuffer RS_S_payloads = ByteBuffer.allocate(T.size * sKeys.length * row_byte_size);
		DoubleBuffer RS_keys = DoubleBuffer.allocate(T.size * sKeys.length);

		RS_R_payloads.position(0);
		RS_S_payloads.position(0);
		RS_keys.position(0);

		BitSet m = new BitSet(W);
		m.set(0, m.length()); //boolean vector register

		while (i + W <= sKeys.length) { // W : # of vector lanes
			k = FundamentalOperations.selectiveLoad(Double.class, k, sKeys, i, m);
			v = FundamentalOperations.selectiveLoad(v, sPayloads,i, m);

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

		RS_R_payloads.limit(RS_R_payloads.position());
		RS_S_payloads.limit(RS_S_payloads.position());
		RS_keys.limit(RS_keys.position());

		return RS_R_payloads;
	}

	public static HashTable buildScalar(Double [] rKeys, ByteBuffer rPayloads, int hashTableSize) {
		HashTable T = new HashTable(hashTableSize);
		return buildScalar(rKeys, rPayloads, T);
	}

	public static HashTable buildScalar(Double [] rKeys, ByteBuffer rPayloads, HashTable T ) {
		double k;
		int h;
		rPayloads.position(0);

		try {
			Utils.printBuffer(rPayloads, rKeys.length);
		} catch (Exception e) {
			e.printStackTrace();
		}

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
			T.put(h, new Bucket(k, ByteBuffer.wrap(row.clone()))); // set empty bucket
		}

		return  T;
	}

	public static HashTable buildVector(int W, Double [] rKeys, ByteBuffer rPayloads, int hashTableSize) {
		int row_byte_size = 387;
		rPayloads.position(0);

		System.out.println("Table: " + Arrays.toString(rPayloads.array()));

		Double [] l = new Double[W];//any vector with unique values per lane
		for (int t = 0; t < W; t++) {
			l[t] = (double) t;
		}
		Double [] l_back = new Double[W];

		int [] o = new int[W];
		for (int t = 0; t < W; t++) {
			o[t] = 0;
		}

		BitSet m = new BitSet(W);
		m.set(0, W); //boolean vector register

		int i = 0;

		ByteBuffer v = ByteBuffer.allocate(row_byte_size * W);

		Double [] k = new Double[W];

		int [] h;

		Double [] kT = new Double[W];

		HashTable T = new HashTable(hashTableSize);

		while (i + W <= rKeys.length) { // W : # of vector lanes
			System.out.println("m: " + Utils.BitSetToString(m, W));

			k = FundamentalOperations.selectiveLoad(Double.class, k, rKeys, i, m);
			System.out.println("k: " + Arrays.toString(k));
			v.position(0);
			rPayloads.position(0);

			System.out.println("i: " + i);

			v = FundamentalOperations.selectiveLoad(v, rPayloads, i, m);

			System.out.println("i: " + i);
			try {
				System.out.println("v: " + Utils.bufferToString(v, W));
			} catch (Exception e) {
				e.printStackTrace();
			}

			i = i + m.cardinality();

			h = hashVector(k, T.size); //multiplicative hashing
			h = add(h,o);

			kT = T.getKeys(h); //gather buckets

			System.out.println("kT: " + Arrays.toString(kT));

			m = isEmpty(kT); // find empty buckets

			System.out.println("m: " + Utils.BitSetToString(m, W));

			//find detect conflicts -> find keys with the same hash value within the vector
			T.put(h, l, m);
			l_back = T.getKeys(h, m);

			System.out.println("l_back: " + Arrays.toString(l_back));
			System.out.println("l: " + Arrays.toString(l));

			System.out.println("m: " + Utils.BitSetToString(m, W));

			m.and(compare(l_back, l));

			System.out.println("com: " + Utils.BitSetToString(compare(l_back, l), W));

			System.out.println("m: " + Utils.BitSetToString(m, W));

			T.put(h, k, v, m); //scatter to buckets

			System.out.println("HashTable: " + T);

			o = incrementOrResetOffsets(o, m); //increment or reset offsets
		}
		int rest = rKeys.length - i + 1;
		Double [] keys = new Double[rest];
		rPayloads.position((i - 1) * row_byte_size);


		for (int t = 0; t < rest; t++) {
			keys[t] = rKeys[i + t - 1];
		}

		System.out.println("keys: " + Arrays.toString(keys));

		System.out.println("rows: " + rPayloads.slice().remaining());

		T = buildScalar(keys, rPayloads.slice(), T);
		return T;
	}
}
