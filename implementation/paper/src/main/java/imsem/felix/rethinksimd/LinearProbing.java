package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.hash.Bucket;
import imsem.felix.rethinksimd.data.hash.HashTable;
import imsem.felix.rethinksimd.util.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.*;

import static imsem.felix.rethinksimd.util.HashUtils.*;
/**
 * Created by felix on 17.05.16.
 */
public class LinearProbing {

	public static ByteBuffer probeScalar(Double [] sKeys, ByteBuffer sPayloads, HashTable T) {
		int row_byte_size = 387;
		sPayloads.position(0);

		int j = 0;

		Double k;
		byte [] v = new byte[row_byte_size];
		int h;

		ByteBuffer RS_R_payloads = ByteBuffer.allocate(sKeys.length * row_byte_size);
		ByteBuffer RS_S_payloads = ByteBuffer.allocate(sKeys.length * row_byte_size);
		DoubleBuffer RS_keys = DoubleBuffer.allocate(sKeys.length);

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

	public static ByteBuffer probeVector(int W, Double [] sKeys, ByteBuffer sPayloads, HashTable T) throws IOException, ClassNotFoundException {
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

		RS_R_payloads.position(0);
		RS_S_payloads.position(0);
		RS_keys.position(0);

		BitSet m = new BitSet(W);
		m.set(0, W); //boolean vector register

		while (i + W <= sKeys.length) { // W : # of vector lanes
			k = FundamentalOperations.selectiveLoad(Double.class, k, sKeys, i, m);

			System.out.println("k: " + Arrays.toString(k));
			
			v = FundamentalOperations.selectiveLoad(v, sPayloads,i, m);

			i = i + m.cardinality();

			h = hashVector(k, T.size);
			h = add(h,o);

			kT = T.getKeys(h); //gather buckets

			System.out.println("kT: " + Arrays.toString(kT));
			
			vT = T.getPayloads(h);

			m = compare(kT, k);

			RS_keys = FundamentalOperations.selectiveStore(k, m, RS_keys); //selectively store matching tuples
			RS_S_payloads = FundamentalOperations.selectiveStore(v, m, RS_S_payloads);
			RS_R_payloads = FundamentalOperations.selectiveStore(vT, m, RS_R_payloads);

			System.out.println("RS_R_payloads: " + Utils.bufferToString(RS_R_payloads, RS_R_payloads.position() / row_byte_size));

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
		int rest = rKeys.length - i;
		Double [] keys = new Double[rest];
		rPayloads.position((i) * row_byte_size);


		for (int t = 0; t < rest; t++) {
			keys[t] = rKeys[i + t];
		}

		System.out.println("keys: " + Arrays.toString(keys));

		System.out.println("rows: " + rPayloads.slice().remaining());

		T = buildScalar(keys, rPayloads.slice(), T);
		return T;
	}
}
