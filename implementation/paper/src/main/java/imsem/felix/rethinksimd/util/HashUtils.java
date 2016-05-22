package imsem.felix.rethinksimd.util;

import java.util.BitSet;

/**
 * Created by felix on 22.05.16.
 */
public class HashUtils {
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
}
