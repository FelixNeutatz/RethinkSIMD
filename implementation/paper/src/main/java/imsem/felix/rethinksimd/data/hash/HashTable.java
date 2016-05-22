package imsem.felix.rethinksimd.data.hash;

import imsem.felix.rethinksimd.util.Utils;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by felix on 22.05.16.
 */
public class HashTable extends Hashtable<Integer, Bucket> {
	public int size;

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
					this.put(indices[i], new Bucket(keys[i], ByteBuffer.wrap(row.clone())));
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
