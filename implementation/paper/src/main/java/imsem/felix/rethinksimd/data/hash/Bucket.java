package imsem.felix.rethinksimd.data.hash;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by felix on 22.05.16.
 */
public class Bucket implements Serializable {
	public double key;
	public ByteBuffer payload;

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
