package imsem.felix.rethinksimd.data;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by felix on 17.05.16.
 */
public class Value <T> implements Serializable {
	public T value;
	
	public Value(T value) {
		this.value = value;
	}
	
	public T get() {
		return value;
	}
	public void set (T value) {
		this.value = value;
	}

	@Override
	public String toString() {
		String s = "" + value;
		return s;
	}
	
	@Override
	public boolean equals (Object obj){
		if (obj == this)
			return true;

		Value<T> s = (Value<T>) obj;

		return (s.get().equals(value));
	}
}
