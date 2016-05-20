package imsem.felix.rethinksimd;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.ArrayList;
import java.util.BitSet;

/**
 * Created by felix on 16.05.16.
 */
public class FundamentalOperations {
	
	public static <T> T[] selectiveStore(Class<T> type, T[] vector, BitSet mask) {
		ArrayList<T> memory = new ArrayList<T>();
		
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				memory.add(vector[i]);
			}
		}
		return memory.toArray((T[]) Array.newInstance(type, memory.size()));
	}

	public static DoubleBuffer selectiveStore(Double[] vector, BitSet mask, DoubleBuffer memory) {
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				memory.put(vector[i]);
			}
		}
		return memory;
	}

	public static ByteBuffer selectiveStore(ByteBuffer vector, BitSet mask, ByteBuffer memory) {
		int row_byte_size = 387;
		byte [] row = new byte[row_byte_size];
		
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				vector.position(0);
				vector.get(row, row_byte_size * i, row_byte_size);
				memory.put(row);
			}
		}
		return memory;
	}
	
	

	public static int[] selectiveStore(int[] vector, BitSet mask) {
		int [] memory = new int [mask.cardinality()];
		
		int j = 0;
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				memory[j] = vector[i];
				j++;
			}
		}
		return memory;
	}

	public static <T> T[] selectiveLoad(Class<T> type, T[] vector, T[] memory, int memory_id, BitSet mask) {
		
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				vector[i] = memory[memory_id];
				memory_id++;
			}
		}
		return vector;
	}

	public static ByteBuffer selectiveLoad(ByteBuffer vector, ByteBuffer memory, BitSet mask) {
		int row_byte_size = 387;
		byte [] row = new byte[row_byte_size];
		vector.position(0);
		
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				memory.get(row);
				vector.put(row);
			}
		}
		return vector;
	}

	public static <T> T[] gather(Class<T> type, int[] indexVector, T[] memory) {
		T [] valueVector = (T[]) Array.newInstance(type, indexVector.length);

		for (int i = 0; i < indexVector.length; i++) {
			valueVector[i] = memory[indexVector[i]];
		}
		return valueVector;
	}
	
	public static <T> T[] scatter(Class<T> type, T[] valueVector, int[] indexVector, T[] memory) {
		for (int i = 0; i < indexVector.length; i++) {
			memory[indexVector[i]] = valueVector[i];
		}
		return memory;
	}
}
