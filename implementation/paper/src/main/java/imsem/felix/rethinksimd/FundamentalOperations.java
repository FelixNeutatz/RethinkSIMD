package imsem.felix.rethinksimd;

import java.lang.reflect.Array;
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

	public static <T> T[] selectiveLoad(Class<T> type, T[] vector, T[] memory, BitSet mask) {
		T [] vectorNew = (T[]) Array.newInstance(type, vector.length);
		int memory_id = 0;
		
		for (int i = 0; i < mask.length(); i++) {
			if (mask.get(i)) {
				vectorNew[i] = memory[memory_id];
				memory_id++;
			} else {
				vectorNew[i] = vector[i];
			}
		}
		return vectorNew;
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
