package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

/**
 * Created by felix on 16.05.16.
 */
public class Utils {
	public static boolean [] toBin (int [] array) {
		boolean [] bArray = new boolean[array.length];
		for (int i = 0; i < array.length; i++) {
			bArray[i] = (array[i] == 1);
		}
		return bArray;
	}

	public static BitSet toBitSet (int [] array) {
		BitSet bitset = new BitSet(array.length);
		for (int i = 0; i < array.length; i++) {
			bitset.set (i, array[i] == 1);
		}
		return bitset;
	}
	
	public static Row[] loadCSV(String filename, String delim) throws IOException {
		ArrayList<Row> table = new ArrayList<Row>();
		BufferedReader br = new BufferedReader(new FileReader(filename));
		try {
			String line = br.readLine();

			String [] values;
			ArrayList<Value> row;
			while (line != null) {
				values = line.split(delim);
				row = new ArrayList<Value>();
				for (int i = 0; i < values.length; i++) {
					//TODO: This is a hack, use apache function to check whether its numeric
					try { 
						row.add(new DoubleValue(Double.parseDouble(values[i])));
					}catch (Exception e) {
						row.add(new StringValue(values[i]));
					}
				}
				table.add(new Row(row));
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		
		return table.toArray(new Row[table.size()]);
	}
	
	public static void printTable(Row [] table) {
		for (int i = 0; i < table.length; i++) {
			System.out.println(table[i]);
		}
	}
	
	public static double [] [] generateKeysIn (Row [] table, int [] keyIndices) {
		double [] [] keysIn = new double [table.length] [keyIndices.length];
		
		for (int r = 0; r < table.length; r++) {
			for (int k = 0; k < keyIndices.length; k++) {
				keysIn [r][k] = ((DoubleValue)table[r].get(keyIndices[k])).get();
			}
		}
		return keysIn;
	}

	public static int bitPopulationCount(boolean [] m) {
		int count = 0;
		for (int i = 0; i < m.length; i++) {
			if (m[i]) {
				count++;
			}
		}
		return count;
	}
}
