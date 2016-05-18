package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.*;

import java.io.*;
import java.nio.ByteBuffer;
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

	public static Row[] loadCSVResource(String filename, String delim) throws IOException {
		return loadCSV(Utils.class.getClassLoader().getResource(filename).getPath(), delim);
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

	public static byte[] serialize(Object obj) throws IOException {
		try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
			try(ObjectOutputStream o = new ObjectOutputStream(b)){
				o.writeObject(obj);
				o.flush();
			}
			return b.toByteArray();
		}
	}

	public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
			try(ObjectInputStream o = new ObjectInputStream(b)){
				return o.readObject();
			}
		}
	}

	public static ByteBuffer toByte(Row[] table) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(100000);
		for (int i = 0; i < table.length; i++) {
			byte [] b = serialize(table[i]);
			buffer.put(b);
		}
		return buffer;
	}
	
	public static void printBuffer(ByteBuffer b, int len) throws IOException, ClassNotFoundException {
		int row_byte_size = 387;
		byte [] row = new byte[row_byte_size];
		int pos = b.position();
		b.position(0);
		for (int i = 0; i < len; i++) {
			try {
				b.get(row);
				Row r = (Row) deserialize(row);
				System.out.println(r);
			}catch (Exception e) {
				
			}
		}
		b.position(pos);
	}

	public static Row[] toRows(ByteBuffer b, int len) {
		Row [] rows = new Row [len];
		int row_byte_size = 387;
		byte [] row = new byte[row_byte_size];
		int pos = b.position();
		b.position(0);
		for (int i = 0; i < len; i++) {
			try {
				b.get(row);
				rows[i] = (Row) deserialize(row);
			}catch (Exception e) {

			}
		}
		b.position(pos);
		
		return rows;
	}
}
