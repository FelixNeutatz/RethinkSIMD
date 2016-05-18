package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;

/**
 * Created by felix on 17.05.16.
 */
public class SelectionScan {
	
	/*
		Check range conditions for all keys for one row
		equivalent for: if (k ≥ k lower ) && (k ≤ k upper )
	 */
	private static boolean checkConditionForAllKeys(double [] k, double [] kLower, double [] kUpper) {
		for (int c = 0; c < k.length; c++) {
			if ( (k[c] < kLower[c]) || (k[c] > kUpper[c]) ) {
				return false;
			}
		}
		return true;
	}

	public static Row[] selectionScanBranching(double [][] tKeysIn, double [] kLower, double [] kUpper, Row [] tPayloadsIn) {
		int j = 0; //output index

		HashMap<Integer,Row> tPayloadsOut = new HashMap<Integer,Row>();
		HashMap<Integer,Double> tKeysOut = new HashMap<Integer,Double>();
		
		for (int i = 0; i < tKeysIn.length; i++) {
			double [] k = tKeysIn[i];							//access key columns
			if (checkConditionForAllKeys(k, kLower, kUpper)){   //short circuit and
				tPayloadsOut.put(j, tPayloadsIn[i]); 			//copy all columns
				//tKeysOut.put(j, k);
				j = j + 1;
			}
		}
		return tPayloadsOut.values().toArray(new Row [tPayloadsOut.size()]);
	}

	public static Row[] selectionScanBranchless(double [][] tKeysIn, double [] kLower, double [] kUpper, Row [] tPayloadsIn) {
		int j = 0; //output index

		HashMap<Integer,Row> tPayloadsOut = new HashMap<Integer,Row>();
		HashMap<Integer,Double> tKeysOut = new HashMap<Integer,Double>();

		int m;
		for (int i = 0; i < tKeysIn.length; i++) {
			double [] k = tKeysIn[i];								//access key columns
			tPayloadsOut.put(j, tPayloadsIn[i]); 					//copy all columns
			//tKeysOut.put(j, k);
			m = checkConditionForAllKeys(k, kLower, kUpper)? 1 : 0;
			j = j + m;												//if-then-else expressions use conditional ...
		}															//... flags to update the index without branching
		tPayloadsOut.remove(j);
		return tPayloadsOut.values().toArray(new Row [tPayloadsOut.size()]);
	}
	
	private static double [][] loadVectorsOfKeyColumns(int W, int currentIndex, double [][] tKeysIn) {
		double [][] k = new double [W] [tKeysIn[0].length];
		for (int i = 0; i < W; i++) {
			for (int c = 0; c < tKeysIn[0].length; c++) {
				k[i][c] = tKeysIn[currentIndex + i][c];
			}
		}
		return k;
	}


	/*
		Check range conditions for all keys for a vector of rows
		equivalent for: if (k ≥ k lower ) && (k ≤ k upper )
	 */
	private static BitSet checkConditionForAllKeys(double [][] k, double [] kLower, double [] kUpper) {
		BitSet m = new BitSet(k.length);
		
		for (int w = 0; w < k.length; w++) {
			m.set(w); //true
			for (int c = 0; c < kLower.length; c++) {
				if ((k[w][c] < kLower[c]) || (k[w][c] > kUpper[c])) {
					m.flip(w);
					break;
				}
			}
		}
		return m;
	}
	
	private static <T> HashMap<Integer,T> put (int [] indices, T [] from, HashMap<Integer,T> to) {
		return put (indices, from, to, indices.length);
	}

	private static <T> HashMap<Integer,T> put (int [] indices, T [] from, HashMap<Integer,T> to, int len) {
		for (int i = 0; i < len; i++) {
			to.put(indices[i], from[indices[i]]);
		}
		return to;
	}

	private static ByteBuffer put (int currentIndex, int [] indices, ByteBuffer from, ByteBuffer to) {
		return put (currentIndex, indices, from, to, indices.length);
	}

	private static ByteBuffer put (int currentIndex, int [] indices, ByteBuffer from, ByteBuffer to, int len) {
		int row_byte_size = 387;
		byte [] row = new byte[row_byte_size]; //TODO: make it dynamically
		for (int i = 0; i < len; i++) {
			from.position((currentIndex + indices[i]) * row_byte_size);
			from.get(row);
			to.put(row.clone());
		}
		return to;
	}
	
	public static ByteBuffer selectionScanVector(int bufferSize, int W, double [][] tKeysIn, double [] kLower, double [] kUpper, ByteBuffer tPayloadsIn) {
		IntBuffer B = IntBuffer.allocate(bufferSize);
		int [] p = new int [W];
		ByteBuffer tPayloadsOut = ByteBuffer.allocate(tPayloadsIn.limit());
		tPayloadsIn.position(0);
		
		int i, j, l; //input, output, and buffer indexes
		i = j = l = 0;
		
		int [] r = new int [W]; //input indexes in vector
		for (int t = 0; t < W; t++) { // of vector lanes
			r[t] = t;
		}

		B.position(0);
		for (i = 0; i < tKeysIn.length; i += W) {
			double [][] k = loadVectorsOfKeyColumns(W, i, tKeysIn); //load vectors of key columns
			BitSet m = checkConditionForAllKeys(k, kLower, kUpper); //predicates to mask

			if (m.cardinality() > 0) { //if (m = false) //optional branch
				B.put(FundamentalOperations.selectiveStore(r, m)); //selectively store indexes
				
				l = l + m.cardinality(); //update buffer index
				
				if (l > bufferSize - W) { //flush buffer
					B.position(0);
					for (int b = 0; b < bufferSize - W; b += W) {
						B.get(p); //load input indexes
						tPayloadsOut = put(j, p, tPayloadsIn, tPayloadsOut); //... streaming stores
						j = j + W;
					}
					int p_size = l - B.position();
					B.get(p, 0, p_size); //move overflow ...
					
					B.position(0);
					B.put(p, 0, p_size); //... indexes to start
					l = bufferSize - W; // update buffer index
				}
			}
		} 
		
		// flush last items after the loop
		int p_size = B.position();
		tPayloadsOut = put(j, p, tPayloadsIn, tPayloadsOut, p_size); //... streaming stores
		
		//return tPayloadsOut.values().toArray(new Row [tPayloadsOut.size()]);
		return  tPayloadsOut;
	}
}
