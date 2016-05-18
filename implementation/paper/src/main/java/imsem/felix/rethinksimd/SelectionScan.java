package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.*;

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
			for (int c = 0; c < k[0].length; c++) {
				if ((k[w][c] < kLower[c]) || (k[w][c] > kUpper[c])) {
					m.flip(w); //false
					break;
				}
			}
		}
		return m;
	}
	
	private static <T> HashMap<Integer,T> put (int [] indices, T [] from, HashMap<Integer,T> to) {
		for (int i = 0; i < indices.length; i++) {
			to.put(indices[i], from[indices[i]]);
		}
		return to;
	}
	
	public static Row[] selectionScanVector(int W, double [][] tKeysIn, double [] kLower, double [] kUpper, Row [] tPayloadsIn) {
		IntBuffer B = IntBuffer.allocate(W);
		int [] p = new int [W];
		HashMap<Integer,Row> tPayloadsOut = new HashMap<Integer,Row>();
		
		int i, j, l; //input, output, and buffer indexes
		i = j = l = 0;
		
		int [] r = new int [W]; //input indexes in vector
		int ri = 0;
		
		for (int t = 0; t < W; t++) { // of vector lanes
			r[t] = t;
		}
		IntBuffer rBuffer = IntBuffer.allocate(W);
		rBuffer.put(r);

		for (i = 0; i < tKeysIn.length; i += W) {
			double [][] k = loadVectorsOfKeyColumns(W, i, tKeysIn); //load vectors of key columns
			BitSet m = checkConditionForAllKeys(k, kLower, kUpper); //predicates to mask

			if (m.cardinality() > 0) { //if (m = false) //optional branch
				System.out.println(m.cardinality());
				B.put(FundamentalOperations.selectiveStore(r, m), l, m.cardinality()); //selectively store indexes
				//System.out.println(Arrays.toString(B.array()));
				l = l + m.cardinality(); //update buffer index
				
				System.out.println("l: " + l);
				System.out.println("B limit: " + B.limit());
				if (l > B.limit() - W) { //flush buffer
					for (int b = 0; b <= B.limit() - W; b += W) {
						System.out.println("hallo2");
						//System.out.println(Arrays.toString(B.array()));
						B.position(0);
						B.get(p, b, W); //load input indexes
						System.out.println(Arrays.toString(p));
						tPayloadsOut = put(p,tPayloadsIn, tPayloadsOut); //... streaming stores
						System.out.println(Arrays.toString(tPayloadsOut.values().toArray(new Row [tPayloadsOut.size()])));
					}
					B.get(p, B.limit() - W, W); //move overflow ...
					B.put(p, 0, W); //... indexes to start
					j = j + B.limit() - W; // update output index
					l = l - B.limit() + W; // update buffer index
				}
			}
			// update index vector
		} // flush last items after the loop
		return tPayloadsOut.values().toArray(new Row [tPayloadsOut.size()]);
	}
}
