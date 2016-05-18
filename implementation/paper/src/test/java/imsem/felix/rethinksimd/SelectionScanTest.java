package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class SelectionScanTest {
	
	public Row [] initTable() throws IOException {
		return Utils.loadCSV("/home/felix/RethinkingSIMD/src/test/resources/data/test.tbl", ",");
	}

	@Test
	public void testSelectionScanBranching() throws IOException {
		Row [] table = initTable();
		Utils.printTable(table);
		
		double [] kLower = {1.0, 1.0};
		double [] kUpper = {2.0, 2.0};
		double [] [] tKeysIn = Utils.generateKeysIn(table, new int[] {0,1});
		
		Row [] result = SelectionScan.selectionScanBranching(tKeysIn, kLower, kUpper, table);
		Utils.printTable(result);

		Row [] resultTable = Utils.loadCSV("/home/felix/RethinkingSIMD/src/test/resources/data/result1.tbl", ",");

		Assert.assertArrayEquals(resultTable, result);
	}

	@Test
	public void testSelectionScanBranchless() throws IOException {
		Row [] table = initTable();
		
		double [] kLower = {1.0, 1.0};
		double [] kUpper = {2.0, 2.0};
		double [] [] tKeysIn = Utils.generateKeysIn(table, new int[] {0,1});

		Row [] result = SelectionScan.selectionScanBranchless(tKeysIn, kLower, kUpper, table);
		Utils.printTable(result);

		Row [] resultTable = Utils.loadCSV("/home/felix/RethinkingSIMD/src/test/resources/data/result1.tbl", ",");

		Assert.assertArrayEquals(resultTable, result);
	}

	@Test
	public void testSelectionScanVector() throws IOException {
		Row [] table = initTable();

		double [] kLower = {1.0, 1.0};
		double [] kUpper = {2.0, 2.0};
		double [] [] tKeysIn = Utils.generateKeysIn(table, new int[] {0,1});

		Row [] result = SelectionScan.selectionScanVector(3, tKeysIn, kLower, kUpper, table);
		Utils.printTable(result);

		Row [] resultTable = Utils.loadCSV("/home/felix/RethinkingSIMD/src/test/resources/data/result1.tbl", ",");

		Assert.assertArrayEquals(resultTable, result);
	}
}
