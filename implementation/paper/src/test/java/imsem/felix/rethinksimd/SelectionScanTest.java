package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SelectionScanTest {
	
	public Row [] initTable() throws IOException {
		return Utils.loadCSVResource("data/test.tbl", ",");
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

		Row [] resultTable = Utils.loadCSVResource("data/result1.tbl", ",");

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

		Row [] resultTable = Utils.loadCSVResource("data/result1.tbl", ",");

		Assert.assertArrayEquals(resultTable, result);
	}

	@Test
	public void testSelectionScanVector() throws IOException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);

		try {
			Utils.printBuffer(tableBuffer, table.length);
		} catch (Exception e) {
			e.printStackTrace();
		}

		double [] kLower = {1.0, 1.0};
		double [] kUpper = {2.0, 2.0};
		double [] [] tKeysIn = Utils.generateKeysIn(table, new int[] {0,1});

		ByteBuffer result = SelectionScan.selectionScanVector(4, 2, tKeysIn, kLower, kUpper, tableBuffer);

		Row [] resultTable = Utils.loadCSVResource("data/result1.tbl", ",");
		Row [] resultRows = Utils.toRows(result, resultTable.length);

		Assert.assertArrayEquals(resultTable, resultRows);
	}
}
