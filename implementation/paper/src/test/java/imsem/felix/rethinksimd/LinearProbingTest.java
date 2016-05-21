package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.Row;
import imsem.felix.rethinksimd.data.Value;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class LinearProbingTest {
	
	public static Row [] initTable() throws IOException {
		return Utils.loadCSVResource("data/test.tbl", ",");
	}
	
	public static LinearProbing.HashTable resultingHashTable(Row [] table, Double [] tKeysIn) throws IOException {
		LinearProbing.HashTable t_should = new LinearProbing.HashTable(100);
		t_should.put(48, new LinearProbing.Bucket(tKeysIn[0],Utils.toByte(table[0])));
		t_should.put(49, new LinearProbing.Bucket(tKeysIn[1],Utils.toByte(table[1])));
		t_should.put(50, new LinearProbing.Bucket(tKeysIn[2],Utils.toByte(table[2])));
		t_should.put(51, new LinearProbing.Bucket(tKeysIn[3],Utils.toByte(table[3])));

		t_should.put(24, new LinearProbing.Bucket(tKeysIn[4],Utils.toByte(table[4])));
		t_should.put(25, new LinearProbing.Bucket(tKeysIn[5],Utils.toByte(table[5])));
		t_should.put(26, new LinearProbing.Bucket(tKeysIn[6],Utils.toByte(table[6])));
		t_should.put(27, new LinearProbing.Bucket(tKeysIn[7],Utils.toByte(table[7])));

		t_should.put(12, new LinearProbing.Bucket(tKeysIn[8],Utils.toByte(table[8])));
		t_should.put(13, new LinearProbing.Bucket(tKeysIn[9],Utils.toByte(table[9])));
		t_should.put(14, new LinearProbing.Bucket(tKeysIn[10],Utils.toByte(table[10])));
		t_should.put(15, new LinearProbing.Bucket(tKeysIn[11],Utils.toByte(table[11])));
		
		return t_should;
	}

	@Test
	public void testBuildScalar() throws IOException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);
		Utils.printTable(table);
		
		
		Double [] tKeysIn = Utils.generateKeysIn(table, 0);
		
		LinearProbing.HashTable T = LinearProbing.buildScalar(tKeysIn, tableBuffer, 100);
		System.out.print(T);

		LinearProbing.HashTable t_should = resultingHashTable(table, tKeysIn);

		Assert.assertEquals(t_should, T);
	}

	@Test
	public void testBuildVector() throws IOException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);
		Utils.printTable(table);


		Double [] tKeysIn = Utils.generateKeysIn(table, 0);

		int W = 2;
		LinearProbing.HashTable T = LinearProbing.buildVector(W, tKeysIn, tableBuffer, 100);
		System.out.print(T);

		LinearProbing.HashTable t_should = resultingHashTable(table, tKeysIn);

		Assert.assertEquals(t_should, T);
	}

	@Test
	public void testProbeScalar() throws IOException, ClassNotFoundException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);
		Utils.printTable(table);


		Row [] table2 = Utils.loadCSVResource("data/test2.tbl", ",");
		ByteBuffer tableBuffer2 = Utils.toByte(table2);

		Double [] tKeysIn = Utils.generateKeysIn(table, 0);

		LinearProbing.HashTable T = LinearProbing.buildScalar(tKeysIn, tableBuffer, 100);
		System.out.print(T);

		Double [] keys = {1.0, 2.0};
		
		
		ByteBuffer foundTuples = LinearProbing.probeScalar(keys, tableBuffer2, T);
		
		System.out.println("Out:");
		Utils.printBuffer(foundTuples, 8);

		Row [] result = Utils.loadCSVResource("data/result1.tbl", ",");
		ByteBuffer resultBuffer = Utils.toByte(result);
		
		Assert.assertEquals(resultBuffer, foundTuples);
	}

	@Test
	public void testProbeVector() throws IOException, ClassNotFoundException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);
		Utils.printTable(table);


		Row [] table2 = Utils.loadCSVResource("data/test2.tbl", ",");
		ByteBuffer tableBuffer2 = Utils.toByte(table2);

		Double [] tKeysIn = Utils.generateKeysIn(table, 0);

		LinearProbing.HashTable T = LinearProbing.buildScalar(tKeysIn, tableBuffer, 100);
		System.out.print(T);

		Double [] keys = {1.0, 2.0};

		int W = 2;
		ByteBuffer foundTuples = LinearProbing.probeVector(W, keys, tableBuffer2, T);

		int row_byte_size = 387;
		System.out.println("Out:");
		Utils.printBuffer(foundTuples, foundTuples.position() / row_byte_size);

		Row [] result = Utils.loadCSVResource("data/result1.tbl", ",");
		ByteBuffer resultBuffer = Utils.toByte(result);

		Assert.assertEquals(resultBuffer, foundTuples);
	}

	
}
