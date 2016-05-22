package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.data.Row;
import imsem.felix.rethinksimd.data.hash.Bucket;
import imsem.felix.rethinksimd.data.hash.HashTable;
import imsem.felix.rethinksimd.util.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LinearProbingTest {
	
	public static Row [] initTable() throws IOException {
		return Utils.loadCSVResource("data/test3.tbl", ",");
	}
	
	public static HashTable resultingHashTable(Row [] table, Double [] tKeysIn) throws IOException {
		HashTable t_should = new HashTable(100);
		t_should.put(48, new Bucket(tKeysIn[0],Utils.toByte(table[0])));
		t_should.put(24, new Bucket(tKeysIn[1],Utils.toByte(table[1])));
		t_should.put(12, new Bucket(tKeysIn[2],Utils.toByte(table[2])));
		t_should.put(0, new Bucket(tKeysIn[3],Utils.toByte(table[3])));

		t_should.put(44, new Bucket(tKeysIn[4],Utils.toByte(table[4])));
		t_should.put(88, new Bucket(tKeysIn[5],Utils.toByte(table[5])));
		t_should.put(32, new Bucket(tKeysIn[6],Utils.toByte(table[6])));
		t_should.put(76, new Bucket(tKeysIn[7],Utils.toByte(table[7])));

		t_should.put(49, new Bucket(tKeysIn[8],Utils.toByte(table[8])));
		t_should.put(20, new Bucket(tKeysIn[9],Utils.toByte(table[9])));
		t_should.put(92, new Bucket(tKeysIn[10],Utils.toByte(table[10])));
		t_should.put(64, new Bucket(tKeysIn[11],Utils.toByte(table[11])));
		
		return t_should;
	}

	@Test
	public void testBuildScalar() throws IOException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);
		Utils.printTable(table);
		
		
		Double [] tKeysIn = Utils.generateKeysIn(table, 0);
		
		HashTable T = LinearProbing.buildScalar(tKeysIn, tableBuffer, 100);
		System.out.print(T);

		HashTable t_should = resultingHashTable(table, tKeysIn);

		Assert.assertEquals(t_should, T);
	}

	@Test
	public void testBuildVector() throws IOException {
		Row [] table = initTable();
		ByteBuffer tableBuffer = Utils.toByte(table);
		Utils.printTable(table);


		Double [] tKeysIn = Utils.generateKeysIn(table, 0);

		int W = 2;
		HashTable T = LinearProbing.buildVector(W, tKeysIn, tableBuffer, 100);
		System.out.print(T);

		HashTable t_should = resultingHashTable(table, tKeysIn);

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

		HashTable T = LinearProbing.buildScalar(tKeysIn, tableBuffer, 100);
		System.out.print(T);

		Double [] keys = {1.0, 2.0};
		
		
		ByteBuffer foundTuples = LinearProbing.probeScalar(keys, tableBuffer2, T);

		int row_byte_size = 387;
		System.out.println("Out:");
		Utils.printBuffer(foundTuples, foundTuples.position() / row_byte_size);

		Row [] result = Utils.loadCSVResource("data/result2.tbl", ",");
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

		HashTable T = LinearProbing.buildScalar(tKeysIn, tableBuffer, 100);
		System.out.print(T);

		Double [] keys = {1.0, 2.0};

		int W = 2;
		ByteBuffer foundTuples = LinearProbing.probeVector(W, keys, tableBuffer2, T);

		int row_byte_size = 387;
		System.out.println("Out:");
		Utils.printBuffer(foundTuples, foundTuples.position() / row_byte_size);

		Row [] result = Utils.loadCSVResource("data/result2.tbl", ",");
		ByteBuffer resultBuffer = Utils.toByte(result);

		Assert.assertEquals(resultBuffer, foundTuples);
	}

	
}
