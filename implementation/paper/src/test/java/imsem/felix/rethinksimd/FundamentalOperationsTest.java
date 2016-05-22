package imsem.felix.rethinksimd;

import imsem.felix.rethinksimd.util.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;


public class FundamentalOperationsTest {

	@Test
	public void testSelectiveStore()
	{
		Character [] vector = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'};
		int [] mask = {0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1};
		Character [] memory = {'B', 'D', 'F', 'G', 'I', 'K', 'N', 'O', 'P'};

		Character [] memoryResult = FundamentalOperations.selectiveStore(Character.class, vector, Utils.toBitSet(mask));
		
		System.out.println("memory: " + Arrays.toString(memoryResult));

		Assert.assertArrayEquals(memory, memoryResult);
	}

	@Test
	public void testSelectiveLoad()
	{
		Character [] vector = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'};
		int [] mask = {0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1};
		Character [] memory = {'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
		Character [] vectorNew = {'A', 'R', 'C', 'S', 'E', 'T', 'U', 'H', 'V', 'J', 'W', 'L', 'M', 'X', 'Y', 'Z'};

		Character [] vectorNewResult = FundamentalOperations.selectiveLoad(Character.class, vector, memory, 0, Utils.toBitSet(mask));

		System.out.println("new vector: " + Arrays.toString(vectorNewResult));

		Assert.assertArrayEquals(vectorNew, vectorNewResult);
	}

	@Test
	public void testGather()
	{
		int [] indexVector = {2, 9, 0, 12, 30, 19, 17, 27, 12, 26, 15, 1, 15, 9, 31, 23};
		Character [] memory = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%', '^'};
		Character [] valueVector = {'C', 'J', 'A', 'M', '%', 'T', 'R', '@', 'M', '!', 'P', 'B', 'P', 'J', '^', 'X'};

		Character [] valueVectorResult = FundamentalOperations.gather(Character.class, indexVector, memory);

		System.out.println("new vector: " + Arrays.toString(valueVectorResult));

		Assert.assertArrayEquals(valueVector, valueVectorResult);
	}

	@Test
	public void testScatter()
	{
		Character [] valueVector = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'};
		int [] indexVector = {2, 9, 0, 12, 30, 19, 17, 27, 12, 26, 15, 1, 15, 9, 31, 23};
		Character [] memoryNew = {'C', 'L', 'A', null, null, null, null, null, null, 'N', null, null, 'I', null, null, 'M', null, 'G', null, 'F', null, null, null, 'P', null, null, 'J', 'H', null, null, 'E', 'O'};
		Character [] memory = new Character[memoryNew.length];
		
		Character [] memoryResult = FundamentalOperations.scatter(Character.class, valueVector, indexVector, memory);

		System.out.println("new vector: " + Arrays.toString(memoryResult));

		Assert.assertArrayEquals(memoryNew, memoryResult);
	}
}
