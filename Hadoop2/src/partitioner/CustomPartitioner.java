package partitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner<K, V> extends Partitioner<K, V> {

	private static final int PART_00 = 0;
	private static final int PART_01 = 1;
	private static final int PART_02 = 2;

	/** Use {@link Object#hashCode()} to partition. */
	public int getPartition(K key, V value, int numReduceTasks) {
		
		
		if (key.toString().equalsIgnoreCase("paracetamol"))
			return PART_00;
		else if (key.toString().equalsIgnoreCase("metacin"))
			return PART_01;
		else
			return PART_02;
	}
	
}