import org.apache.hadoop.mapreduce.Partitioner;

public class DrugPartitioner<K, V> extends Partitioner<K, V> {

	/** Use {@link Object#hashCode()} to partition. */
	public int getPartition(K key, V value, int numReduceTasks) {
		
		// return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		
		if (key.toString().equalsIgnoreCase("paracetamol"))
			return 0;
		else if (key.toString().equalsIgnoreCase("metacin"))
			return 1;
		else
			return 2;
	}
	
}