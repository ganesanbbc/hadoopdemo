package org.samples.hdfs.training;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class JavaClient {

	public boolean ifExists(Path source) throws IOException {
		FileSystem hdfs = getFileSystem();
		boolean isExists = hdfs.exists(source);
		return isExists;
	}
	
	private Configuration getNamenodeController() {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:50000");
		return conf;
	}

	public void getHostnames() throws IOException {

		FileSystem fs = getFileSystem();

		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

		String[] names = new String[dataNodeStats.length];
		for (int i = 0; i < dataNodeStats.length; i++) {
			names[i] = dataNodeStats[i].getHostName();
			System.out.println((dataNodeStats[i].getHostName()));
		}
	}

	private FileSystem getFileSystem() throws IOException {
		Configuration config = getNamenodeController();
		FileSystem fs = FileSystem.get(config);
		return fs;
	}

	public void getBlockLocations(String source) throws IOException {

		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(source);

		// Check if the file already exists
		if (!(ifExists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		FileStatus fileStatus = fileSystem.getFileStatus(srcPath);

		BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		int blkCount = blkLocations.length;

		System.out.println("File :" + filename + "stored at:");
		for (int i = 0; i < blkCount; i++) {
			String[] hosts = blkLocations[i].getHosts();
			System.out.format("Host %d: %s %n", i, hosts);
		}

	}

	public void getModificationTime(String source) throws IOException {

		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(source);

		// Check if the file already exists
		if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
		long modificationTime = fileStatus.getModificationTime();

		System.out.format("File %s; Modification time : %0.2f %n", filename, modificationTime);

	}

	public void copyFromLocal(String source, String dest) throws IOException {

		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(source);

		Path dstPath = new Path(dest);
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		try {
			fileSystem.copyFromLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + "copied to " + dest);
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}

	public void copyToLocal(String source, String dest) throws IOException {

		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(source);

		Path dstPath = new Path(dest);
		// Check if the file already exists
		if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
		}

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		try {
			fileSystem.copyToLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + "copied to " + dest);
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}

	public void renameFile(String fromthis, String tothis) throws IOException {

		FileSystem fileSystem = getFileSystem();
		Path fromPath = new Path(fromthis);
		Path toPath = new Path(tothis);

		if (!(fileSystem.exists(fromPath))) {
			System.out.println("No such destination " + fromPath);
			return;
		}

		if (fileSystem.exists(toPath)) {
			System.out.println("Already exists! " + toPath);
			return;
		}

		try {
			boolean isRenamed = fileSystem.rename(fromPath, toPath);
			if (isRenamed) {
				System.out.println("Renamed from " + fromthis + "to " + tothis);
			}
		} catch (Exception e) {
			System.out.println("Exception :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}

	}

	public void addFile(String source, String dest) throws IOException {

		FileSystem fileSystem = getFileSystem();

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		} else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
	}

	public void readFile(String file) throws IOException {

		FileSystem fileSystem = getFileSystem();

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		FSDataInputStream in = fileSystem.open(path);

		String filename = file.substring(file.lastIndexOf('/') + 1, file.length());

		OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));

		byte[] b = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(b)) > 0) {
			out.write(b, 0, numBytes);
		}

		in.close();
		out.close();
		fileSystem.close();
	}

	public void deleteFile(String file) throws IOException {

		FileSystem fileSystem = getFileSystem();

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		fileSystem.delete(new Path(file), true);

		fileSystem.close();
	}

	// dir = /testjava
	public void mkdirsdd(String dir) throws IOException {

		FileSystem fs = getFileSystem();
		Path path = new Path(dir);
		if (fs.exists(path)) {
			System.out.println("Dir " + dir + " already exists!");
			return;
		}

		fs.mkdirs(path);
		System.out.println("created dir");

		fs.close();
	}

	

	public static void main(String[] args) throws IOException {

		JavaClient jc = new JavaClient();
		jc.mkdirsdd("/fromJavaProgram");
		
	}
}
