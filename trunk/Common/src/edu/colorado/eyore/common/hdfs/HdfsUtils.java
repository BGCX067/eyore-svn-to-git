package edu.colorado.eyore.common.hdfs;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {

	FileSystem hdfsCluster;

	public HdfsUtils(String namenodeAddr, int namenodePort) throws IOException {
		hdfsCluster = new Path("hdfs://" + namenodeAddr + ":" + namenodePort
				+ "/").getFileSystem(new Configuration());

	}

	/**
	 * Writes an existing local file to the HDFS file system at the HDFS path
	 * specified
	 * 
	 * @param localPath
	 *            - local file - e.g. new File("/root/home/file.txt")
	 * @param remotePath
	 *            - HDFS path - e.g. "/dir/file.txt"
	 * 
	 * @throws IOException
	 */
	public void writeLocalFileToHdfs(File localPath, String remotePath)
			throws IOException {
		Path local = new Path(localPath.getAbsolutePath());
		Path remote = new Path(remotePath);

		final boolean deleteTheSourceFile = false;
		final boolean overriteTheDestFile = true;
		hdfsCluster.copyFromLocalFile(deleteTheSourceFile, overriteTheDestFile,
				local, remote);
		// this is a quick and dirty "replicate jar to all
		// data nodes"
		hdfsCluster.setReplication(remote, (short) 512);
	}

	/**
	 * Writes an existing HDFS file to the local file system - will overwrite if
	 * already exists
	 * 
	 * @param remotePath
	 *            - HDFS path - e.g. "/dir/file.txt"
	 * @param localPath
	 *            - path of the local file to write (not its parent directory) -
	 *            e.g. new File("/root/home/file.txt")
	 * 
	 * @throws IOException
	 */
	public void writeHdfsFileToLocal(String remotePath, File localPath)
			throws IOException {
		Path local = new Path(localPath.getAbsolutePath());
		Path remote = new Path(remotePath);
		final boolean deleteTheSourceFile = false;
		hdfsCluster.copyToLocalFile(deleteTheSourceFile, remote, local);
	}

	/**
	 * Returns the full paths to files inside the specified directory (not
	 * recursively)
	 * 
	 * @param hdfsDirPath
	 *            - e.g. /some/directory
	 * @return - e.g. /some/directory/file1, /some/directory/file2
	 */
	public List<String> getFilePathsFromHdfsDir(String hdfsDirPath, int fileSplitsPerHdfsBlock)
			throws IOException {

		ArrayList<String> files = new ArrayList<String>();

		Path hdfsDir = new Path(hdfsDirPath);

		FileStatus[] outputFileStatus = hdfsCluster.listStatus(hdfsDir);
		for (FileStatus status : outputFileStatus) {
			if (!status.isFile()) {
				continue;
			}
			Path file = status.getPath();
			String filePath = "/" + hdfsDir.getName() + "/" + file.getName();
			for(HdfsFileSplit split : HdfsFileSplit.split(filePath, this, fileSplitsPerHdfsBlock)){
				files.add(split.toString());	
			}
			
		}

		return files;
	}

	/**
	 * This function renames the HDFS files specified so that they are located
	 * in the specified directory instead of their current HDFS directory
	 * 
	 * @param files
	 *            - list of HDFS file paths - full paths to HDFS files
	 * @param newDirectory
	 *            - the files will be renamed so that they now exist in this
	 *            directory
	 */
	public void moveFilesToNewDir(List<String> files, String newDirectory)
			throws IOException {
		Path dirPath = new Path(newDirectory);
		hdfsCluster.mkdirs(dirPath);
		FileStatus dirStatus = hdfsCluster.getFileStatus(dirPath);

		for (String file : files) {
			Path origFile = new Path(file);
			FileStatus origFileStatus = hdfsCluster.getFileStatus(origFile);
			if (!origFileStatus.isFile()) {
				throw new IOException("The file " + file
						+ " could not be found on HDFS");
			}
			Path newFile = new Path(dirStatus.getPath() + "/"
					+ origFile.getName());

			hdfsCluster.rename(origFile, newFile);
		}
	}

	/**
	 * Validate HDFS directory path
	 * 
	 * @param dirPath
	 *            - path to dir on HDFS
	 * @return true if directory exists else false
	 * @throws IOException
	 */
	public boolean hdfsDirExists(String dirPath) {
		try {
			return hdfsCluster.getFileStatus(new Path(dirPath)).isDirectory();
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * Checks if a file path exists as a file on HDFS
	 * 
	 * @param filePath
	 * @return true if exists as file else false
	 * @throws IOException
	 */
	public boolean hdfsFileExists(String filePath) throws IOException {
		try {
			return hdfsCluster.getFileStatus(new Path(filePath)).isFile();
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * Get an input stream for reading from a file on HDFS - can wrap in a
	 * BufferedReader to read line by line just like reading from a normal file
	 * in Java
	 * 
	 * @param hdfsFilePath
	 * @return
	 * @throws IOException
	 */
	public InputStream getHdfsFileInputStream(String hdfsFilePath)
			throws IOException {
		if(HdfsFileSplit.isPathAFileSpit(hdfsFilePath)){
			HdfsFileSplit split = HdfsFileSplit.getSplitFromString(hdfsFilePath);			
			FSDataInputStream in = hdfsCluster.open(new Path(split.getFileName()));
			return new LineSplitFSDataInputStream(in, split); 
		}
		return hdfsCluster.open(new Path(hdfsFilePath));
	}

	/**
	 * Opens an HDFS output stream to a uniquely named file - file name also has
	 * job id and vertex index for debugging.
	 */
	@SuppressWarnings("unchecked")
	public OutputInfo getHdfsFileOutputStream(String jobId, int vertexIndex,
			int vertexStage, Class vertexClass) throws IOException {
		String uniqueFileName = "eyore/tmp/" + "job" + jobId + "_vIndex"
				+ vertexIndex + "_vStage" + vertexStage + "_class"
				+ vertexClass.getName() + "__" + UUID.randomUUID().toString()
				+ ".txt";

		OutputStream os = hdfsCluster.create(new Path(uniqueFileName), false);
		return new OutputInfo(os, uniqueFileName);
	}

	/**
	 * Contains an opened output stream (to HDFS) and the name of the path to
	 * the file being written
	 */
	public static class OutputInfo {
		private OutputStream os;
		private String filePath;

		OutputInfo(OutputStream os, String filePath) {
			this.os = os;
			this.filePath = filePath;
		}

		public OutputStream getOutputStream() {
			return os;
		}

		public String getFilePath() {
			return filePath;
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		String inputFileHdfs = "/f3.txt";
		File outputFile = File.createTempFile("one", "two");
		System.out.println(outputFile.getAbsolutePath());
		PrintStream fout  = new PrintStream(outputFile);
		
		HdfsUtils h = new HdfsUtils("localhost", 9000);
		Path file = new Path(inputFileHdfs);
		//FSDataInputStream in = 


		List<HdfsFileSplit> splits = HdfsFileSplit.split(inputFileHdfs, h, 64);
		
		for (HdfsFileSplit s : splits) {
			
			//System.err.println("SPLIT start " + s.startByte + " len " + s.len
				//	+ "[");
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(new LineSplitFSDataInputStream(h.hdfsCluster.open(file), s)));
			
			String line = reader.readLine();
			while(line != null){
				fout.println(line);
				line = reader.readLine();
			}
			
		}
	}

	

	

}
