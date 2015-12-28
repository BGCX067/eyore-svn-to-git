package edu.colorado.eyore.common.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

public class HdfsFileSplit {

		private long startByte;
		private long lenInBytes;
		private String fileName;
		
		public long getStartByte(){
			return startByte;
		}
		public long getLenInBytes(){
			return lenInBytes;
		}
		
		public String getFileName(){
			return fileName;
		}
	
		public HdfsFileSplit(String fileName, long startByte, long lenInBytes){
			this.fileName = fileName;
			this.startByte = startByte;
			this.lenInBytes = lenInBytes;
		}
		
		private static final Pattern splitStrPattern = Pattern.compile("^split:(\\d+)[-](\\d+)[/][/](.+)$");
		public static HdfsFileSplit getSplitFromString(String split){
			Matcher m = splitStrPattern.matcher(split);
			if(! m.matches()){
				throw new RuntimeException("didnt match");
			}
			return new HdfsFileSplit(m.group(3), Long.parseLong(m.group(1)), Long.parseLong(m.group(2)));
		}
		public static boolean isPathAFileSpit(String testString){
			Matcher m = splitStrPattern.matcher(testString);
			return m.matches();
		}
		
		@Override
		public String toString() {
			return "split:" + startByte + "-" + lenInBytes + "//" + fileName;
		};
		
		public static List<HdfsFileSplit> split(String hdfsFilePath, HdfsUtils uts, int splitsPerHdfsBlock)
				throws IOException {
			

			Path p = new Path(hdfsFilePath);
			
			final long chunkSize = uts.hdfsCluster.getFileStatus(p).getBlockSize() * splitsPerHdfsBlock;
			
			final long fileLen = uts.hdfsCluster.getFileStatus(p).getLen();
			ArrayList<HdfsFileSplit> splits = new ArrayList<HdfsFileSplit>();
			
			if(fileLen == 0){
				return splits;
			}
			
			//System.err.println("file len is " + fileLen);
			if (fileLen <= chunkSize) {
				HdfsFileSplit fs = new HdfsFileSplit(hdfsFilePath, 0L, fileLen);
				splits.add(fs);
			} else {
				long nextSplitStart = 0;
				while (nextSplitStart < fileLen) {
					HdfsFileSplit fs;
					if ((fileLen - nextSplitStart) > chunkSize) {
						fs = new HdfsFileSplit(hdfsFilePath, nextSplitStart, chunkSize);
						nextSplitStart += chunkSize;
					} else {
						fs = new HdfsFileSplit(hdfsFilePath, nextSplitStart, fileLen - nextSplitStart);
						nextSplitStart += fs.lenInBytes;
					}
					splits.add(fs);
				}
			}

			return splits;
		}
}
