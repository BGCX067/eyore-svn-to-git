package edu.colorado.eyore.common.hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;

public class LineSplitFSDataInputStream extends InputStream{

	private byte[] currentLine;
	private int pos;
	private SplitInputLineReader lineReader;
	boolean inited = false;
	public LineSplitFSDataInputStream(FSDataInputStream in, HdfsFileSplit split)throws IOException{
		lineReader = new SplitInputLineReader(in, split);
	}
			
	
	@Override
	public int read() throws IOException {
		if(! inited){
			currentLine = lineReader.nextLine();
			pos = 0;
			inited = true;
		}
		if(currentLine == null){
			return -1;
		}
		
		if(pos < currentLine.length){
			pos++;
			return currentLine[pos-1];
		}else{
			currentLine = lineReader.nextLine();
			if(currentLine == null){
				return -1;
			}else{
				pos = 1;
				return currentLine[0];
			}
		}
	}



	public static class SplitInputLineReader {
		protected FSDataInputStream in;
		protected long bytesRead = 0;
		protected HdfsFileSplit split;

		protected boolean finished = false;

		public SplitInputLineReader(FSDataInputStream in, HdfsFileSplit split)
				throws IOException {
			this.in = in;
			this.split = split;

			if(split.getLenInBytes() == 0){
				finished = true;
				try{
					in.close();
				}catch(IOException e){
					
				}
				return;
			}
			
			if (split.getStartByte() == 0) {
				// this is start of line so 
				// return
				return;
			}

			// See if this is start of a line
			in.seek(split.getStartByte() - 1); 
			byte test = in.readByte();
			in.seek(split.getStartByte());
			if (test != '\n') {
				// Start byte is NOT the start of a line, so discard
				// input until the next start of a line
				forwardUntilNextLineStart();
			}
			
			// upon return, next byte read should be at the start of a 
			// line
		}
		
		public byte[] nextLine()throws IOException{
			ArrayList<Byte> nextLine = new ArrayList<Byte>();
			if(finished){
				try{
					in.close();
				}catch(IOException e){
					
				}
				return null;
			}
			byte nextChar = 0;
			while(nextChar != '\n'){
				try{
					nextChar = in.readByte();
					bytesRead++;
					nextLine.add(nextChar);
				}catch(EOFException e){
					finished = true;
					break;
				}
			}
			if(bytesRead >= split.getLenInBytes()){
				finished = true;
			}
			byte[] line = new byte[nextLine.size()];
			for(int i = 0; i < line.length; i++){
				line[i] = nextLine.get(i);
			}
			return line;
		}

		protected void forwardUntilNextLineStart() throws IOException {
			try {
				byte nextChar = in.readByte();
				bytesRead++;
				while (nextChar != '\n') {
					nextChar = in.readByte();
					bytesRead++;
				}
			} catch (EOFException e) {
				finished = true;
				try{
					in.close();
				}catch(IOException ee){
					
				}
			}
			
			if(bytesRead == split.getLenInBytes()){
				finished = true;
				try{
					in.close();
				}catch(IOException e){
					
				}
			}			
		}

	}

}
