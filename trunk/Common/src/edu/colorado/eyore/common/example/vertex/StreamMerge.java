package edu.colorado.eyore.common.example.vertex;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.colorado.eyore.common.hdfs.HdfsUtils.OutputInfo;
import edu.colorado.eyore.common.io.EBufferedReader;
import edu.colorado.eyore.common.io.EBufferedWriter;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexOutput;

/**
 * Merge N sorted input streams into one (sorted)
 * output stream
 * 
 *  - in common because multiple jobs will use this vertex
 * 
 */
public class StreamMerge extends Vertex {

	@Override
	public void run(VertexContext context) throws IOException {

		OutputInfo outInfo = context.getHdfs().getHdfsFileOutputStream(context.getJobId(),
				context.getVertexIndex(), context.getStageIndex(),
				this.getClass());
		
		OutputStream os = outInfo.getOutputStream();
		
		// An Eyore BufferedWriter - so we can change buffer sizes in one place if
		// it ends up mattering.  See also EBufferedReader.
		EBufferedWriter writer = null;
		try{
			List<InputStream> inStream = context.getInputs();
			
			// Using a BufferedWriter because it's probably
			// the best choice for performance
			writer = new EBufferedWriter(new OutputStreamWriter(os));
			StreamMerge.mergeSort(inStream, writer);
		
		} 
		finally {
			
			// Make sure to call close so that any buffered output gets written
			// otherwise output may go missing
			writer.close();
		}
		
		// Setup our output map.
		VertexOutput vOutput = new VertexOutput();
		List<String> output = new ArrayList<String>();
		output.add(outInfo.getFilePath());
		Map<Integer, List<String>> outputMap = new HashMap<Integer, List<String>>();
		if(context.getNumVerticesNextStage() == null || context.getNumVerticesNextStage() == 0){
			outputMap.put(null, output);	
		}else{
			outputMap.put(0, output);	
		}
		
		vOutput.setOutputMap(outputMap);
		context.setvOutput(vOutput);
	}
	
	public static void mergeSort(List<InputStream> inStream,
			EBufferedWriter writer) throws IOException {

		LinkedList<LookAheadReader> readers = new LinkedList<LookAheadReader>();
		for (int index = 0; index < inStream.size(); index++ ) {
			readers.add(new LookAheadReader(new EBufferedReader(new InputStreamReader(inStream.get(index)))));
		}

		Iterator<LookAheadReader> readerItr;
		while(readers.size() > 0){
			readerItr = readers.iterator();
			LookAheadReader readerWithLargestCount = null;
			Long largestCount = Long.MIN_VALUE;
			String nextLine = null;
			Long nextCount = null;
			LookAheadReader nextReader = null;
			while(readerItr.hasNext()){
				nextReader = readerItr.next();
				nextLine = nextReader.peekNextLine();
				if(nextLine == null){
					readerItr.remove();
					continue;
				}
				nextCount = Long.parseLong(nextLine.split("\\s")[1]);
				if(nextCount > largestCount){
					largestCount = nextCount;
					readerWithLargestCount = nextReader;
				}
			}
			if(readerWithLargestCount != null){
				writer.write(readerWithLargestCount.readLine() + "\n");
			}
		}
	}
	
	
	/**
	 * A decorator over BufferedReader that allows seeing what the
	 * next line is without actually reading the next line 
	 *
	 */
	public static class LookAheadReader extends EBufferedReader{

		public LookAheadReader(Reader in)throws IOException {
			super(in);
		}
		
		@Override
		public int read() throws IOException {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public int read(char[] cbuf) throws IOException {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public int read(char[] cbuf, int off, int len) throws IOException {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public int read(CharBuffer target) throws IOException {
			throw new UnsupportedOperationException();
		}

		private boolean nextLineValid = false;
		private String nextLine = null;
		public String peekNextLine() throws IOException{
			if(nextLineValid){
				return nextLine;
			}else{
				nextLine = super.readLine();
				nextLineValid = true;
				return nextLine;
			}			
		}

		@Override
		public String readLine() throws IOException {
			if(nextLineValid){
				nextLineValid = false;
				return nextLine;				
			}else{
				String next = super.readLine();
				return next;
			}
		}
		
	}
	
}
