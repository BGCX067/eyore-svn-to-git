package edu.colorado.eyore.common.example.vertex;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.colorado.eyore.common.hdfs.HdfsUtils.OutputInfo;
import edu.colorado.eyore.common.io.EBufferedReader;
import edu.colorado.eyore.common.io.EBufferedWriter;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexOutput;

public class MapWordFreq extends Vertex {

	private static final Pattern LINE_RE = Pattern
			.compile("^en\\s+(\\S+)\\s+\\d+\\s+(\\d+)$");

	@Override
	public void run(VertexContext context) throws IOException {
		
		TreeMap<String, Long> map = new TreeMap<String, Long>();
		
		int verticesInNextStage;
		if(context.getNumVerticesNextStage() == null){
			// no next stage.  for testing purposes allow this case and use 4 output files
			verticesInNextStage = 4;
		}else{			
			verticesInNextStage = context.getNumVerticesNextStage();
		}

		// Iterate over all InputStreams and for each iterate over all lines
		Iterator<InputStream> itr = context.getInputs().iterator();
		while(itr.hasNext()){
			// Use Eyore BufferedReader so that we can change buffer size if necessary
			EBufferedReader reader = new EBufferedReader(new InputStreamReader(itr.next()));
			String nextLine = reader.readLine();
			Matcher matcher = null;
			String url = null;
			Long freq = null;
			while(nextLine != null){
				matcher = LINE_RE.matcher(nextLine);
				if(! matcher.matches()){
					throw new RuntimeException("did not match line: " + nextLine);
				}
				url = matcher.group(1);
				freq = Long.parseLong(matcher.group(2));
				
				if(! map.containsKey(url)){
					map.put(url, freq);
				}else{
					map.put(url, map.get(url)+freq);
				}
				nextLine = reader.readLine();
			}
		}

		// For each vertex in the next stage, setup an output file
		// - use Eyore BufferedWriter so that we can change buffer size if necessary
		EBufferedWriter[] writers = new EBufferedWriter[verticesInNextStage];
		String[] outFiles = new String[verticesInNextStage];		
		for(int i = 0; i < writers.length; i++){
			OutputInfo outInfo = context.getHdfs().getHdfsFileOutputStream(
					context.getJobId(), 
					context.getVertexIndex(),
					context.getStageIndex(),
					getClass());

			writers[i] = new EBufferedWriter(new OutputStreamWriter(outInfo.getOutputStream()));
			outFiles[i] = outInfo.getFilePath();
		}

		// partition the "URL COUNT" lines over the output
		int destination = -1;
		for(String url : map.keySet()){
			destination = url.hashCode()%verticesInNextStage;
			destination *= (destination <0 ? -1 : 1);
			writers[destination].write(url + " " + map.get(url) + "\n");
		}

		
		
		// Now write the info that says where each output file is 
		// going (which vertex index [0,n-1] ) in the next vertex stage
		Map<Integer, List<String>> outMap = new HashMap<Integer, List<String>>();
		if(context.getNumVerticesNextStage() == null){
			// This is test mode & there's no next stage 
			// - all output goes to the final output directory
			ArrayList<String> filesForVertex = new ArrayList<String>();
			for(int i = 0; i < writers.length; i++){
				writers[i].close();
				filesForVertex.add(outFiles[i]);				
			}
			outMap.put(null, filesForVertex);	
		}else{
			// Normal mode - files go to the correct vertex in next stage
			for(int i = 0; i < writers.length; i++){
				writers[i].close();
				ArrayList<String> filesForVertex = new ArrayList<String>(1);
				filesForVertex.add(outFiles[i]);
				outMap.put(i, filesForVertex);	
			}
		}
		
		VertexOutput vOut = new VertexOutput();
		vOut.setOutputMap(outMap);
		context.setvOutput(vOut);

	}

}
