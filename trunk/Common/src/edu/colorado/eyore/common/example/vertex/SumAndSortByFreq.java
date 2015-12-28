package edu.colorado.eyore.common.example.vertex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import edu.colorado.eyore.common.hdfs.HdfsUtils.OutputInfo;
import edu.colorado.eyore.common.io.EBufferedWriter;
import edu.colorado.eyore.common.util.DescLongComparator;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexOutput;

public class SumAndSortByFreq extends Vertex {

	@Override
	public void run(VertexContext context) throws IOException {
		
		
		int verticesInNextStage;
		if(context.getNumVerticesNextStage() == null){
			// no next stage.  for testing purposes allow this case and use 4 output files
			verticesInNextStage = 1;
		}else{			
			verticesInNextStage = context.getNumVerticesNextStage();
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


		TreeMap<Long, Set<String>> descFreqSortMap = SumAndSortByFreq.getSortedMap(context.getInputs());
		
		Iterator<Long> descFreqItr = descFreqSortMap.keySet().iterator();
		Long nextFreq;
		int destination = -1;
		while(descFreqItr.hasNext()){
			nextFreq = descFreqItr.next();
			Iterator<String> wordItr = descFreqSortMap.get(nextFreq).iterator();			
			while(wordItr.hasNext()){
				String word = wordItr.next();

				if(context.getNumVerticesNextStage() == null || context.getNumVerticesNextStage() == 0){
					destination = 0;
				}else{
					destination = context.getVertexIndex() % verticesInNextStage;
				}
				writers[destination].write(word + " " + nextFreq + "\n");
			}
		}

		
		
		// Now write the info that says where each output file is 
		// going (which vertex index [0,n-1] ) in the next vertex stage
		Map<Integer, List<String>> outMap = new HashMap<Integer, List<String>>();

		// Normal mode - files go to the correct vertex in next stage
		for(int i = 0; i < writers.length; i++){
			writers[i].close();
			ArrayList<String> filesForVertex = new ArrayList<String>(1);
			filesForVertex.add(outFiles[i]);
			if(context.getNumVerticesNextStage() == null || context.getNumVerticesNextStage() == 0){
				outMap.put(null, filesForVertex);
			}else{
				outMap.put(i, filesForVertex);
			}
		}

		
		VertexOutput vOut = new VertexOutput();
		vOut.setOutputMap(outMap);
		context.setvOutput(vOut);

		
	}
	
	public static TreeMap<Long, Set <String>> getSortedMap(List<InputStream> inStream)throws IOException {
		// The comparator is passed to the map so that it does descending order instead
		// of ascending order sort
		TreeMap<String, Long> wordTotalMap = new TreeMap<String, Long>();
		for (int i = 0; i < inStream.size(); i++ ) {
			BufferedReader br = new BufferedReader(new InputStreamReader(inStream.get(i)));
			String strLine;

				while ((strLine = br.readLine()) != null) {

					String split[] = strLine.split("\\s");
					String word = split[0];
					long count = Long.parseLong(split[1]);
					
					// Check if count already exists
					if(wordTotalMap.containsKey(word)){
						count += wordTotalMap.get(word);
					}
					wordTotalMap.put(word, count);
				}

				br.close();
		}
		
		TreeMap<Long, Set<String>> descFreqSortMap = new TreeMap<Long, Set<String>>(new DescLongComparator());
		Iterator<String> wordItr = wordTotalMap.keySet().iterator();
		String nextWord = null;
		Long nextFreq = null;
		while(wordItr.hasNext()){
			nextWord = wordItr.next();
			nextFreq = wordTotalMap.get(nextWord);
			Set<String> wordSet;
			if(! descFreqSortMap.containsKey(nextFreq)){
				wordSet = new HashSet<String>();
			}else{
				wordSet = descFreqSortMap.get(nextFreq);
			}
			wordSet.add(nextWord);
			descFreqSortMap.put(nextFreq, wordSet);
		}

		return descFreqSortMap;
	}
}
