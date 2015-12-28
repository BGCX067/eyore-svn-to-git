package edu.colorado.eyore.mrwordcount;

import java.io.BufferedReader;
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

import edu.colorado.eyore.common.hdfs.HdfsUtils.OutputInfo;
import edu.colorado.eyore.common.io.EBufferedWriter;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexOutput;

public class MRCountFreqVertex extends Vertex {
	@Override
	public void run(VertexContext context) throws IOException {
		
		OutputInfo outInfo = context.getHdfs().getHdfsFileOutputStream(context.getJobId(),
				context.getVertexIndex(), context.getStageIndex(),
				this.getClass());
		TreeMap<String, Long> wordMap = MRCountFreqVertex.getWordMap(context.getInputs());
		
		// For each vertex in the next stage, setup an output file
		// - use Eyore BufferedWriter so that we can change buffer size if necessary
		EBufferedWriter writer = new EBufferedWriter(new OutputStreamWriter(outInfo.getOutputStream()));
		Iterator<String> wordIter = wordMap.keySet().iterator();
		Iterator<Long> countIter = wordMap.values().iterator();
		while(wordIter.hasNext()) {
			writer.write(wordIter.next() + " " + countIter.next() + "\n");
		}
		
		writer.close();
		
		// Setup our output map.
		VertexOutput vOutput = new VertexOutput();
		List<String> output = new ArrayList<String>();
		output.add(outInfo.getFilePath());
		Map<Integer, List<String>> outputMap = new HashMap<Integer, List<String>>();
		outputMap.put(context.getVertexIndex(), output); // Pass to same vertex index in next stage.
		vOutput.setOutputMap(outputMap);
		context.setvOutput(vOutput);
	}
	
	public static TreeMap<String, Long> getWordMap(List<InputStream> inStream) throws IOException {

		TreeMap<String, Long> wordMap = new TreeMap<String, Long>();
		for (int i = 0; i < inStream.size(); i++ ) {
			BufferedReader br = new BufferedReader(new InputStreamReader(inStream.get(i)));
			String strLine;

			while ((strLine = br.readLine()) != null) {

				String split[] = strLine.split("\\s");
				String word = split[0];
				long count = Long.parseLong(split[1]);
					
				// Check if count already exists
				if(wordMap.containsKey(word)){
					count += wordMap.get(word);
				}
				wordMap.put(word, count);
			}

			br.close();
		}

		return wordMap;
	}
}
