package edu.colorado.eyore.mrwordcount;

import java.util.ArrayList;

import edu.colorado.eyore.common.example.vertex.MapWordFreq;
import edu.colorado.eyore.common.example.vertex.StreamMerge;
import edu.colorado.eyore.common.example.vertex.SumAndSortByFreq;
import edu.colorado.eyore.common.job.JobSpecification;
import edu.colorado.eyore.common.vertex.VertexStage;

public class MRWordCountJobSpec extends JobSpecification {
	public MRWordCountJobSpec(){
		super.vertexStages = new ArrayList<VertexStage>();
	
		
		// **** STAGE 1 - Map Count freq per chunk
		VertexStage countPerChunk = new VertexStage();
		countPerChunk.setVertex(MapWordFreq.class);
		vertexStages.add(countPerChunk);
		
		// **** STAGE 2 - Reduce - frequency per word
		VertexStage totalCount = new VertexStage();
		totalCount.setNumVertices(19);
		totalCount.setVertex(MRCountFreqVertex.class);
		vertexStages.add(totalCount);
		
		// **** STAGE 3 - Map: Invert from (word, freq) to (freq, word).
		VertexStage invert = new VertexStage();
		invert.setNumVertices(19);
		invert.setVertex(SumAndSortByFreq.class);
		vertexStages.add(invert);
		
		// **** STAGE 4 - Reduce: (word, freq) sorted (desc.) by frequency
		//VertexStage merge = new VertexStage();
		//merge.setNumVertices(2);
		//merge.setVertex(StreamMerge.class);
		//vertexStages.add(merge);

		// **** STAGE 4 - Reduce: (word, freq) sorted (desc.) by frequency
		//VertexStage merge2 = new VertexStage();
		//merge2.setNumVertices(1);
		//merge2.setVertex(StreamMerge.class);
		//vertexStages.add(merge2);
		
		
	}
}
