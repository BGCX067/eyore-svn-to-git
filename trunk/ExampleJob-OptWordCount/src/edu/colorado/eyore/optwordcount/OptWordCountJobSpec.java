package edu.colorado.eyore.optwordcount;

import java.util.ArrayList;

import edu.colorado.eyore.common.example.vertex.MapWordFreq;
import edu.colorado.eyore.common.example.vertex.StreamMerge;
import edu.colorado.eyore.common.example.vertex.SumAndSortByFreq;
import edu.colorado.eyore.common.job.JobSpecification;
import edu.colorado.eyore.common.vertex.VertexStage;

public class OptWordCountJobSpec extends JobSpecification {
	public OptWordCountJobSpec(){
		super.vertexStages = new ArrayList<VertexStage>();
				
		
		// **** STAGE 1 - count freq per chunk
		VertexStage countPerChunk = new VertexStage();
		countPerChunk.setVertex(MapWordFreq.class);
		vertexStages.add(countPerChunk);
		
		// **** STAGE 2 - Find word freq's and sort
		VertexStage totalCountAndSort = new VertexStage();
		totalCountAndSort.setNumVertices(19);
		totalCountAndSort.setVertex(SumAndSortByFreq.class);
		vertexStages.add(totalCountAndSort);
		
		
		
		// **** STAGE 3 - merge output into one (desc) sorted file
		VertexStage merge = new VertexStage();
		merge.setNumVertices(8);
		merge.setVertex(StreamMerge.class);
		vertexStages.add(merge);

		

		
		VertexStage merge2 = new VertexStage();
		merge2.setNumVertices(1);
		merge2.setVertex(StreamMerge.class);
		vertexStages.add(merge2);
		
		
	}
}
