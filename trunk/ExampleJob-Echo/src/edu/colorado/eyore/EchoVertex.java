package edu.colorado.eyore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.colorado.eyore.common.hdfs.HdfsUtils.OutputInfo;
import edu.colorado.eyore.common.io.EBufferedWriter;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexOutput;

public class EchoVertex extends Vertex {

	@Override
	public void run(VertexContext context)throws IOException {
		VertexOutput vOutput = new VertexOutput();
		
		// Print hello eyore, because we can!
		System.out.println("Hello Eyore!");

		OutputInfo outInfo = context.getHdfs().getHdfsFileOutputStream(context.getJobId(),
				context.getVertexIndex(), context.getStageIndex(),
				this.getClass());
		
		OutputStream os = outInfo.getOutputStream();
		
		// An Eyore BufferedWriter - so we can change buffer sizes in one place if
		// it ends up mattering.  See also EBufferedReader.
		EBufferedWriter writer = null;
		try{
			// Using a BufferedWriter because it's probably
			// the best choice for performance
			writer = new EBufferedWriter(new OutputStreamWriter(os));
			writer.write("eyore writes one line\n");
		}finally{
			
			// Make sure to call close so that any buffered output gets written
			// otherwise output may go missing
			writer.close();
		}
		
		// Setup our output map.
		List<String> output = new ArrayList<String>();
		output.add(outInfo.getFilePath());
		Map<Integer, List<String>> outputMap = new HashMap<Integer, List<String>>();
		outputMap.put(null, output);
		vOutput.setOutputMap(outputMap);
		context.setvOutput(vOutput);
	}

}
