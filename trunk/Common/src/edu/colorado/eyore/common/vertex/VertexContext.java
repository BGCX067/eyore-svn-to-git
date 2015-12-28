package edu.colorado.eyore.common.vertex;

import java.io.InputStream;
import java.util.List;

import edu.colorado.eyore.common.hdfs.HdfsUtils;


/**
 * This class is populated by the VertexServer with 
 * any resources needed by a Vertex to run it's computation
 * (probably input file streams, HDFS path to write output) &
 * is updated by the Vertex run method with any data that the
 * Vertex needs to ultimately communicate back to the JobServer.  
 * This is likely to be info like where the Vertex actually
 * wrote each output file & the vertex in the next stage who
 * is the recipient of this output file)
 *
 */
public class VertexContext {

	private List<InputStream> inputs;
	private VertexOutput vOutput;
	private Integer numVerticesNextStage;
	private HdfsUtils hdfs;
	private int vertexIndex;
	private String jobId;
	private int stageIndex;
	
	/**
	 * These will most likely be FsInputStream 
	 * (Hadoop HDFS supported input stream implementation),
	 *  but using InputStream allows any sub-type
	 *  to be used at run-time
	 *  
	 *  The Vertex can wrap a BufferredReader
	 *  around each InputStream to read the input
	 *  line by line.
	 */
	public List<InputStream> getInputs(){
		return inputs;
	}
	public void setInputs(List<InputStream> inputs){
		this.inputs = inputs;
	}
	public void setvOutput(VertexOutput vOutput) {
		this.vOutput = vOutput;
	}
	public VertexOutput getvOutput() {
		return vOutput;
	}
	
	/**
	 * Number of vertices in the next stage - needed
	 * for output partitioning to N vertices.  Should
	 * be NULL if this vertex is in the last stage. 
	 */
	public Integer getNumVerticesNextStage(){
		return numVerticesNextStage;
	}
	public void setNumVerticesNextStage(Integer vertices){
		this.numVerticesNextStage = vertices;
	}
	
	/**
	 * Vertex can use this to get an HDFS output stream 
	 */
	public HdfsUtils getHdfs(){
		return this.hdfs;
	}
	public void setHdfs(HdfsUtils hdfs){
		this.hdfs = hdfs;
	}
	
	/**
	 * (zero based) Index of vertex within the stage - here to 
	 * allow vertex to print some debugging info 
	 */
	public int getVertexIndex(){
		return vertexIndex;
	}
	public void setVertexIndex(int vertexIndex){
		this.vertexIndex = vertexIndex;
	}
	
	/**
	 * (zero based) stage index of vertex - here to allow
	 * vertex to print debugging info  
	 */
	public int getStageIndex(){
		return stageIndex;
	}
	public void setStageIndex(int stageIndex){
		this.stageIndex = stageIndex;
	}
	
	/**
	 * Job id - here to allow vertex to be able to print debugging info if
	 * needed 
	 */
	public String getJobId(){
		return this.jobId;
	}
	public void setJobId(String jobId){
		this.jobId = jobId;
	}
	
	
}
