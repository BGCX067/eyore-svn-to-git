package edu.colorado.eyore.common.vertex;

/**
 * Describes a stage of a Dryad-like computation - which Vertex
 * instance will be running each vertex in the stage & how many
 * vertices will run in the stage.
 */
public class VertexStage {

	
	private Class vertex;
	private int numVertices;
	
	/**
	 * The class of the vertex to run for each vertex in the stage -
	 * unlike Dryad, each vertex in the same "stage" will be identical
	 * (the same Class) to keep things simple for now
	 */
	public <V extends Vertex> Class<V> getVertex(){
		return vertex;
	}
	public <V extends Vertex> void setVertex(Class<V> vertex){
		this.vertex = vertex;
	}
	
	/**
	 * How many vertices in the stage need to be executed
	 */
	public int getNumVertices(){
		return numVertices;
	}
	public void setNumVertices(int numVertices){
		this.numVertices = numVertices;
	}
}
