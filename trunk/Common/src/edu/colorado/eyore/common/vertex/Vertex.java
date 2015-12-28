package edu.colorado.eyore.common.vertex;

import java.io.IOException;


/**
 * Vertices are executed on VertexServers and scheduled by the JobServer.
 * Vertex execution is completely controlled by the client and the run method
 * must be overridden.
 */
public abstract class Vertex {
	
	/**
	 * Vertex execution method. Client must override this method.
	 * @param context Vertex context, containing input and output paths.
	 */
	abstract public void run(VertexContext context) throws IOException;
}
