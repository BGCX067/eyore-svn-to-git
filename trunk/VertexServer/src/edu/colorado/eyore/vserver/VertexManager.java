package edu.colorado.eyore.vserver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import edu.colorado.eyore.common.vertex.VertexDescriptor;

public class VertexManager {
	
	int availThreads;
	int threadsOccupied;
	
	public VertexManager(int availableThreads){
		this.availThreads = availableThreads;
		threadsOccupied = 0;
	}
	
	synchronized public int getAvailableThreads(){
		int ret = availThreads - threadsOccupied;
		if(ret < 0){
			return 0;
		}return ret;
	}
	synchronized public void decrementAvailableThreads(){
		threadsOccupied ++;
	}
	synchronized private void freeThread(){
		threadsOccupied--;
	}
	
	protected ArrayDeque<VertexDescriptor> compVertices = new ArrayDeque<VertexDescriptor>();
	
	/**
	 * Call to report vertex as completed successfully
	 * @param vDescriptor
	 */
	public synchronized void finishSuccessVertex(VertexDescriptor vDescriptor) {
		vDescriptor.setExecutionFinished(true);
		vDescriptor.setExecutionSuccessful(true);
		compVertices.add(vDescriptor);
		freeThread();
	}
	
	/**
	 * Call to report vertex as failed
	 * @param vDescriptor
	 */
	public synchronized void finishFailedVertex(VertexDescriptor vDescriptor){
		vDescriptor.setExecutionFinished(true);
		vDescriptor.setExecutionSuccessful(false);
		compVertices.add(vDescriptor);
		freeThread();
	}
	
	public synchronized List<VertexDescriptor> getCompVertex() {
		ArrayList<VertexDescriptor> compVertices = new ArrayList<VertexDescriptor>();
		
		// For now just assign the next available vertices up to available/3
		// - may need to tune later
		for(int i = 0; i < this.compVertices.size(); i++) {
			VertexDescriptor vDescriptor = this.compVertices.remove();
			compVertices.add(vDescriptor);
		}
		
		return compVertices;
	}
}
