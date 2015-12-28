package edu.colorado.eyore.vserver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.colorado.eyore.common.JarClassLoadUtil;
import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexDescriptor;

/**
 * VertexThread class runs executable client Vertices as requested by the JobServer.
 */
public class VertexThread implements Runnable {
	private Vertex vertex;
	private VertexDescriptor vDescriptor;
	private VertexManager vManager;
	private Logger logger = Logger.getLogger(VertexThread.class.getName());
	private List <InputStream> inputStream;
	private HdfsUtils hdfs;
	
	/**
	 * Constructor, passes the VertexContext for Vertex to be executed.
	 * @param hdfs 
	 * @param vContext
	 * @throws IOException 
	 */
	public VertexThread(VertexManager vManager, VertexDescriptor vDescriptor, HdfsUtils hdfs)
		throws IOException {
		
		this.vManager = vManager;
		this.vDescriptor = vDescriptor;
		this.hdfs = hdfs;
		File localJarCopy = File.createTempFile("vServer-tempLocalJar-", "");

		hdfs.writeHdfsFileToLocal(vDescriptor.getVertexJarPath(), localJarCopy);
		
		if( !localJarCopy.exists()){
			throw new IOException("failed making local copy of jar file ");
		}
		
		Class<Vertex> cls = JarClassLoadUtil.loadClassFromJar(vDescriptor.getVertexClassName(),
				localJarCopy);
		this.vertex = JarClassLoadUtil.getInstanceFromClass(cls);
		
		logger.info("Loaded vertex: " + vDescriptor.getVertexClassName());
		
		// Open our list of input streams and add them to our list.
		inputStream = new ArrayList<InputStream>();
		List <String> inputPaths = vDescriptor.getInputPaths();		
		for (int index = 0; index < inputPaths.size(); index++) {
			String hdfsInputFilePath = inputPaths.get(index);
			
			// Check for file exists on HDFS and get an input stream
			// if so
			//if(hdfs.hdfsFileExists(hdfsInputFilePath)){
				inputStream.add(hdfs.getHdfsFileInputStream(hdfsInputFilePath));
			//}else{
				// Want to fail vertex because any input paths
				// must exist on HDFS for correct job execution - they
				// where either intended for execution as initial job input
				// or a previous vertex stage wrote them
				//logger.severe("Input file (" + hdfsInputFilePath + 
					//	") not found" );
				//vManager.finishFailedVertex(vDescriptor);
				//return;
			//}
		}
	}

	/**
	 * Run method for executing the Vertex.
	 */
	@Override
	public void run() {
		// Setup our VertexContext
		VertexContext vContext = new VertexContext();
		vContext.setInputs(inputStream);
		vContext.setNumVerticesNextStage(vDescriptor.getNumVerticesNextStage());
		vContext.setJobId(vDescriptor.getJobId());
		vContext.setHdfs(hdfs);
		vContext.setVertexIndex(vDescriptor.getVertexNumber());
		vContext.setStageIndex(vDescriptor.getStageNumber());
		
		// Run the Vertex
		try{
			vertex.run(vContext);
		}catch(Exception e){
			logger.log(Level.SEVERE, "Vertex execution failed.  Descriptor: "+
					vDescriptor, e);
			vManager.finishFailedVertex(vDescriptor);
			return;
		}
		
		// Set the Vertex descriptor output map.
		this.vDescriptor.setOutput(vContext.getvOutput());
		this.vManager.finishSuccessVertex(vDescriptor);
		
		// Close our input stream(s).
		for (int index = 0; index < this.inputStream.size(); index++) {
			InputStream is = this.inputStream.get(index);
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
