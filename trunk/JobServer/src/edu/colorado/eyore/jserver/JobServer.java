package edu.colorado.eyore.jserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.apache.hadoop.hdfs.HftpFileSystem;

import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.net.RequestResponseUtil;
import edu.colorado.eyore.common.vertex.VertexDescriptor;
import edu.colorado.eyore.common.vertex.VertexHeartbeat;
import edu.colorado.eyore.common.vertex.VertexServerInfo;

/**
 * Multi-threaded server that accepts & responds to client and "vertex server" requests 
 */
public class JobServer {
	
	protected static Logger logger = Logger.getLogger(JobServer.class.getName());
	
	// port on which server will listen
	protected int listenPort;
	
	// Size of thread pool
	protected int numThreads;
	
	protected HdfsUtils hdfs;
	
	// a thread pool
	protected ExecutorService execSrv;
	
	/**
	 * Manages allocation, status of jobs
	 */
	protected JobManager jobManager;
	
	// used to track which was the last id
	// to be provided so that the next provided
	// id will be unique
	private int lastJobId = 0;
	
	public JobServer(Properties props){
		this.listenPort = Integer.parseInt(props.getProperty("listen.port"));
		this.numThreads = Integer.parseInt(props.getProperty("threads"));
		
		try{
			this.hdfs = new HdfsUtils(props.getProperty("namenode.address"), 
				Integer.parseInt(props.getProperty("namenode.port")));
		}catch(IOException e){
			logger.log(Level.SEVERE, "Failed connecting to HDFS", e);
			throw new RuntimeException(e);
		}
		
		int splitsPerHdfsBlock = Integer.parseInt(props.getProperty("file.splits.per.hdfs.block"));
		
		jobManager = new JobManager(Integer.parseInt(props.getProperty("max.simultaneous.jobs")), hdfs, splitsPerHdfsBlock);
	}
	
	public void start() throws IOException{
		ServerSocket serverSocket = new ServerSocket(listenPort);
		logger.info("Server listening on port " + listenPort);
		
		execSrv = Executors.newFixedThreadPool(numThreads);
		logger.info("Created thread pool of size " + numThreads);
		
		// the main server loop
		while(true){
			// each connection is handled in a new thread object
			// by a thread in the thread pool
			execSrv.submit(new ServerThread(serverSocket.accept()));			
		}
	}
	
	/**
	 * This is an inner class for a server thread that handles
	 * requests
	 */
	protected class ServerThread implements Runnable{
		protected Socket clientSock;
		public ServerThread(Socket clientSock){
			this.clientSock = clientSock;
		}

		@Override
		public void run() {
			try{

				BufferedReader in = new BufferedReader(
						new InputStreamReader(clientSock.getInputStream()));
				
				String request = in.readLine();

				if(request.startsWith("CLIENT REQUEST JOB:")){
					Object result = RequestResponseUtil.serverResponse(
							new ClientRequestProtocol(JobServer.this, hdfs),
							clientSock, request);
					logger.info("Received job ID request from client");
					
				} else if(request.startsWith("CLIENT REQUEST JOB START")){
					logger.info("Received job start request from client");
					Object result = RequestResponseUtil.serverResponse(
							new ClientRequestProtocol(JobServer.this, hdfs),
							clientSock, request);
					logger.info("Processed job start request from client");
					if(result != null && result instanceof JobDescriptor){
						JobDescriptor jDesc = (JobDescriptor) result;
						logger.info("Starting job with ID " + jDesc.getJobId() +
								" Jar " + jDesc.getHdfsJarPath());
						jobManager.addNewJobFromClient(jDesc);
					}else{
						logger.severe("Rcv unknown client job start request");
					}					
				} else if(request.startsWith("CLIENT REQUEST STATUS")){
					Object result = RequestResponseUtil.serverResponse(
							new ClientRequestProtocol(JobServer.this, hdfs),
							clientSock, request);
					if(result != null && result instanceof JobDescriptor){
						JobDescriptor jDesc = (JobDescriptor) result;						
						logger.info("Responded with status for job ID "
								+ jDesc.getJobId());
					}else{
						logger.severe("Rcv unknown job status request");
					}
				} else if (request.startsWith("JSERVER HEARTBEAT:")) {
					Object result = RequestResponseUtil.serverResponse(
							new VertexHeartbeatProtocol(jobManager),
								clientSock, request);
					if (result != null && result instanceof VertexHeartbeat) {
						VertexHeartbeat vHeartbeat = (VertexHeartbeat)result;
						VertexServerInfo vServer = new VertexServerInfo();
						vServer.setId(vHeartbeat.getUid());
						
						// Get the status of any completed Vertices we have.
						List <VertexDescriptor> compVertices = vHeartbeat.getVertexDescriptor();
						if (compVertices != null) {
							logger.info("Received completion for: " +
									compVertices.size() + " jobs" );
							for ( int i = 0; i < compVertices.size(); i++) {
								jobManager.updateVertexStatus(compVertices.get(i));
							}
						}
											
						logger.info("Recieved VertexHeartbeat from: " +
								vHeartbeat.getUid() + " with threads:" +
								vHeartbeat.getNumThreads());
					}
					
				} else {
					logger.info("Received unknown message: " + request);
				}
								
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		
	}
	
	/**
	 * Get a new unique job id
	 */
	synchronized int getUniqueJobId(){
		lastJobId++;
		return lastJobId;
	}
	
	/**
	 * @param args [0] is the file path to the properties file for server config 
	 * [1] is the path to the logger configuration file
	 * - if neither are specified, uses the defaults in the "resource" directory
	 */
	public static void main(String[] args) throws IOException{
		// get path to properties file - default to default properties file in current dir 
		// if not specified
		
		File propFile;
		if(args.length == 0 || args[0] == null || args[0].isEmpty()){
			// not specified, so load the default from the class path
			URL file = 
				JobServer.class.getClassLoader()
				.getResource("edu/colorado/eyore/jserver/resource/jserver.properties");
			propFile = new File(file.getFile());
		}else{
			propFile = new File(args[0]);
		}
		if(! propFile.exists()){
			throw new IOException("Properties file not found");
		}			
		
		// load the server properties from the properties file
		Properties props = new Properties();
		props.load(new FileInputStream(propFile));
		
		// setup logging
		File logConfigFile;
		if(args.length < 2 || args[1] == null || args[1].isEmpty()){
			URL file = JobServer.class.getClassLoader().getResource("edu/colorado/eyore/jserver/resource/jserver.log.properties");
			logConfigFile = new File(file.getFile());
		}else{
			logConfigFile = new File(args[1]);
		}
		LogManager.getLogManager().readConfiguration(new FileInputStream(logConfigFile));	
		
		// finally launch the server
		JobServer js = new JobServer(props);
		
		
		js.start();
	}
}
