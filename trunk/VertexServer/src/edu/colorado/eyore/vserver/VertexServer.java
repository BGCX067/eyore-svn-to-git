package edu.colorado.eyore.vserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.net.RequestResponseUtil;
import edu.colorado.eyore.common.vertex.VertexDescriptor;
import edu.colorado.eyore.common.vertex.VertexHeartbeat;
import edu.colorado.eyore.common.vertex.VertexHeartbeatResponse;

/**
 * VertexServer class handles a thread pool of available Vertex slots. Vertex
 * job execution requests are passed to the VertexServer from the JobServer.
 */
public class VertexServer {
	// VertexServer logger.
	Logger logger = Logger.getLogger(VertexServer.class.getName());
	
	private UUID uid = UUID.randomUUID();
	
	// JobServer address and port.
	protected String jobServerAddr;
	protected int jobServerPort;
	
	// ExecutorService for VertexThread(s).
	protected ExecutorService execSrv;
	protected int numThreads;

	protected HdfsUtils hdfs = null;
	
	protected VertexManager vManager;
	
	/**
	 * VertexServer constructor initializes the JobServer address and port.
	 * @param jobServerAddr Address of the JobServer
	 * @param jobServerPort Port of the JobServer
	 */
	public VertexServer(Properties props) {

		this.jobServerAddr = props.getProperty("jobserver.address");
		this.jobServerPort = Integer.parseInt(props.getProperty("jobserver.port"));
		this.numThreads = Integer.parseInt(props.getProperty("threads"));
		this.execSrv = Executors.newFixedThreadPool(numThreads);
		
		try{
			hdfs = new HdfsUtils(props.getProperty("namenode.address"), 
				Integer.parseInt(props.getProperty("namenode.port")));
		}catch(IOException e){
			logger.log(Level.SEVERE, "Failed connecting to HDFS", e);
			throw new RuntimeException(e);
		}
		 vManager = new VertexManager(numThreads);
	}
	
	/**
	 * Start method to start execution of the VertexServer.
	 */
	public void start() {
		Timer t = new Timer("vertex periodic heartbeat");
		TimerTask task = new TimerTask() { // Heartbeat timer.
			@Override
			public void run() {
				try {
					// Initialize our VertexHeartbeat
					VertexHeartbeat vhb = new VertexHeartbeat();
					vhb.setUid(uid.toString());
					vhb.setNumThreads(vManager.getAvailableThreads());
					vhb.setVertexDescriptor(vManager.getCompVertex());
					
					logger.info("sending heartbeat to " 
							+ VertexServer.this.jobServerAddr + " on port "
							+ VertexServer.this.jobServerPort + " compVert:" +
							vhb.getVertexDescriptor().size());
					
					VertexHeartbeatResponse vhbr =
						(VertexHeartbeatResponse)RequestResponseUtil.
							clientRequest(new VertexHeartbeatProtocol(vhb),
									jobServerAddr, jobServerPort);
					
					// Get our VertexContexts.
					List<VertexDescriptor> vertexDescriptor = vhbr.getVertexDescriptor();
					if ( vertexDescriptor != null ) {
						for ( int i = 0; i < vertexDescriptor.size(); i++ ) {
							// We have a valid VertexContext, submit this to our executor service.
							vManager.decrementAvailableThreads();
							execSrv.submit(new VertexThread(vManager, vertexDescriptor.get(i),
								hdfs));
						}
					}
				} catch(Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
		
		// Schedule our next heartbeat message.
		t.scheduleAtFixedRate(task, Calendar.getInstance().getTime(), 5000);
	}
	
	
	/**
	 * @param args [0] is the file path to the properties file for server configuration. 
	 * [1] is the path to the logger configuration file.
	 * - if neither are specified, uses the defaults in the "resource" directory
	 */
	public static void main(String[] args) throws IOException {
		// Get path to properties file - default to default properties file in current directory 
		// if not specified.
		File propFile;
		if ((0 == args.length) || (null == args[0]) || args[0].isEmpty()) {
			// Not specified, so load the default from the class path.
			URL file = 
				VertexServer.class.getClassLoader()
				.getResource("edu/colorado/eyore/vserver/resource/vserver.properties");
			propFile = new File(file.getFile());
		} else {
			propFile = new File(args[0]);
		}
		if (!propFile.exists()) {
			throw new IOException("Properties file not found");
		}
		
		// Load the server properties from the properties file.
		Properties props = new Properties();
		props.load(new FileInputStream(propFile));
		
		// Setup logging.
		File logConfigFile;
		if ((args.length < 2) || (null == args[1]) || args[1].isEmpty()) {
			URL file = VertexServer.class.getClassLoader().getResource(
					"edu/colorado/eyore/vserver/resource/vserver.log.properties");
			logConfigFile = new File(file.getFile());
		} else {
			logConfigFile = new File(args[1]);
		}
		LogManager.getLogManager().readConfiguration(new FileInputStream(logConfigFile));	
		
		// Finally launch the VertexServer.
		VertexServer vs = new VertexServer(props);
		vs.start();
	}
}
