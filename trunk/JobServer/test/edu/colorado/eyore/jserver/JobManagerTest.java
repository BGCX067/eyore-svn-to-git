package edu.colorado.eyore.jserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.job.JobSpecification;
import edu.colorado.eyore.common.job.JobStatus;
import edu.colorado.eyore.common.vertex.Vertex;
import edu.colorado.eyore.common.vertex.VertexContext;
import edu.colorado.eyore.common.vertex.VertexDescriptor;
import edu.colorado.eyore.common.vertex.VertexOutput;
import edu.colorado.eyore.common.vertex.VertexServerInfo;
import edu.colorado.eyore.common.vertex.VertexStage;

/**
 * Unit testing for the JobManager 
 *
 */
public class JobManagerTest {
	
	private HdfsUtils hdfs;
	private JobManager jobMgr;
	
	@Before
	public void setup(){
		// make a dummy instance for testing
		hdfs = EasyMock.createMock(HdfsUtils.class);
		
		jobMgr = new JobManager(2, hdfs, 1);
	}

	@Test
	public void testClientSubmitJobOneStage()throws Exception{
		JobDescriptor jDesc = getJobDescriptor("1");
		JobSpecification jSpec = jDesc.getJobSpecification();
		VertexStage stage1 = new VertexStage();
		stage1.setNumVertices(3);
		stage1.setVertex(DummyVertex1.class);
		jSpec.getVertexStages().add(stage1);
		
		ArrayList<String> inFiles = new ArrayList();
		inFiles.add("/input/path/file1");		
		EasyMock.expect(hdfs.getFilePathsFromHdfsDir(
				"/input/path",1)).andReturn(inFiles);
		
		EasyMock.replay(hdfs);
		
		jobMgr.addNewJobFromClient(jDesc);
		
		EasyMock.verify(hdfs);
		
		// check job queues
		Assert.assertTrue(jobMgr.jobsInProgress.containsKey("1"));
		Assert.assertEquals(0, jobMgr.unprocessedJobQ.size());
		
		// check job status object
		JobStatus jStatus = jobMgr.jobsInProgress.get("1").getJobStatus();		
		Assert.assertNotNull(jStatus);
		Assert.assertEquals(0, jStatus.getCurrentVertexStage());
		Assert.assertEquals(1, jStatus.getTotalVertexStages());
		Assert.assertFalse(jStatus.getExecutionFinished());
		Assert.assertEquals(3, jStatus.getTotalVerticesCurrentStage());
		Assert.assertEquals(3, jStatus.getTotalVerticesInProgressCurrentStage());
		
		// check vertex meta-data
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setVertexNumber(0);
		v1.setStageNumber(0);
		v1.setJobId("1");
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setVertexNumber(1);
		v2.setStageNumber(0);
		v2.setJobId("1");
		VertexDescriptor v3 = new VertexDescriptor();
		v3.setVertexNumber(2);
		v3.setStageNumber(0);
		v3.setJobId("1");
		
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v1));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v2));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v3));	
		
		for(VertexDescriptor v : jobMgr.allocatableVertices){
			Assert.assertEquals("vertex in last stage - next stage vert count should be NULL",
					null, v.getNumVerticesNextStage());
		}

		Assert.assertEquals(0, jobMgr.verticesInProgress.size());		
		
	}
	
	@Test
	public void testClientSubmitJobTwoStages()throws Exception{
		JobDescriptor jDesc = getJobDescriptor("1");
		JobSpecification jSpec = jDesc.getJobSpecification();
		VertexStage stage1 = new VertexStage();
		stage1.setNumVertices(3);
		stage1.setVertex(DummyVertex1.class);
		VertexStage stage2 = new VertexStage();
		stage2.setNumVertices(5);
		stage2.setVertex(DummyVertex2.class);
		
		jSpec.getVertexStages().add(stage1);
		jSpec.getVertexStages().add(stage2);
		
		ArrayList<String> inFiles = new ArrayList();
		inFiles.add("/input/path/file1");		
		EasyMock.expect(hdfs.getFilePathsFromHdfsDir(
				"/input/path",1)).andReturn(inFiles);
		
		EasyMock.replay(hdfs);
		
		jobMgr.addNewJobFromClient(jDesc);
		
		EasyMock.verify(hdfs);
		
		// check job queues
		Assert.assertTrue(jobMgr.jobsInProgress.containsKey("1"));
		Assert.assertEquals(0, jobMgr.unprocessedJobQ.size());
		
		// check job status object
		JobStatus jStatus = jobMgr.jobsInProgress.get("1").getJobStatus();		
		Assert.assertNotNull(jStatus);
		Assert.assertEquals(0, jStatus.getCurrentVertexStage());
		Assert.assertEquals(2, jStatus.getTotalVertexStages());
		Assert.assertFalse(jStatus.getExecutionFinished());
		Assert.assertEquals(3, jStatus.getTotalVerticesCurrentStage());
		Assert.assertEquals(3, jStatus.getTotalVerticesInProgressCurrentStage());
		
		// check vertex meta-data
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setVertexNumber(0);
		v1.setStageNumber(0);
		v1.setJobId("1");
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setVertexNumber(1);
		v2.setStageNumber(0);
		v2.setJobId("1");
		VertexDescriptor v3 = new VertexDescriptor();
		v3.setVertexNumber(2);
		v3.setStageNumber(0);
		v3.setJobId("1");
		
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v1));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v2));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v3));	
		
		Assert.assertEquals(0, jobMgr.verticesInProgress.size());		
		
		for(VertexDescriptor v : jobMgr.allocatableVertices){
			Assert.assertEquals("Check next stage vertex count set correctly in VertexDescriptor", 
					new Integer(5), v.getNumVerticesNextStage());
		}
	}
	

	/**
	 * Job submitted when another job is already running &
	 * there is capacity to run this new job simultaneously
	 * @throws Exception
	 */
	@Test
	public void testClientSubmitJobRunsSimultaneously()throws Exception{
		jobMgr.numSimultenousJobs = 2;
		
		// setup first job
		JobDescriptor jDesc = getJobDescriptor("1");
		JobSpecification jSpec = jDesc.getJobSpecification();						
		VertexStage stage1 = new VertexStage();
		stage1.setNumVertices(3);
		stage1.setVertex(DummyVertex1.class);
		jSpec.getVertexStages().add(stage1);
		
		// setup second job
		JobDescriptor jDesc2 = getJobDescriptor("2");
		JobSpecification jSpec2 = jDesc2.getJobSpecification();						
		VertexStage stage2 = new VertexStage();
		stage2.setNumVertices(2);
		stage2.setVertex(DummyVertex2.class);
		jSpec2.getVertexStages().add(stage2);
		
		
		ArrayList<String> inFiles = new ArrayList();
		inFiles.add("/input/path/file1");		
		EasyMock.expect(hdfs.getFilePathsFromHdfsDir(
				"/input/path",1)).andReturn(inFiles).times(2);
		
		EasyMock.replay(hdfs);
		
		jobMgr.addNewJobFromClient(jDesc);
		jobMgr.addNewJobFromClient(jDesc2);
		
		EasyMock.verify(hdfs);
		
		// check job queues
		Assert.assertTrue(jobMgr.jobsInProgress.containsKey("1"));
		Assert.assertTrue(jobMgr.jobsInProgress.containsKey("2"));
		Assert.assertEquals(0, jobMgr.unprocessedJobQ.size());
		
		// check job status object
		JobStatus jStatus = jobMgr.jobsInProgress.get("1").getJobStatus();		
		Assert.assertNotNull(jStatus);
		Assert.assertEquals(0, jStatus.getCurrentVertexStage());
		Assert.assertEquals(1, jStatus.getTotalVertexStages());
		Assert.assertFalse(jStatus.getExecutionFinished());
		Assert.assertEquals(3, jStatus.getTotalVerticesCurrentStage());
		Assert.assertEquals(3, jStatus.getTotalVerticesInProgressCurrentStage());

		JobStatus jStatus2 = jobMgr.jobsInProgress.get("2").getJobStatus();		
		Assert.assertNotNull(jStatus2);
		Assert.assertEquals(0, jStatus2.getCurrentVertexStage());
		Assert.assertEquals(1, jStatus2.getTotalVertexStages());
		Assert.assertFalse(jStatus2.getExecutionFinished());
		Assert.assertEquals(2, jStatus2.getTotalVerticesCurrentStage());
		Assert.assertEquals(2, jStatus2.getTotalVerticesInProgressCurrentStage());
		
		
		// check vertex meta-data
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setVertexNumber(0);
		v1.setStageNumber(0);
		v1.setJobId("1");
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setVertexNumber(1);
		v2.setStageNumber(0);
		v2.setJobId("1");
		VertexDescriptor v3 = new VertexDescriptor();
		v3.setVertexNumber(2);
		v3.setStageNumber(0);
		v3.setJobId("1");

		VertexDescriptor v1_2 = new VertexDescriptor();
		v1_2.setVertexNumber(0);
		v1_2.setStageNumber(0);
		v1_2.setJobId("2");
		VertexDescriptor v2_2 = new VertexDescriptor();
		v2_2.setVertexNumber(1);
		v2_2.setStageNumber(0);
		v2_2.setJobId("2");
		
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v1));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v2));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v3));		
		
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v1_2));
		Assert.assertTrue(jobMgr.allocatableVertices.contains(v2_2));

		Assert.assertEquals(0, jobMgr.verticesInProgress.size());		
		
	}
	
	
	@Test
	public void testAssignVerticesToServer_NoAssignableVertices(){
		VertexServerInfo vServer = new VertexServerInfo();
		vServer.setAvailableThreads(100);
		vServer.setId("123");
		
		List<VertexDescriptor> assigned = jobMgr.assignVerticesToServer(vServer);
		
		Assert.assertEquals(0, assigned.size());
	}
	
	/**
	 * Test odd case where vertex server has 0 available threads
	 */
	@Test
	public void testAssignVerticesToServer_VServerHasNoCapacity(){
		// Make some assignable vertices
		for(int i = 0; i < 10; i++){
			VertexDescriptor v = new VertexDescriptor();
			v.setJobId(i+"");
			v.setVertexNumber(i);
			v.setStageNumber(i);
			jobMgr.allocatableVertices.add(v);
		}

		VertexServerInfo vServer = new VertexServerInfo();
		vServer.setAvailableThreads(0);
		vServer.setId("123");
		
		List<VertexDescriptor> assigned = jobMgr.assignVerticesToServer(vServer);
		
		Assert.assertEquals(0, assigned.size());
		
		Assert.assertEquals(10, jobMgr.allocatableVertices.size());
		Assert.assertEquals(0, jobMgr.verticesInProgress.size());
	}

	@Test
	public void testAssignVerticesToServer(){
		// Make some assignable vertices
		for(int i = 0; i < 10; i++){
			VertexDescriptor v = new VertexDescriptor();
			v.setJobId(i+"");
			v.setVertexNumber(i);
			v.setStageNumber(i);
			jobMgr.allocatableVertices.add(v);
		}
		
		VertexServerInfo vServer = new VertexServerInfo();
		vServer.setAvailableThreads(9);
		vServer.setId("123");
		
		List<VertexDescriptor> assigned = jobMgr.assignVerticesToServer(vServer);
		
		// current algorithm gives a requesting vertex at most 1/3 of its capacity
		Assert.assertEquals(3, assigned.size());
		
		Assert.assertEquals(7, jobMgr.allocatableVertices.size());
		Assert.assertEquals(3, jobMgr.verticesInProgress.size());
		
		for (VertexDescriptor v : assigned){
			Assert.assertTrue(jobMgr.verticesInProgress.contains(v));
			Assert.assertEquals(vServer.getId(), v.getVertexServerAssignment());
		}
	}
	
	@Test
	public void testJobStatusQuery(){
		// setup a dummy job
		JobDescriptor jDesc = getJobDescriptor("1");
		JobSpecification jSpec = jDesc.getJobSpecification();						
		VertexStage stage1 = new VertexStage();
		stage1.setNumVertices(3);
		stage1.setVertex(DummyVertex1.class);
		jSpec.getVertexStages().add(stage1);
		JobStatus jobStatus = new JobStatus();
		jobStatus.setCurrentVertexStage(4);
		jobStatus.setTotalVertexStages(5);
		jobStatus.setExecutionFinished(false);
		jobStatus.setTotalVerticesCurrentStage(10);
		jobStatus.setTotalVerticesInProgressCurrentStage(5);
		jDesc.setJobStatus(jobStatus);
		
		jobMgr.jobsInProgress.put("1", jDesc);
		
		JobStatus status = jobMgr.jobStatusQuery("1");
		
		// make sure the status matches what was associated with the
		// job in the job manager
		Assert.assertNotNull(status);
		Assert.assertEquals(5, status.getTotalVertexStages());
		Assert.assertEquals(4, status.getCurrentVertexStage());
		Assert.assertEquals(10, status.getTotalVerticesCurrentStage());
		Assert.assertEquals(5, status.getTotalVerticesInProgressCurrentStage());
		Assert.assertFalse(status.getExecutionFinished());
	}
	
	/**
	 * This is the case where the job isn't being executed yet
	 */
	@Test
	public void testJobStatusQuery_StatusNotAvailable(){
		
		JobStatus status = jobMgr.jobStatusQuery("1");
		
		// make sure the status matches what was associated with the
		// job in the job manager
		Assert.assertNull(status);
	}
	
	/**
	 * When a job's status is queried, if the job is finished,
	 * it should be removed from the JobManager
	 */
	@Test
	public void testJobStatusQuery_jobFinished(){
		// setup a dummy job
		JobDescriptor jDesc = getJobDescriptor("1");
		JobSpecification jSpec = jDesc.getJobSpecification();						
		VertexStage stage1 = new VertexStage();
		stage1.setNumVertices(3);
		stage1.setVertex(DummyVertex1.class);
		jSpec.getVertexStages().add(stage1);
		JobStatus jobStatus = new JobStatus();
		jobStatus.setCurrentVertexStage(5);
		jobStatus.setTotalVertexStages(5);
		jobStatus.setExecutionFinished(true);
		jobStatus.setTotalVerticesCurrentStage(5);
		jobStatus.setTotalVerticesInProgressCurrentStage(5);
		jDesc.setJobStatus(jobStatus);
		
		jobMgr.jobsInProgress.put("1", jDesc);
		
		JobStatus status = jobMgr.jobStatusQuery("1");
		
		// make sure the job was removed from the "jobs in progress" list
		Assert.assertFalse(jobMgr.jobsInProgress.containsKey("1"));
		
		// make sure the status matches what was associated with the
		// job in the job manager
		Assert.assertNotNull(status);
		Assert.assertEquals(5, status.getTotalVertexStages());
		Assert.assertEquals(5, status.getCurrentVertexStage());	
		Assert.assertEquals(5, status.getTotalVerticesCurrentStage());
		Assert.assertEquals(5, status.getTotalVerticesInProgressCurrentStage());
		Assert.assertTrue(status.getExecutionFinished());
	}
	
	/**
	 * Vertex finishes successfully, but the current stage is not finished yet and
	 * there is a next vertex stage, which means the output from this vertex needs to
	 * be mapped to the appropriate next stage vertices (the focus of this test).
	 * 
	 * Output should be recored for the next stage vertex.  The output is not yet actually
	 * given to the next stage vertex because the current stage is not yet finished.
	 */
	@Test
	public void testUpdateVertexStatus_vertexSuccessful_currentStageNotFinished_hasNextStage(){
		JobDescriptor jobInProgress = new JobDescriptor();
		jobInProgress.setJobId("1");
		
		JobStatus jobStatus = new JobStatus();
		jobStatus.setCurrentVertexStage(1);
		jobStatus.setTotalVertexStages(3);
		jobStatus.setExecutionFinished(false);
		jobStatus.setTotalVerticesCurrentStage(4);
		jobStatus.setTotalVerticesInProgressCurrentStage(2);
		
		jobInProgress.setJobStatus(jobStatus);		
		jobMgr.jobsInProgress.put("1", jobInProgress);
		
		// this will be a vertex that is still in progress
		// when status is reported for vertex v2
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setJobId("1");
		v1.setVertexNumber(1);
		v1.setStageNumber(1);
		v1.setVertexServerAssignment("vs1");
		v1.setExecutionFinished(false);
		
		// this will be the vertex that's status is reported
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setJobId("1");
		v2.setVertexNumber(2);
		v2.setStageNumber(1);
		v2.setVertexServerAssignment("vs2");
		v2.setExecutionFinished(true);
		v2.setExecutionSuccessful(true);
		
		jobMgr.verticesInProgress.add(v1);
		jobMgr.verticesInProgress.add(v2);

		// Give this vertex some dummy output file paths to report
		// - these need to be appropriately recorded for use with next
		// vertex stage vertices as indicated by map key values
		VertexOutput v2Output = new VertexOutput();
		HashMap<Integer, List<String>> v2OutputMap = new HashMap<Integer, List<String>>();
		ArrayList<String> outputForNextVertex0 = new ArrayList<String>();
		outputForNextVertex0.add("dummyOutputFileForVertex0");
		outputForNextVertex0.add("dummyOutputFile2ForVertex0");
		v2OutputMap.put(0, outputForNextVertex0);

		ArrayList<String> outputForNextVertex1 = new ArrayList<String>();
		outputForNextVertex1.add("dummyOutputFileForVertex1");
		v2OutputMap.put(1, outputForNextVertex1);
		v2Output.setOutputMap(v2OutputMap);
		v2.setOutput(v2Output);
		
		// make sure VertexDescriptor equality is working OK
		Assert.assertTrue(jobMgr.verticesInProgress.contains(v1));
		Assert.assertTrue(jobMgr.verticesInProgress.contains(v2));
		
		// since the job wasnt started in the usual way, set up the output map
		jobMgr.outputMap.put(jobInProgress.getJobId(), new HashMap<Integer, HashMap<Integer,ArrayList<String>>>());
		jobMgr.updateVertexStatus(v2);
		
		Assert.assertFalse("Vertex 'v2' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v2));
		Assert.assertTrue("Vertex 'v1' should still be in progress",
				jobMgr.verticesInProgress.contains(v1));
		 
		JobStatus newJobStatus = jobMgr.jobsInProgress.get("1").getJobStatus();
		Assert.assertEquals("Total vertices in progress should have been decremented", 
				1, newJobStatus.getTotalVerticesInProgressCurrentStage());
		Assert.assertEquals("Total vertices in current stage should not have changed",
				4, newJobStatus.getTotalVerticesCurrentStage());
		Assert.assertEquals("Current vertex stage should not have changed", 1,
				newJobStatus.getCurrentVertexStage());
		Assert.assertFalse("Job is still in progress", newJobStatus.getExecutionFinished());
		
		// Check the output from v2 recorded for the next stage vertices
		HashMap<Integer, ArrayList<String>> nextStageVertexOutputMap = 
			jobMgr.outputMap.get(jobInProgress.getJobId()).get(2);
		Assert.assertNotNull("output map for next stage shouldnt be null", nextStageVertexOutputMap);
		
		Assert.assertEquals("2 vertices in next stage should have output stored", 
				2, nextStageVertexOutputMap.keySet().size());
		
		Assert.assertEquals("Next stage vertex 0 should have 2 output files", 2, nextStageVertexOutputMap.get(0).size());
		Assert.assertTrue(nextStageVertexOutputMap.get(0).contains("dummyOutputFileForVertex0"));
		Assert.assertTrue(nextStageVertexOutputMap.get(0).contains("dummyOutputFile2ForVertex0"));
		
		Assert.assertEquals("Next stage vertex 1 should have 1 output file", 1, nextStageVertexOutputMap.get(1).size());
		Assert.assertTrue(nextStageVertexOutputMap.get(1).contains("dummyOutputFileForVertex1"));

	}
	
	/**
	 * Vertex is successful, but the current stage is not completed yet and
	 * the current stage IS the last stage which means the output needs to be treated
	 * specially since it is the final output (this is the focus of this test)
	 * 
	 * Output should be recored for being included in the final output.  Since the current
	 * stage is not finished, the processing of the final output is not done yet.
	 */
	@Test
	public void testUpdateVertexStatus_vertexSuccessful_currentStageNotFinished_isLastStage(){
		JobDescriptor jobInProgress = new JobDescriptor();
		jobInProgress.setJobId("1");
		
		JobStatus jobStatus = new JobStatus();
		jobStatus.setCurrentVertexStage(2);
		jobStatus.setTotalVertexStages(3);
		jobStatus.setExecutionFinished(false);
		jobStatus.setTotalVerticesCurrentStage(4);
		jobStatus.setTotalVerticesInProgressCurrentStage(2);
		
		jobInProgress.setJobStatus(jobStatus);		
		jobMgr.jobsInProgress.put("1", jobInProgress);
		
		// this will be a vertex that is still in progress
		// when status is reported for vertex v2
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setJobId("1");
		v1.setVertexNumber(1);
		v1.setStageNumber(2);
		v1.setVertexServerAssignment("vs1");
		v1.setExecutionFinished(false);
		
		// this will be the finished vertex that's status is reported
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setJobId("1");
		v2.setVertexNumber(2);
		v2.setStageNumber(2);
		v2.setVertexServerAssignment("vs2");
		v2.setExecutionFinished(true);
		v2.setExecutionSuccessful(true);
		
		jobMgr.verticesInProgress.add(v1);
		jobMgr.verticesInProgress.add(v2);

		// Give this vertex some dummy output file paths to report 
		// as part of the final output (there is no next vertex stage)
		VertexOutput v2Output = new VertexOutput();
		HashMap<Integer, List<String>> v2OutputMap = new HashMap<Integer, List<String>>();
		ArrayList<String> finalOutput = new ArrayList<String>();
		finalOutput.add("finalOutput1");
		finalOutput.add("finalOutput2");
		v2OutputMap.put(null, finalOutput);
		v2Output.setOutputMap(v2OutputMap);
		v2.setOutput(v2Output);
		
		// since the job wasnt started in the usual way, set up the output map
		jobMgr.outputMap.put(jobInProgress.getJobId(), new HashMap<Integer, HashMap<Integer,ArrayList<String>>>());
		jobMgr.updateVertexStatus(v2);
		
		Assert.assertFalse("Vertex 'v2' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v2));
		Assert.assertTrue("Vertex 'v1' should still be in progress",
				jobMgr.verticesInProgress.contains(v1));
		 
		JobStatus newJobStatus = jobMgr.jobsInProgress.get("1").getJobStatus();
		Assert.assertEquals("Total vertices in progress should have been decremented", 
				1, newJobStatus.getTotalVerticesInProgressCurrentStage());
		Assert.assertEquals("Total vertices in current stage should not have changed",
				4, newJobStatus.getTotalVerticesCurrentStage());
		Assert.assertEquals("Current vertex stage should not have changed", 2,
				newJobStatus.getCurrentVertexStage());
		Assert.assertFalse("Job is still in progress", newJobStatus.getExecutionFinished());
		
		// Check the output from v2 recorded for the final output (there is no next stage)
		HashMap<Integer, ArrayList<String>> nextStageVertexOutputMap = 
			jobMgr.outputMap.get(jobInProgress.getJobId()).get(null);
		Assert.assertNotNull("output map for final outpout shouldnt be null", nextStageVertexOutputMap);
		
		Assert.assertEquals("only the 'null vertex' denoting the final output should have output stored", 
				1, nextStageVertexOutputMap.keySet().size());
		
		Assert.assertEquals("null vertex should have 2 output files", 2, nextStageVertexOutputMap.get(null).size());
		Assert.assertTrue(nextStageVertexOutputMap.get(null).contains("finalOutput1"));
		Assert.assertTrue(nextStageVertexOutputMap.get(null).contains("finalOutput2"));

	}
	
	
	/**
	 * The last vertex of the current stage completes successfully.  This
	 * is not the last stage, so the output from this vertex and the others
	 * in the stage, must be transferred to the new stage as it's vertices
	 * are allocated 
	 * 
	 */
	@Test
	public void testUpdateVertexStatus_vertexSuccessful_currentStageFinished_hasNextStage(){
		JobDescriptor jobInProgress = new JobDescriptor();
		jobInProgress.setJobId("1");
		
		JobSpecification jobSpec = new JobSpecification(){
			{
				vertexStages = new ArrayList<VertexStage>();
				VertexStage stage0 = new VertexStage();
				vertexStages.add(stage0);
				
				VertexStage stage1 = new VertexStage();
				stage1.setNumVertices(4);
				stage1.setVertex(DummyVertex1.class);
				vertexStages.add(stage1);
				
				// need to define stage 2 since the current
				// stage of the test will be stage 1 and 
				// we want to test transition to stage 2
				VertexStage stage2 = new VertexStage();
				stage2.setNumVertices(3);
				stage2.setVertex(DummyVertex2.class);
				vertexStages.add(stage2);
			}
			
		};
		jobInProgress.setJobSpecification(jobSpec);
		
		JobStatus jobStatus = new JobStatus();
		jobStatus.setCurrentVertexStage(1);
		jobStatus.setTotalVertexStages(3);
		jobStatus.setExecutionFinished(false);
		jobStatus.setTotalVerticesCurrentStage(4);
		jobStatus.setTotalVerticesInProgressCurrentStage(3);
		
		jobInProgress.setJobStatus(jobStatus);		
		jobMgr.jobsInProgress.put("1", jobInProgress);
		
		// This vertex will complete first, having no output
		// for the next stage
		VertexDescriptor v0 = new VertexDescriptor();
		v0.setJobId(jobInProgress.getJobId());
		v0.setVertexNumber(0);
		v0.setStageNumber(1);
		v0.setVertexServerAssignment("some server");
		v0.setExecutionFinished(true);
		v0.setExecutionSuccessful(true);
		VertexOutput v0Output = new VertexOutput();
		v0Output.setOutputMap(new HashMap<Integer, List<String>>());
		v0.setOutput(v0Output);

		// This vertext will complete after 'v0' with
		// output files for one vertex in the
		// next stage (will be vertex 0)
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setJobId(jobInProgress.getJobId());
		v1.setVertexNumber(1);
		v1.setStageNumber(1);
		v1.setVertexServerAssignment("another server");
		v1.setExecutionFinished(true);
		v1.setExecutionSuccessful(true);
		VertexOutput v1Output = new VertexOutput();
		v1.setOutput(v1Output);
		HashMap<Integer, List<String>> v1OutputMap = new HashMap<Integer, List<String>>();
		v1.getOutput().setOutputMap(v1OutputMap);
		v1OutputMap.put(0, new ArrayList<String>());
		v1OutputMap.get(0).add("outputForV0_fromV1");


		// This is the last vertex to complete in the stage
		// - has output for vertices 0 and 2 in the next stage
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setJobId(jobInProgress.getJobId());
		v2.setVertexNumber(2);
		v2.setStageNumber(1);
		v2.setVertexServerAssignment("vs2");
		v2.setExecutionFinished(true);
		v2.setExecutionSuccessful(true);
		VertexOutput v2Output = new VertexOutput();
		v2.setOutput(v2Output);
		HashMap<Integer, List<String>> v2OutputMap = new HashMap<Integer, List<String>>();
		v2Output.setOutputMap(v2OutputMap);
		v2Output.getOutputMap().put(0, new ArrayList<String>());
		v2Output.getOutputMap().get(0).add("outputForV0_fromV2");
		v2Output.getOutputMap().get(0).add("outputForV0_fromV2_2");
		
		v2Output.getOutputMap().put(2, new ArrayList<String>());
		v2Output.getOutputMap().get(2).add("outputForV2_fromV2");
		

		// Add the vertices in to the in progress list
		// as they would be by the job manager
		jobMgr.verticesInProgress.add(v0);
		jobMgr.verticesInProgress.add(v1);
		jobMgr.verticesInProgress.add(v2);
		
		// since the job wasnt started in the usual way, set up the output map
		jobMgr.outputMap.put(jobInProgress.getJobId(), new HashMap<Integer, HashMap<Integer,ArrayList<String>>>());

		jobMgr.updateVertexStatus(v0);
		jobMgr.updateVertexStatus(v1);
		jobMgr.updateVertexStatus(v2);
		
		Assert.assertFalse("Vertex 'v0' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v0));
		Assert.assertFalse("Vertex 'v1' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v1));
		Assert.assertFalse("Vertex 'v2' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v2));
		
		JobStatus newJobStatus = jobMgr.jobsInProgress.get("1").getJobStatus();
		
		Assert.assertEquals("Current stage should have transition form 1 to 2", 
				2, newJobStatus.getCurrentVertexStage());

		Assert.assertEquals("Total vertices in progress is not correct", 
				jobSpec.getVertexStages().get(2).getNumVertices(), 
				newJobStatus.getTotalVerticesInProgressCurrentStage());

		Assert.assertEquals("Total vertices in stage is not correct", 
				jobSpec.getVertexStages().get(2).getNumVertices(), 
				newJobStatus.getTotalVerticesCurrentStage());

		Assert.assertFalse("Job is still in progress", newJobStatus.getExecutionFinished());


		//
		// Verify the output for the now current stage is correct based on the previous stage
		// (output of v0,v1,v2 above)
		//		
		HashMap<Integer, ArrayList<String>> stageVertexOutputMap = 
			jobMgr.outputMap.get(jobInProgress.getJobId()).get(newJobStatus.getCurrentVertexStage());

		Assert.assertEquals("Veretx 0 should have 3 total output files", 
				3, stageVertexOutputMap.get(0).size());
		Assert.assertTrue(stageVertexOutputMap.get(0).contains("outputForV0_fromV1"));
		Assert.assertTrue(stageVertexOutputMap.get(0).contains("outputForV0_fromV2"));
		Assert.assertTrue(stageVertexOutputMap.get(0).contains("outputForV0_fromV2_2"));
		
		Assert.assertNull("Vertex #1 should have no output files", 
				stageVertexOutputMap.get(1));
		
		Assert.assertEquals("Vertex #2 should have 1 output file",
				1, stageVertexOutputMap.get(2).size());
		Assert.assertTrue(stageVertexOutputMap.get(2).contains("outputForV2_fromV2"));
		
	}
	
	/**
	 * The last vertex of the current stage completes successfully.  This
	 * is the last stage, so the output from this vertex and the others
	 * in the stage, must be transferred as the final output to the output
	 * data structure and the output must be written to the final HDFS directory
	 * (specified in the JobSpecification)
	 */
	@Test
	public void testUpdateVertexStatus_vertexSuccessful_currentStageFinished_jobIsFinished()throws Exception{
		JobDescriptor jobInProgress = new JobDescriptor();
		jobInProgress.setJobId("1");
		
		JobSpecification jobSpec = new JobSpecification(){
			{
				vertexStages = new ArrayList<VertexStage>();
				VertexStage stage0 = new VertexStage();
				vertexStages.add(stage0);
				
				VertexStage stage1 = new VertexStage();
				stage1.setNumVertices(4);
				stage1.setVertex(DummyVertex1.class);
				vertexStages.add(stage1);
				
				// need to define stage 2 since the current
				// stage of the test will be stage 1 and 
				// we want to test transition to stage 2
				VertexStage stage2 = new VertexStage();
				stage2.setNumVertices(3);
				stage2.setVertex(DummyVertex2.class);
				vertexStages.add(stage2);
				
				this.outputDataPath = "/final/output/path";
			}
			
		};
		jobInProgress.setJobSpecification(jobSpec);
		
		JobStatus jobStatus = new JobStatus();
		jobStatus.setCurrentVertexStage(2);
		jobStatus.setTotalVertexStages(3);
		jobStatus.setExecutionFinished(false);
		jobStatus.setTotalVerticesCurrentStage(4);
		jobStatus.setTotalVerticesInProgressCurrentStage(3);
		
		jobInProgress.setJobStatus(jobStatus);		
		jobMgr.jobsInProgress.put("1", jobInProgress);
		
		// This vertex will complete first, having final output
		VertexDescriptor v0 = new VertexDescriptor();
		v0.setJobId(jobInProgress.getJobId());
		v0.setVertexNumber(0);
		v0.setStageNumber(2);
		v0.setVertexServerAssignment("some server");
		v0.setExecutionFinished(true);
		v0.setExecutionSuccessful(true);
		VertexOutput v0Output = new VertexOutput();
		v0Output.setOutputMap(new HashMap<Integer, List<String>>());
		v0.setOutput(v0Output);

		// This vertext will complete after 'v0' with
		// 2 final output files
		VertexDescriptor v1 = new VertexDescriptor();
		v1.setJobId(jobInProgress.getJobId());
		v1.setVertexNumber(1);
		v1.setStageNumber(2);
		v1.setVertexServerAssignment("another server");
		v1.setExecutionFinished(true);
		v1.setExecutionSuccessful(true);
		VertexOutput v1Output = new VertexOutput();
		v1.setOutput(v1Output);
		HashMap<Integer, List<String>> v1OutputMap = new HashMap<Integer, List<String>>();
		v1.getOutput().setOutputMap(v1OutputMap);
		v1OutputMap.put(null, new ArrayList<String>());
		v1OutputMap.get(null).add("finalOutputFromV1");
		v1OutputMap.get(null).add("finalOutputFromV1_2");


		// This is the last vertex to complete in the stage
		// - has 1 final output file
		VertexDescriptor v2 = new VertexDescriptor();
		v2.setJobId(jobInProgress.getJobId());
		v2.setVertexNumber(2);
		v2.setStageNumber(2);
		v2.setVertexServerAssignment("vs2");
		v2.setExecutionFinished(true);
		v2.setExecutionSuccessful(true);
		VertexOutput v2Output = new VertexOutput();
		v2.setOutput(v2Output);
		HashMap<Integer, List<String>> v2OutputMap = new HashMap<Integer, List<String>>();
		v2Output.setOutputMap(v2OutputMap);
		v2Output.getOutputMap().put(null, new ArrayList<String>());
		v2Output.getOutputMap().get(null).add("finalOutputFromV2");		
		

		// Add the vertices in to the in progress list
		// as they would be by the job manager
		jobMgr.verticesInProgress.add(v0);
		jobMgr.verticesInProgress.add(v1);
		jobMgr.verticesInProgress.add(v2);
		
		// since the job wasnt started in the usual way, set up the output map
		jobMgr.outputMap.put(jobInProgress.getJobId(), new HashMap<Integer, HashMap<Integer,ArrayList<String>>>());

		// Here, check for final output processing using HDFS utility
		hdfs.moveFilesToNewDir((List<String>)EasyMock.notNull(), EasyMock.eq(jobSpec.getOutputPath()));	
		EasyMock.replay(hdfs);
		
		jobMgr.updateVertexStatus(v0);
		jobMgr.updateVertexStatus(v1);
		jobMgr.updateVertexStatus(v2);
		
		EasyMock.verify(hdfs);
		
		Assert.assertFalse("Vertex 'v0' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v0));
		Assert.assertFalse("Vertex 'v1' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v1));
		Assert.assertFalse("Vertex 'v2' should have been removed from verticesInProgress set after completion", 
				jobMgr.verticesInProgress.contains(v2));
		
		JobStatus newJobStatus = jobMgr.jobsInProgress.get("1").getJobStatus();
		
		Assert.assertEquals("Current stage should still be 2 since 2 is the last stage", 
				2, newJobStatus.getCurrentVertexStage());

		Assert.assertEquals("Total vertices in progress is not correct", 
				0,
				newJobStatus.getTotalVerticesInProgressCurrentStage());

		Assert.assertEquals("Total vertices in stage is not correct", 
				4,
				newJobStatus.getTotalVerticesCurrentStage());

		Assert.assertTrue("Job is now finished", newJobStatus.getExecutionFinished());
		
		Assert.assertNull("Make sure job output map was cleaned up", jobMgr.outputMap.get("1"));
		
	}	
	
	
	/**
	 * Case where VertexServer reports a vertex as having failed -
	 * job should be failed 
	 */
	@Test
	public void testUpdateVertexStatus_vertexFailed(){
		JobStatus status = new JobStatus();
		status.setExecutionFinished(false);
		status.setCurrentVertexStage(1);
		status.setTotalVertexStages(2);
		status.setTotalVerticesInProgressCurrentStage(2);
		status.setTotalVerticesCurrentStage(2);
		
		JobDescriptor job = new JobDescriptor();
		job.setJobId("1");
		job.setJobStatus(status);
		
		jobMgr.jobsInProgress.put(job.getJobId(), job);

		// setup one in progress vertex - that the update
		// is for
		VertexDescriptor vertex1 = new VertexDescriptor();
		vertex1.setExecutionFinished(false);
		vertex1.setStageNumber(1);
		vertex1.setVertexNumber(1);
		vertex1.setJobId("1");
		jobMgr.verticesInProgress.add(vertex1);
		
		// setup another vertex that hasn't started executing
		// yet
		VertexDescriptor vertex2 = new VertexDescriptor();
		vertex2.setExecutionFinished(false);
		vertex2.setStageNumber(1);
		vertex2.setVertexNumber(0);
		vertex2.setJobId("1");
		jobMgr.allocatableVertices.add(vertex2);
		
		// copy of vertex1 - from vertex server
		VertexDescriptor vertex1_vserverCopy = new VertexDescriptor();		
		vertex1_vserverCopy.setStageNumber(1);
		vertex1_vserverCopy.setVertexNumber(1);
		vertex1_vserverCopy.setJobId("1");
		vertex1_vserverCopy.setExecutionFinished(true);
		vertex1_vserverCopy.setExecutionSuccessful(false);
		
		jobMgr.updateVertexStatus(vertex1_vserverCopy);
		
		Assert.assertTrue("Since the job failed, it should be marked as finished running", 
				jobMgr.jobsInProgress.get("1").getJobStatus().getExecutionFinished());		
		
		// vertex tracking data structures should be empty now since the job
		// was failed
		Assert.assertEquals(0, jobMgr.verticesInProgress.size());
		Assert.assertEquals(0, jobMgr.allocatableVertices.size());
		
		// check job status data - vertices in progress should reflect the fact that
		// the failed vertex did not complete
		Assert.assertEquals(1, jobMgr.jobsInProgress.get("1").getJobStatus().getCurrentVertexStage());
		Assert.assertEquals(2, jobMgr.jobsInProgress.get("1").getJobStatus().getTotalVertexStages());
		Assert.assertEquals(2, jobMgr.jobsInProgress.get("1").getJobStatus().getTotalVerticesInProgressCurrentStage());
		Assert.assertEquals(2, jobMgr.jobsInProgress.get("1").getJobStatus().getTotalVerticesCurrentStage());				
		
	}
	
	
	/**
	 * Case where vertex update comes after job has been
	 * removed from the job manager (because a vertex
	 * already failed and a client status request afterward
	 * caused the job to be removed)
	 */
	@Test
	public void testUpdateVertexStatus_jobNotFound(){

		// setup one in progress vertex - that the update
		// is for
		VertexDescriptor vertex1 = new VertexDescriptor();
		vertex1.setExecutionFinished(false);
		vertex1.setStageNumber(1);
		vertex1.setVertexNumber(1);
		vertex1.setJobId("1");

		
		jobMgr.updateVertexStatus(vertex1);
		

		// vertex tracking data structures should be empty still
		Assert.assertEquals(0, jobMgr.verticesInProgress.size());
		Assert.assertEquals(0, jobMgr.allocatableVertices.size());
		
	}
	
	private static JobDescriptor getJobDescriptor(String jobId){
		JobDescriptor jDesc = new JobDescriptor();
		JobSpecification jSpec = new JobSpecification(){
			{
				this.inputDataPath = "/input/path";
				this.outputDataPath = "/output/path";
				this.vertexStages = new ArrayList<VertexStage>();	
			}
		};
		
		jDesc.setJobId(jobId);
		jDesc.setHdfsJarPath("/path/job.jar");
		jDesc.setJobSpecification(jSpec);
		return jDesc;
	}
	
	/*
	 * A vertex implementation for testing
	 */
	private static class DummyVertex1 extends Vertex{

		@Override
		public void run(VertexContext context) {
			
		}
		
	}

	
	/*
	 * A vertex implementation for testing
	 */
	private static class DummyVertex2 extends Vertex{

		@Override
		public void run(VertexContext context) {
			
		}
		
	}

	
	/*
	 * A vertex implementation for testing
	 */
	private static class DummyVertex3 extends Vertex{

		@Override
		public void run(VertexContext context) {
			
		}
		
	}

}







