
package com.kingsoft.samples;


import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Future;


import com.kingsoft.services.exception.KSCClientException;
import com.kingsoft.services.exception.KSCServiceException;
import com.kingsoft.services.kmr.*;
import com.kingsoft.services.auth.*;
import com.kingsoft.services.kmr.model.*;
import com.kingsoft.services.kmr.util.*;




public class KMRSample {

    public static void main(String[] args) throws IOException {

        final String endpoint = "your endpint";
        final String ak = "your ak";
        final String sk = "your sk";
        KSCCredentials credential = new BasicKSCCredentials(ak, sk);

        KSCMapReduceClient kmrClient = new KSCMapReduceClient(endpoint, credential);

        testListClusters(kmrClient);
    }


    private static String testRunJobFlow(KSCMapReduceClient kmrClient) throws IOException {


        String clusterName = "test-sdk";

        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: RunJobFlow");
        System.out.println("===========================================\n");

        String jobFlowId = "";
        try {

            System.out.println("Creating cluster " + clusterName + "\n");

            RunJobFlowRequest request = new RunJobFlowRequest()
                    .withName(clusterName)
                    .withLogUri("ks3://kmrhkbj/kmr-test/log")
                    .withInstances(new JobFlowInstancesConfig()
                            .withKeepJobFlowAliveWhenNoSteps(true)
                            .withInstanceGroups(
                                    new InstanceGroupConfig()
                                            .withInstanceRole("MASTER")
                                            .withInstanceCount(1)
                                            .withInstanceType("kmr.general"),
                                    new InstanceGroupConfig()
                                            .withInstanceType("kmr.general")
                                            .withInstanceCount(3)
                                            .withInstanceRole("CORE")
                            )
                    )
                    ;

            RunJobFlowResult result = kmrClient.runJobFlow(request);


            System.out.println("jobflow_id: ");
            System.out.println(result.getJobFlowId());
            System.out.println();
            jobFlowId = result.getJobFlowId();
        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
        return jobFlowId;
    }

    private static String testAddJobFlowSteps(KSCMapReduceClient kmrClient, String clusterId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: AddJobFlowSteps");
        System.out.println("===========================================\n");

        String stepId = "";
        try {

            StepConfig test_step = new StepConfig()
                    .withName("test_sdk_step")
                    .withActionOnFailure("CONTINUE")
                    .withHadoopJarStep(new StreamingStep()
                            .withInputs("ks3://kmrhkbj/kmr-test/job/wordcount-input")
                            .withOutput("ks3://kmrhkbj/step_output")
                            .withMapper( "ks3://kmrhkbj/kmr-test/job/binaries/streaming_mapper_example.py")
                            .withReducer("ks3://kmrhkbj/kmr-test/job/binaries/streaming_reducer_example.py")
                            .withNumMapTasks("1")
                            .withNumReduceTasks("1")
                            .toHadoopJarStepConfig()
                    );


            AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
                    .withJobFlowId(clusterId)
                    .withSteps(test_step)
                    ;

            AddJobFlowStepsResult result = kmrClient.addJobFlowSteps(request);


            System.out.println("stepId: ");
            System.out.println(result.getStepIds());
            System.out.println();
            stepId = result.getStepIds().get(0);

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
        return stepId;
    }

    private static void testDescribeCluster(KSCMapReduceClient kmrClient, String clusterId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: DescribeCluster");
        System.out.println("===========================================\n");

        try {

            DescribeClusterRequest request = new DescribeClusterRequest()
                    .withClusterId(clusterId);

            DescribeClusterResult result = kmrClient.describeCluster(request);

            System.out.println("Cluster: ");
            System.out.println(result.getCluster().toString());
            System.out.println();

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }

    private static void testDescribeStep(KSCMapReduceClient kmrClient, String clusterId, String stepId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: DescribeStep");
        System.out.println("===========================================\n");

        try {

            DescribeStepRequest request = new DescribeStepRequest()
                    .withClusterId(clusterId)
                    .withStepId(stepId);

            DescribeStepResult result = kmrClient.describeStep(request);

            System.out.println("Step: ");
            System.out.println(result.toString());
            System.out.println();

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }

    private static void testListClusters(KSCMapReduceClient kmrClient)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: ListClusters");
        System.out.println("===========================================\n");

        try {
            ListClustersRequest request = new ListClustersRequest()
                    .withClusterStates("RUNNING");
            ListClustersResult result = kmrClient.listClusters(request);


            System.out.println("Clusters: ");
            System.out.println(result.toString());
            System.out.println();

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }

    private static String testListInstanceGroups(KSCMapReduceClient kmrClient, String clusterId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: ListInstanceGroups");
        System.out.println("===========================================\n");

        String instanceGroupId = "";
        try {

            ListInstanceGroupsRequest request = new ListInstanceGroupsRequest()
                    .withClusterId(clusterId);

            ListInstanceGroupsResult result = kmrClient.listInstanceGroups(request);

            System.out.println("InstanceGroups: ");
            System.out.println(result.toString());
            System.out.println();
            instanceGroupId = result.getInstanceGroups().get(0).getId();
        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
        return instanceGroupId;
    }

    private static void testListInstances(KSCMapReduceClient kmrClient, String clusterId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: ListInstances");
        System.out.println("===========================================\n");

        try {

            ListInstancesRequest request = new ListInstancesRequest()
                    .withClusterId(clusterId);

            ListInstancesResult result = kmrClient.listInstances(request);

            System.out.println("Instances: ");
            System.out.println(result.toString());
            System.out.println();

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }

    private static void testListSteps(KSCMapReduceClient kmrClient, String clusterId, String stepId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: ListSteps");
        System.out.println("===========================================\n");

        try {

            ListStepsRequest request = new ListStepsRequest()
                    .withClusterId(clusterId)
                    .withMarker("offset=0 & limit=100")
                    .withStepIds(stepId)
                    .withStepStates("FAILED");

            ListStepsResult result = kmrClient.listSteps(request);

            System.out.println("Steps: ");
            System.out.println(result.toString());
            System.out.println();

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }


    private static void testModifyInstanceGroups(KSCMapReduceClient kmrClient,
                                                 String clusterId, String instanceGroupId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: ModifyInstanceGroups");
        System.out.println("===========================================\n");

        try {

            ModifyInstanceGroupsRequest request = new ModifyInstanceGroupsRequest()
                    .withClusterId(clusterId)
                    .withInstanceGroups(new InstanceGroupModifyConfig()
                            .withInstanceGroupId(instanceGroupId)
                            .withInstanceCount(4)
                            );

            kmrClient.modifyInstanceGroups(request);


        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }



    private static void testSetTerminationProtection(KSCMapReduceClient kmrClient,
                                                     String clusterId, Boolean terminateProtection)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: SetTerminateProtection");
        System.out.println("===========================================\n");

        try {

            SetTerminationProtectionRequest request = new SetTerminationProtectionRequest()
                    .withJobFlowIds(clusterId)
                    .withTerminationProtected(terminateProtection);

             kmrClient.setTerminationProtection(request);

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }


    private static void testTerminateJobFlows(KSCMapReduceClient kmrClient,
                                                     String clusterId)
            throws IOException {


        System.out.println("===========================================");
        System.out.println("Getting Started with KMR: TerminateJobFlows");
        System.out.println("===========================================\n");

        try {

            TerminateJobFlowsRequest request = new TerminateJobFlowsRequest()
                    .withJobFlowIds(clusterId);

            kmrClient.terminateJobFlows(request);

        } catch (KSCServiceException kse) {
            System.out.println("Caught an KSCServiceException, which means your request made "
                    + "to KMR, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + kse.getMessage());
        } catch (KSCClientException kce) {
            System.out.println("Caught an KSCClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with KMR, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + kce.getMessage());
        }
    }

}
