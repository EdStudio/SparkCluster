package io.edstud.spark.env.aws

import scala.collection.immutable.List
import scala.collection.JavaConverters._
import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult
import com.amazonaws.services.elasticmapreduce.model.ListInstanceGroupsRequest
import com.amazonaws.services.elasticmapreduce.model.AddInstanceGroupsRequest
import com.amazonaws.services.elasticmapreduce.model.ModifyInstanceGroupsRequest
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupModifyConfig
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType

import io.edstud.spark.env.ClusterProvider

/*
    Region Reference: http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-region.html
    Max Instance Count: 20
    Max Instance Group: 50
    Remarks: TODO - For eu-central-1 region, adjust the bucket name to s3://eu-central-1.support.elasticmapreduce/
    https://github.com/awslabs/emr-bootstrap-actions/blob/master/spark/config.file
*/

/*
    getNormalizedInstanceHours(): Int
    getStatus(): ClusterStatus
*/

class AmazonEMR (credentials: AWSCredentials) {

    private val emr: AmazonElasticMapReduceClient = new AmazonElasticMapReduceClient(credentials)

    private var clusters: List[ClusterSummary] = listCluster()

    def createCluster(): EMRBuilder = {
        new EMRBuilder(emr)
    }

    def getClusterByName(name: String, force: Boolean = false): Option[EMRCluster] = {

        if (force) reload()

        var result: Option[EMRCluster] = None
        val filtered = clusters.filter(_.getName == name)

        if (filtered.size == 1) {
            result = this.describeCluster(filtered(0).getId)
        }

        result
    }

    def getClusterSize(force: Boolean = false): Int = {

        if (force) reload()

        clusters.size
    }

    def describeCluster(id: String): Option[EMRCluster] = {

        var result: Option[EMRCluster] = None

        try {

            val search = emr.describeCluster(
                new DescribeClusterRequest().withClusterId(id)
            )

            val clusterInfo = emr.listInstanceGroups(
                new ListInstanceGroupsRequest().withClusterId(id)
            )

            result = Some(
                new EMRCluster(search.getCluster, clusterInfo.getInstanceGroups.asScala.toList)
            )

        } catch {
            case e: Exception => result = None
        }

        result
    }

    def addInstanceGroups(id: String, roleType: InstanceRoleType, config: EC2Instance): Boolean = {
        val result = emr.addInstanceGroups(
            new AddInstanceGroupsRequest().withJobFlowId(id).withInstanceGroups(
                EMRBuilder.initInstanceGroup(roleType, config)
            )
        )

        result.getInstanceGroupIds() != null
    }

    def modifyInstanceGroups(id: String, groupId: String, count: Int): Boolean = {
        var result: Boolean = false

        try {

            emr.modifyInstanceGroups(
                new ModifyInstanceGroupsRequest().withInstanceGroups(
                    new InstanceGroupModifyConfig().withInstanceGroupId(groupId).withInstanceCount(count)
                )
            )

            // Without further confirmation...

        } catch {
            case e: Exception => result = false
        }

        result
    }

    def terminateCluster(id: String): Boolean = {

        var result: Boolean = false

        try {

            emr.terminateJobFlows(
                new TerminateJobFlowsRequest().withJobFlowIds(id)
            )

            // Confirmation process
            result = this.describeCluster(id) match {
                case Some(cluster) => cluster.isTerminated
                case None => false
            }

        } catch {
            case e: Exception => result = false
        }

        result
    }

    private def listCluster(): List[ClusterSummary] = {
        val clusters = emr.listClusters().getClusters()

        clusters.asScala.toList
    }

    private def reload() = {
        clusters = listCluster()
    }

}

object AmazonEMR {

    def apply (accessKey: String, secretKey: String) {
        val credentials = new BasicAWSCredentials(accessKey, secretKey)

        new AmazonEMR(credentials)
    }

}