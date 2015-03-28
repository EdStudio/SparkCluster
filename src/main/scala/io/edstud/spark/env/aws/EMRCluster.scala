package io.edstud.spark.env.aws

import scala.collection.immutable.List
import com.amazonaws.services.elasticmapreduce.model.Cluster
import com.amazonaws.services.elasticmapreduce.model.ClusterState
import com.amazonaws.services.elasticmapreduce.model.InstanceGroup
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType

class EMRCluster (
    private val cluster: Cluster,
    private val instanceGroups: List[InstanceGroup]) {

    def isStarting() : Boolean = isState(ClusterState.STARTING) || isState(ClusterState.BOOTSTRAPPING)

    def isRunning() : Boolean = isState(ClusterState.RUNNING) || isState(ClusterState.WAITING)

    def isTerminated() : Boolean = isState(ClusterState.TERMINATING) || isState(ClusterState.TERMINATED) || isState(ClusterState.TERMINATED_WITH_ERRORS)

    def isTransient(): Boolean = cluster.isAutoTerminate

    val master: InstanceGroup = getInstanceGroupByRole(InstanceRoleType.MASTER)(0)

    val core: Option[InstanceGroup] = getInstanceGroupByRole(InstanceRoleType.CORE).headOption

    val tasks: List[InstanceGroup] = getInstanceGroupByRole(InstanceRoleType.TASK)

    private def getInstanceGroupByRole(roleType: InstanceRoleType): List[InstanceGroup] = {
        instanceGroups.filter(_.getInstanceGroupType == roleType.toString)
    }

    private def isState(state: ClusterState): Boolean = {
        cluster.getStatus().getState() == state.toString
    }

    def getSparkMaster(port: Int): String = {
        "spark://%s:%d".format(cluster.getMasterPublicDnsName, port)
    }

    override def toString(): String = {
        cluster.toString + instanceGroups.map(_.toString).reduce(_ + _)
    }

}