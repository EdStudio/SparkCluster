package io.edstud.spark.env.aws

import java.util.ArrayList
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.StepConfig
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.PlacementType
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig
import com.amazonaws.services.elasticmapreduce.model.MarketType
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import io.edstud.spark.env.ClusterBuilder

class EMRBuilder (emr: AmazonElasticMapReduceClient) {

    private val request: RunJobFlowRequest = new RunJobFlowRequest()

    private val sparkScript: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig()

    def init(): String = {
        val sparkInstall = new BootstrapActionConfig()
            .withName("Spark Install")
            .withScriptBootstrapAction(
                sparkScript.withPath("s3://support.elasticmapreduce/spark/install-spark")
            )

        val result: RunJobFlowResult = emr.runJobFlow(
            request.withBootstrapActions(sparkInstall)
        )

        result.getJobFlowId
    }

    def withName(name: String): this.type = {
        request.withName(name)

        this
    }

    def withConfig(config: EMRConfig): this.type = {

        // TODO: Modify / Check - Security Group inbound traffic for current IP

        request
            .withLogUri(config.info.s3bucket.getOrElse("")) // s3://myawsbucket/
            .withServiceRole(config.roles.service) // service_role
            .withJobFlowRole(config.roles.jobflow) // jobflow_role
            //.withTerminationProtected(false)
            .withInstances(
                new JobFlowInstancesConfig().withKeepJobFlowAliveWhenNoSteps(config.info.isTransientClutser).withInstanceGroups(
                    initInstanceGroups(config.instanceGroups)
                ).withPlacement(
                    new PlacementType().withAvailabilityZone(config.info.region.getOrElse(""))
                )//.withEc2KeyName(config.keyPair)
            )


        this
    }

    def withSparkVersion(version: String): this.type = {
        sparkScript.withArgs("-v=" + version)

        this
    }

    def withAmiVersion(version: String): this.type = {
        request.withAmiVersion(version)

        this
    }

    def withDebugEnabled(): this.type = {
        val enableDebugging: StepConfig = new StepConfig()
            .withName("Enable debugging")
            .withActionOnFailure("CONTINUE") // "TERMINATE_JOB_FLOW", "CANCEL_AND_WAIT", "CONTINUE"
            .withHadoopJarStep(new StepFactory().newEnableDebuggingStep())

        request.withSteps(enableDebugging)

        this
    }


    private def initInstanceGroups(config: EMRInstanceGroups): ArrayList[InstanceGroupConfig] = {

        val groups = new ArrayList[InstanceGroupConfig]();

        groups.add(EMRBuilder.initInstanceGroup(InstanceRoleType.MASTER, config.master))

        if (config.core.isDefined)
            groups.add(EMRBuilder.initInstanceGroup(InstanceRoleType.CORE, config.core.head))

        if (config.tasks != null) {
            for (taskGroupConfig <- config.tasks) {
                groups.add(EMRBuilder.initInstanceGroup(InstanceRoleType.TASK, taskGroupConfig))
            }
        }

        groups
    }

}

object EMRBuilder {

    def initInstanceGroup(roleType: InstanceRoleType, config: EC2Instance): InstanceGroupConfig = {

        val group = new InstanceGroupConfig()
            .withInstanceRole(roleType) // InstanceRoleType.MASTER, InstanceRoleType.CORE, InstanceRoleType.TASK
            .withInstanceType(config.instanceType) // "m3.xlarge", "m1.large" etc...
            .withInstanceCount(config.count)

        if (config.bidPrice.isDefined) {
            group.withMarket(MarketType.SPOT).withBidPrice(config.bidPrice.toString)
        } else {
            group.withMarket(MarketType.ON_DEMAND)
        }

        group
    }

}