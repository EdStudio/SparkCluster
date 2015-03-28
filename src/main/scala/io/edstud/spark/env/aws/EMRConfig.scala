package io.edstud.spark.env.aws

case class EMRConfig (info: EMRInfo, roles: AWSRole, instanceGroups: EMRInstanceGroups)

case class EMRInfo(s3bucket: Option[String], region: Option[String], isTransientClutser: Boolean)

case class AWSRole(service: String, jobflow: String)

case class EMRInstanceGroups (master: EC2Instance, core: Option[EC2Instance], tasks: Array[EC2Instance])

case class EC2Instance (instanceType: String, count: Int, bidPrice: Option[Double])

