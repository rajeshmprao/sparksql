package org.sahemant.DeploymentManager.Models

case class TableEntity(name: String, provider: String, location:String, schema:List[SqlTableField])
