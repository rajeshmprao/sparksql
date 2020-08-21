package org.sahemant.DeploymentManager.Models

import net.liftweb.json.JsonAST.JValue

case class SqlTableField(name: String, datatype: String, nullable: Boolean, metadata: Map[String, JValue])
