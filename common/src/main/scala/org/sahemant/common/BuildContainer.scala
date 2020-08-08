package org.sahemant.common

import java.util.Dictionary

case class BuildContainer(schemas:List[SqlTable], tables: List[SqlTable], values: Map[String, String])
