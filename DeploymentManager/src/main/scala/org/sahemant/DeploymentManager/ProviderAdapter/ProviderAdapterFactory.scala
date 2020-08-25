package org.sahemant.DeploymentManager.ProviderAdapter

import org.apache.spark.sql.SparkSession

object ProviderAdapterFactory {
  def getProviderAdapter(provider: String): SparkSession => IProviderAdapter = {
    if (provider.equalsIgnoreCase("delta")) {
      val deltaProvider = (sparkSession:SparkSession) => new DeltaProviderAdapter(sparkSession)
      return  deltaProvider
    }
    throw new NotImplementedError(s"adapter for provider : $provider is not implemented")
  }
}
