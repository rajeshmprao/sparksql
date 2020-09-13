package org.sahemant.DeploymentManager.ProviderAdapter

import org.apache.commons.lang3.NotImplementedException

import org.apache.spark.sql.SparkSession

object ProviderAdapterFactory {
  def getProviderAdapter(provider: String): SparkSession => IProviderAdapter = {
    if (provider.equalsIgnoreCase("delta")) {
      val deltaProvider = (sparkSession: SparkSession) => new DeltaProviderAdapter(sparkSession)
      return  deltaProvider
    }

    throw new NotImplementedException(s"adapter for provider : $provider is not implemented")
  }
}
