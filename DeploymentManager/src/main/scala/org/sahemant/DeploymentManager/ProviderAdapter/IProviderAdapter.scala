package org.sahemant.DeploymentManager.ProviderAdapter

import org.sahemant.DeploymentManager.Models.TableEntity

trait IProviderAdapter {
  def alterSchema(newTable: TableEntity, oldTable: TableEntity)

  def changeProviderOrLocation(newTable: TableEntity, oldTable: TableEntity)
}
