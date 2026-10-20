// Azure resources for the Capital Bikeshare lakehouse.
//
// Two resource groups keep collected data apart from compute:
//   rg-<prefix>-data      ADLS Gen2 account holding the landing files and tables
//   rg-<prefix>-platform  Databricks workspace and the access connector
// make down deletes the platform group and keeps the data group.

targetScope = 'subscription'

@description('Azure region for every resource.')
param location string

@description('Short lowercase prefix for resource names, 3 to 10 letters or digits.')
@minLength(3)
@maxLength(10)
param namePrefix string

@description('Tags applied to every resource group and resource.')
param tags object = {
  project: 'capital-bikeshare-analytics-lakehouse'
  managedBy: 'bicep'
}

var prefix = toLower(namePrefix)
var dataGroupName = 'rg-${prefix}-data'
var platformGroupName = 'rg-${prefix}-platform'

resource dataGroup 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: dataGroupName
  location: location
  tags: tags
}

resource platformGroup 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: platformGroupName
  location: location
  tags: tags
}

module storage 'modules/storage.bicep' = {
  name: 'storage'
  scope: dataGroup
  params: {
    location: location
    namePrefix: prefix
    tags: tags
  }
}

module databricks 'modules/databricks.bicep' = {
  name: 'databricks'
  scope: platformGroup
  params: {
    location: location
    namePrefix: prefix
    tags: tags
  }
}

module storageAccess 'modules/storage-access.bicep' = {
  name: 'storage-access'
  scope: dataGroup
  params: {
    storageAccountName: storage.outputs.storageAccountName
    principalId: databricks.outputs.accessConnectorPrincipalId
  }
}

output workspaceUrl string = databricks.outputs.workspaceUrl
output workspaceResourceId string = databricks.outputs.workspaceResourceId
output accessConnectorId string = databricks.outputs.accessConnectorId
output storageAccountName string = storage.outputs.storageAccountName
output storageContainer string = storage.outputs.containerName
output dataResourceGroup string = dataGroup.name
output platformResourceGroup string = platformGroup.name
