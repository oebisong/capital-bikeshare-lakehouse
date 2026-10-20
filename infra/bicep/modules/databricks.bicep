// Premium Azure Databricks workspace and the access connector whose managed
// identity Unity Catalog uses to reach the storage account. Premium is the
// tier that supports Unity Catalog.

param location string
param namePrefix string
param tags object

var managedGroupName = 'rg-${namePrefix}-databricks-managed'

resource accessConnector 'Microsoft.Databricks/accessConnectors@2024-05-01' = {
  name: 'dbac-${namePrefix}'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

resource workspace 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: 'dbw-${namePrefix}'
  location: location
  tags: tags
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedGroupName)
  }
}

output workspaceUrl string = workspace.properties.workspaceUrl
output workspaceResourceId string = workspace.id
output accessConnectorId string = accessConnector.id
output accessConnectorPrincipalId string = accessConnector.identity.principalId
