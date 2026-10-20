// Gives the access connector's managed identity read and write access to the
// storage account data. This role is the only storage permission Unity Catalog needs.

param storageAccountName string
param principalId string

var storageBlobDataContributor = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)

resource account 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource blobDataAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(account.id, principalId, storageBlobDataContributor)
  scope: account
  properties: {
    roleDefinitionId: storageBlobDataContributor
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}
