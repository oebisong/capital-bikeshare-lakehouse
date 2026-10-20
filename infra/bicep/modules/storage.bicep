// ADLS Gen2 account for the lakehouse. One container holds the landing files
// (external volume) and the managed Unity Catalog tables.

param location string
param namePrefix string
param tags object
param containerName string = 'lakehouse'

// Storage account names are global: 3 to 24 lowercase letters and digits.
var storageAccountName = take('st${namePrefix}${uniqueString(subscription().id, resourceGroup().id)}', 24)

resource account 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    // Databricks reaches the account through the access connector's managed
    // identity, so account keys are switched off.
    allowSharedKeyAccess: false
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: account
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

output storageAccountName string = account.name
output containerName string = container.name
