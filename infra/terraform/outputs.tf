output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.this.workspace_id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "key_vault_name" {
  value = azurerm_key_vault.this.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.this.vault_uri
}

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.this.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.this.fully_qualified_domain_name
}

output "catalogs" {
  value = [for c in databricks_catalog.this : c.name]
}
