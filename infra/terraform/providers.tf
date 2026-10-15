provider "azurerm" {
  features {}

  subscription_id = var.subscription_id
}

# Account console: metastore assignment only.
# Authenticates with your Azure CLI login (`az login`) as a Databricks account admin.
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}

# Workspace: storage credential, external locations, catalogs and schemas.
provider "databricks" {
  alias                       = "workspace"
  host                        = "https://${azurerm_databricks_workspace.this.workspace_url}"
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}
