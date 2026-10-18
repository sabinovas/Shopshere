# ---------------------------------------------------------------------------
# Workspace objects: Key Vault-backed secret scope and the Git folder.
# Both were created by hand (see docs/commands/databricks-api.md).
# ---------------------------------------------------------------------------

# Secret scope "shopsphere-kv", backed by kv-shopsphere-dev27.
# Read in notebooks with dbutils.secrets.get("shopsphere-kv", "sql-admin-password")
resource "databricks_secret_scope" "shopsphere_kv" {
  provider = databricks.workspace
  name     = "shopsphere-kv"

  keyvault_metadata {
    resource_id = azurerm_key_vault.this.id
    dns_name    = azurerm_key_vault.this.vault_uri
  }

  depends_on = [azurerm_role_assignment.kv_databricks_reader]
}

# Git folder that tracks the public repo's main branch.
# Path: /Repos/<working_user>/Shopshere
resource "databricks_repo" "shopshere" {
  provider     = databricks.workspace
  url          = "https://github.com/sabinovas/Shopshere.git"
  git_provider = "gitHub"
  path         = "/Repos/${var.working_user}/Shopshere"
  branch       = "main"
}
