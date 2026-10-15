# ---------------------------------------------------------------------------
# Unity Catalog: metastore -> workspace, storage credential, external
# locations, catalogs and schemas (PLAN.md sections 5 and Week 1)
# ---------------------------------------------------------------------------

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  metastore_id = var.metastore_id
  workspace_id = azurerm_databricks_workspace.this.workspace_id
}

resource "databricks_storage_credential" "lake" {
  provider = databricks.workspace
  name     = "sc-${local.name_prefix}"
  comment  = "Access Connector identity for the ShopSphere lake"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }

  depends_on = [
    databricks_metastore_assignment.this,
    azurerm_role_assignment.connector_blob,
  ]
}

resource "databricks_external_location" "lake" {
  provider        = databricks.workspace
  for_each        = toset(["landing", "uc-managed", "checkpoints"])
  name            = "el-${each.key}"
  url             = "abfss://${each.key}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lake.name
  comment         = "ShopSphere ${each.key} container"
}

locals {
  lake_schemas = ["bronze", "silver", "gold", "ml", "ai", "ops", "volumes"]

  catalog_schemas = {
    for pair in setproduct(var.catalog_envs, local.lake_schemas) :
    "${pair[0]}.${pair[1]}" => { env = pair[0], schema = pair[1] }
  }
}

resource "databricks_catalog" "this" {
  provider     = databricks.workspace
  for_each     = toset(var.catalog_envs)
  name         = "shopsphere_${each.key}"
  comment      = "ShopSphere ${each.key} catalog"
  storage_root = "abfss://uc-managed@${azurerm_storage_account.lake.name}.dfs.core.windows.net/${each.key}"

  depends_on = [databricks_external_location.lake]
}

resource "databricks_schema" "this" {
  provider     = databricks.workspace
  for_each     = local.catalog_schemas
  catalog_name = databricks_catalog.this[each.value.env].name
  name         = each.value.schema
  comment      = "${each.value.schema} layer"
}
