data "azurerm_client_config" "current" {}

locals {
  name_prefix = "${var.project}-${var.environment}"

  tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ---------------------------------------------------------------------------
# Resource group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# ADLS Gen2 storage (hierarchical namespace on) with the three containers
# from PLAN.md: landing, uc-managed, checkpoints
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "lake" {
  name                     = "st${var.project}${var.environment}${var.name_suffix}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_container" "lake" {
  for_each = toset(["landing", "uc-managed", "checkpoints"])

  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# Access Connector (managed identity) that Unity Catalog uses to reach storage
# ---------------------------------------------------------------------------
resource "azurerm_databricks_access_connector" "this" {
  name                = "ac-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "connector_blob" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Databricks workspace (Premium is required for Unity Catalog)
# ---------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${local.name_prefix}"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "rg-dbw-${local.name_prefix}-managed"
  tags                        = local.tags
}

# ---------------------------------------------------------------------------
# Key Vault: backs the Databricks secret scope (scope itself is created in
# the workspace, see README)
# ---------------------------------------------------------------------------
resource "azurerm_key_vault" "this" {
  name                       = "kv-${local.name_prefix}-${var.name_suffix}"
  resource_group_name        = azurerm_resource_group.this.name
  location                   = var.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  tags                       = local.tags
}

resource "azurerm_role_assignment" "kv_admin" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ---------------------------------------------------------------------------
# Event Hubs (Basic tier) for the clickstream generator in Week 3
# ---------------------------------------------------------------------------
resource "azurerm_eventhub_namespace" "this" {
  name                = "evhns-${local.name_prefix}-${var.name_suffix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  sku                 = "Basic"
  tags                = local.tags
}

resource "azurerm_eventhub" "clickstream" {
  name              = "clickstream"
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = 2
  message_retention = 1
}

# ---------------------------------------------------------------------------
# Azure SQL (Basic tier) holds the Olist orders, customers and payments
# ---------------------------------------------------------------------------
resource "azurerm_mssql_server" "this" {
  name                         = "sql-${local.name_prefix}-${var.name_suffix}"
  resource_group_name          = azurerm_resource_group.this.name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_login
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = local.tags
}

resource "azurerm_mssql_database" "olist" {
  name        = "olist"
  server_id   = azurerm_mssql_server.this.id
  sku_name    = "Basic"
  max_size_gb = 2
  tags        = local.tags
}

resource "azurerm_mssql_firewall_rule" "client" {
  name             = "allow-client-ip"
  server_id        = azurerm_mssql_server.this.id
  start_ip_address = var.sql_client_ip
  end_ip_address   = var.sql_client_ip
}

resource "azurerm_mssql_firewall_rule" "azure_services" {
  name             = "allow-azure-services"
  server_id        = azurerm_mssql_server.this.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# ---------------------------------------------------------------------------
# Cost control: monthly budget with an alert at 80% (PLAN.md section 10)
# ---------------------------------------------------------------------------
resource "azurerm_consumption_budget_subscription" "monthly" {
  name            = "budget-${local.name_prefix}"
  subscription_id = "/subscriptions/${var.subscription_id}"
  amount          = var.budget_amount
  time_grain      = "Monthly"

  time_period {
    start_date = var.budget_start_date
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    contact_emails = [var.budget_alert_email]
  }
}
