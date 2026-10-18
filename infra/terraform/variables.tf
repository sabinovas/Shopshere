variable "subscription_id" {
  description = "Azure subscription ID to deploy into (Azure subscription 1)."
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID (from the account console)."
  type        = string
}

variable "metastore_id" {
  description = "ID of the existing Unity Catalog metastore (shopsphere-metastore) in the same region."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "centralindia"
}

variable "project" {
  description = "Project short name, used in resource names."
  type        = string
  default     = "shopsphere"
}

variable "environment" {
  description = "Environment name. Week 1 deploys dev only; prod is added later if the budget allows."
  type        = string
  default     = "dev"
}

variable "name_suffix" {
  description = "Short lowercase alphanumeric suffix that makes globally unique names (storage, Key Vault, SQL, Event Hubs) unique. Built environment uses 27."
  type        = string
  default     = "27"

  validation {
    condition     = can(regex("^[a-z0-9]{1,6}$", var.name_suffix))
    error_message = "name_suffix must be 1-6 lowercase letters or digits."
  }
}

variable "catalog_envs" {
  description = "Environments that get a Unity Catalog catalog named shopsphere_<env>."
  type        = list(string)
  default     = ["dev"]
}

variable "sql_admin_login" {
  description = "Admin login for the Azure SQL server that holds the Olist source data."
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "Admin password for Azure SQL. Pass it via TF_VAR_sql_admin_password (it is stored in Key Vault as sql-admin-password), never commit it."
  type        = string
  sensitive   = true
}

variable "sql_client_ip" {
  description = "Your public IP address, allowed through the SQL firewall so you can load the Olist CSVs."
  type        = string
}

variable "databricks_service_principal_id" {
  description = "Object ID of the Azure Databricks service principal in this tenant (2ff814a6-… app). Needs Key Vault Secrets User to back the secret scope."
  type        = string
}

variable "working_user" {
  description = "Databricks user whose Git folder is created (/Repos/<user>/Shopshere)."
  type        = string
}

variable "budget_amount" {
  description = "Monthly subscription budget in the subscription currency."
  type        = number
  default     = 50
}

variable "budget_alert_email" {
  description = "Email address that receives budget alerts."
  type        = string
}

variable "budget_start_date" {
  description = "Budget start date (first day of a month, RFC3339)."
  type        = string
  default     = "2026-10-01T00:00:00Z"
}
