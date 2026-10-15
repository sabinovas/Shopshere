variable "subscription_id" {
  description = "Azure subscription ID to deploy into."
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID (from the account console)."
  type        = string
}

variable "metastore_id" {
  description = "ID of an existing Unity Catalog metastore in the same region. Create it once in the account console and reuse it."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "westeurope"
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
  description = "Short lowercase alphanumeric suffix that makes globally unique names (storage, Key Vault, SQL, Event Hubs) unique."
  type        = string

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
  default     = "shopsphere_admin"
}

variable "sql_admin_password" {
  description = "Admin password for Azure SQL. Pass it via TF_VAR_sql_admin_password, never commit it."
  type        = string
  sensitive   = true
}

variable "sql_client_ip" {
  description = "Your public IP address, allowed through the SQL firewall so you can load the Olist CSVs."
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
  default     = "2026-11-01T00:00:00Z"
}
