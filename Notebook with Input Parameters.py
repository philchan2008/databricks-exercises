# Databricks notebook source
dbutils.widgets.text("year_param1", "2025", "Enter Year")
dbutils.widgets.combobox("year_param2", "2025", [str(year) for year in range(2000, 2026)])
year_value = dbutils.widgets.get("year_param1")
print(f"Selected year: {year_value}")

dbutils.widgets.dropdown("year_param3", "2025", [str(year) for year in range(2000, 2026)])
year_value = dbutils.widgets.get("year_param3")
print(f"Selected year: {year_value}")

dbutils.widgets.multiselect("year_param4", "2025", [str(year) for year in range(2000, 2026)])
year_value = dbutils.widgets.get("year_param4")
print(f"Selected year: {year_value}")   
