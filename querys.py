# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Vendedores Japão") \
    .getOrCreate()

# COMMAND ----------

# DBTITLE 1,Qual país possui a maior quantidade de itens cancelados?
cancelados = """
SELECT 
    c.country, 
    SUM(od.quantity_Ordered) AS total_cancelled_items
FROM hive_metastore. default.orders o
JOIN hive_metastore. default.orderdetails od ON o.order_Number = od.order_Number
JOIN hive_metastore. default.customers c ON o.customer_Number = c.customer_Number
WHERE 
    o.status = 'Cancelled'
GROUP BY 
    c.country
ORDER BY 
    total_cancelled_items DESC
LIMIT 1;
"""

# COMMAND ----------

cancelados_df = spark.sql(cancelados)

# COMMAND ----------

cancelados_output_path = "/mnt/delta/itens_cancelados_pais"
cancelados_df.write.format("delta").mode("overwrite").save(cancelados_output_path)

# COMMAND ----------

#############################################################################################################################

# COMMAND ----------

# DBTITLE 1,2 - Qual o faturamento da linha de produto mais vendido, considere os pedidos com status &#39;Shipped&#39;, cujo pedido foi realizado no ano de 2005?
faturameto = """
SELECT 
    pl.product_line, 
    SUM(od.quantity_Ordered * od.price_Each) AS total_revenue
FROM hive_metastore.default.orders  o
JOIN hive_metastore.default.orderdetails  od ON o.order_Number = od.order_Number
JOIN hive_metastore.default.products p ON od.product_Code = p.product_Code
JOIN product_lines pl ON p.product_Line = pl.product_Line
WHERE 
    o.status = 'Shipped'
    AND YEAR(o.order_Date) = 2005
GROUP BY 
    pl.product_line
ORDER BY 
    total_revenue DESC
LIMIT 1;
"""

# COMMAND ----------

faturamento_df = spark.sql(faturameto)

# COMMAND ----------

faturamento_output_path = "/mnt/delta/faturamento_linha_produto"
faturamento_df.write.format("delta").mode("overwrite").save(faturamento_output_path)

# COMMAND ----------

##################################################################################################################################################################

# COMMAND ----------

# DBTITLE 1,Traga na consulta o Nome, sobrenome e e-mail dos vendedores do Japão, lembrando que o local-part do e-mail deve estar mascarado.
vendedores_japao = """
SELECT 
    e.first_Name AS nome, 
    e.last_Name AS sobrenome, 
    CONCAT('*****@', SUBSTRING_INDEX(e.email, '@', -1)) AS email_mascarado
FROM 
    hive_metastore.default.employees e
WHERE 
    e.office_Code IN (
        SELECT office_Code 
        FROM offices 
        WHERE city = 'Tokyo'
    )
ORDER BY 
    e.last_Name, e.first_Name;
"""

# COMMAND ----------

vendedores_df = spark.sql(vendedores_japao)

# COMMAND ----------

vendedores_output_path = "/mnt/delta/vendedores_japao"
vendedores_df.write.format("delta").mode("overwrite").save(vendedores_output_path)


# COMMAND ----------

spark.stop()
