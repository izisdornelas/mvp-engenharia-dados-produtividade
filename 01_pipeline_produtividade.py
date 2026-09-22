# Databricks notebook source
# MAGIC %md
# MAGIC # MVP — produtividade de equipes na confecção
# MAGIC Fonte: [UCI, licença CC BY 4.0](https://archive.ics.uci.edu/dataset/597/productivity+prediction+of+garment+employees).
# MAGIC Execute primeiro a célula de preparação. Faça upload manual do arquivo `garments_worker_productivity.csv` ao volume indicado e, então, execute o restante de cima para baixo. A Free Edition pode restringir acesso externo; por isso a ingestão não depende de download pela internet.
# MAGIC **Status (22/09/2026):** executado no Databricks Free Edition; as cinco tabelas persistentes e as saídas foram conferidas. O HTML exportado com resultados registra a execução.

# COMMAND ----------

# 1. Configuração. Use um catálogo/esquema onde sua conta possa criar tabelas e volumes.
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "mvp_produtividade"
VOLUME = "entrada_uci"
CSV_NAME = "garments_worker_productivity.csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
CSV_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{CSV_NAME}"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"
print("Envie o CSV a:", CSV_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze — preservar a estrutura e os valores recebidos
# MAGIC Faça o upload do CSV para o volume antes de executar a próxima célula. Todos os campos são lidos como texto; o arquivo original continua guardado no volume.

# COMMAND ----------

raw = spark.read.option("header", True).option("inferSchema", False).option("mode", "FAILFAST").csv(CSV_PATH)
source_columns = ["date", "quarter", "department", "day", "team", "targeted_productivity", "smv", "wip", "over_time", "incentive", "idle_time", "idle_men", "no_of_style_change", "no_of_workers", "actual_productivity"]
assert raw.columns == source_columns, f"Colunas inesperadas no CSV: {raw.columns}"
bronze = raw.withColumn("source_file", F.lit(CSV_NAME)).withColumn("ingested_at", F.current_timestamp())
bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}.bronze_produtividade")
print("Linhas Bronze:", spark.table(f"{TABLE_PREFIX}.bronze_produtividade").count())
display(spark.table(f"{TABLE_PREFIX}.bronze_produtividade").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Perfil inicial — todos os atributos
# MAGIC Valores vazios e nulos são mostrados por coluna. Examinar domínio, datas, duplicatas e valores extremos antes de decidir tratamentos.

# COMMAND ----------

br = spark.table(f"{TABLE_PREFIX}.bronze_produtividade")
for field in source_columns:
    missing = br.filter(F.col(field).isNull() | (F.trim(F.col(field)) == "")).count()
    distinct = br.select(field).distinct().count()
    print(f"{field}: ausentes={missing}, distintos={distinct}")
print("Linhas completamente duplicadas:", br.groupBy(*source_columns).count().filter("count > 1").agg(F.sum(F.col("count") - 1)).first()[0] or 0)
display(br.groupBy("department").agg(F.count("*").alias("linhas"), F.sum(F.when(F.col("wip").isNull() | (F.trim("wip") == ""), 1).otherwise(0)).alias("wip_ausente")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver — tipos, padronização e regras verificáveis
# MAGIC `wip` ausente permanece nulo; produtividade realizada maior que 1 recebe um sinalizador, sem truncamento. `quarter` é uma subdivisão do mês na fonte, não trimestre civil.

# COMMAND ----------

numeric_columns = ["targeted_productivity", "smv", "wip", "over_time", "incentive", "idle_time", "idle_men", "no_of_style_change", "no_of_workers", "actual_productivity"]
typed = br.select(*[F.col(c) for c in source_columns], "source_file")
typed = typed.withColumn("data_registro", F.to_date("date", "M/d/yyyy"))
typed = typed.withColumn("equipe", F.expr("try_cast(team as int)"))
typed = typed.withColumn("setor_original", F.col("department"))
typed = typed.withColumn("setor", F.when(F.lower(F.trim("department")) == "sweing", F.lit("sewing")).otherwise(F.lower(F.trim("department"))))
for field in numeric_columns:
    typed = typed.withColumn(field, F.expr(f"try_cast(trim(`{field}`) as double)"))
typed = typed.withColumn("atingiu_meta", F.col("actual_productivity") >= F.col("targeted_productivity"))
typed = typed.withColumn("produtividade_acima_de_um", F.col("actual_productivity") > 1)
typed = typed.withColumn("dia_confere", F.date_format("data_registro", "EEEE") == F.col("day"))

# Falhas de conversão dos campos obrigatórios interrompem o pipeline, sem perda silenciosa.
required = ["data_registro", "equipe", "targeted_productivity", "actual_productivity"]
bad_required = typed.filter(F.expr(" OR ".join(f"`{x}` IS NULL" for x in required))).count()
assert bad_required == 0, f"Campos obrigatórios inválidos em {bad_required} linhas"
bad_day = typed.filter(~F.col("dia_confere") | F.col("dia_confere").isNull()).count()
assert bad_day == 0, f"Dia da semana inconsistente em {bad_day} linhas"
duplicate_keys = typed.groupBy("data_registro", "setor", "equipe").count().filter("count > 1").count()
assert duplicate_keys == 0, f"Chaves repetidas após padronização: {duplicate_keys}"

silver = typed.drop("date", "department", "team")
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}.silver_produtividade")
print("Linhas Silver:", spark.table(f"{TABLE_PREFIX}.silver_produtividade").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evidências de qualidade
# MAGIC Registrar no relatório as contagens, os limites observados e o impacto de cada regra. Um extremo é candidato a investigação; não é automaticamente um erro.

# COMMAND ----------

s = spark.table(f"{TABLE_PREFIX}.silver_produtividade")
assert s.count() == br.count(), "A transformação alterou o número de linhas"
print("WIP nulo:", s.filter(F.col("wip").isNull()).count())
print("Produtividade > 1:", s.filter("produtividade_acima_de_um").count())
for field in numeric_columns:
    values = s.agg(F.count(F.col(field)).alias("informados"), F.min(field).alias("minimo"), F.max(field).alias("maximo")).first()
    print(field, values.asDict())
print("Categorias de trimestre do mês e de setor:")
display(s.groupBy("quarter", "setor").count().orderBy("quarter", "setor"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold — tabelas próprias para as perguntas
# MAGIC O percentual de metas cumpridas sempre usa o total de registros do grupo como denominador. A média de `wip` ignora valores nulos e mostra quantos registros a sustentam.

# COMMAND ----------

def measures(grouped):
    return grouped.agg(
        F.count("*").alias("registros"),
        F.sum(F.col("atingiu_meta").cast("long")).alias("metas_atingidas"),
        F.round(100 * F.avg(F.col("atingiu_meta").cast("double")), 2).alias("percentual_metas_atingidas"),
        F.round(F.avg("targeted_productivity"), 4).alias("meta_media"),
        F.round(F.avg("actual_productivity"), 4).alias("produtividade_media"),
        F.round(F.avg("over_time"), 2).alias("minutos_extras_medios"),
        F.round(F.avg("idle_time"), 2).alias("inatividade_media"),
        F.count("wip").alias("registros_wip_informado"),
        F.round(F.avg("wip"), 2).alias("wip_medio_informado"),
    )

gold = {
    "gold_produtividade_setor": measures(s.groupBy("setor")),
    "gold_produtividade_dia_setor": measures(s.groupBy("data_registro", "setor")),
    "gold_produtividade_equipe": measures(s.groupBy("setor", "equipe")),
}
for name, frame in gold.items():
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{TABLE_PREFIX}.{name}")
    print(name, spark.table(f"{TABLE_PREFIX}.{name}").count(), "linhas")
assert spark.table(f"{TABLE_PREFIX}.gold_produtividade_setor").agg(F.sum("registros")).first()[0] == s.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Respostas técnicas — capturar resultados na plataforma
# MAGIC Interpretar os resultados observados no relatório após executar o notebook. Comparações de indicadores operacionais são descritivas e não identificam causa.

# COMMAND ----------

print("Pergunta 1: cumprimento das metas por setor")
display(spark.sql(f"SELECT * FROM {TABLE_PREFIX}.gold_produtividade_setor ORDER BY setor"))
print("Pergunta 2: evolução diária por setor e resultados por equipe")
display(spark.sql(f"SELECT * FROM {TABLE_PREFIX}.gold_produtividade_dia_setor ORDER BY data_registro, setor"))
display(spark.sql(f"SELECT * FROM {TABLE_PREFIX}.gold_produtividade_equipe ORDER BY setor, equipe"))
print("Pergunta 3: indicadores operacionais por setor e cumprimento de meta")
display(s.groupBy("setor", "atingiu_meta").agg(
    F.count("*").alias("registros"), F.round(F.avg("over_time"), 2).alias("minutos_extras_medios"),
    F.round(F.avg("idle_time"), 2).alias("inatividade_media"),
    F.count("wip").alias("registros_wip_informado"), F.round(F.avg("wip"), 2).alias("wip_medio_informado")
).orderBy("setor", "atingiu_meta"))
