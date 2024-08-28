from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, round, to_date, lit, pow, current_timestamp, datediff, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import sqlite3
import pandas as pd

spark = SparkSession.builder \
    .appName("Desafio Dev Databricks - Solução Completa") \
    .config("spark.hadoop.io.file.buffer.size", "4096") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.io.nativeio.nativepath", "false") \
    .getOrCreate()

#Step 1
IDRISCO_CARGA_CENTRAL = "fff27777-89d6-481e-af20-cfdaa5adadad"
DTREFERENCIA = "2024-08-31"

spark.conf.set("spark.local.dir", "C:/Windows/Temp")

df = spark.read.csv("dados-entrada.csv", header=True, inferSchema=True, sep=';')

df = df.withColumn("DATAINICIOVIGENCIA", to_date(col("DATAINICIOVIGENCIA"), "yyyy-MM-dd")) \
       .withColumn("DATAFINALVIGENCIA", to_date(col("DATAFINALVIGENCIA"), "yyyy-MM-dd")) \
       .withColumn("DATAREFERENCIA", to_date(col("DATAREFERENCIA"), "yyyy-MM-dd")) \
       .withColumn("DHREGISTROTD", to_date(col("DHREGISTROTD"), "yyyy-MM-dd"))

df_transformed = df.withColumn("idrisco_carga_central", expr("unhex('fff2777789d6481eaf20cfdaa5adadad')")) \
                   .withColumn("dsctacos", expr("'1612000'")) \
                   .withColumn("dsorgrec", expr("'0199'")) \
                   .withColumn("nrtaxidx", expr("21")) \
                   .withColumn("nrperidx", expr("100")) \
                   .withColumn("nrvarcam", expr("790")) \
                   .withColumn("cdnatope", expr("1")) \
                   .withColumn("dscaresp", expr("NULL")) \
                   .withColumn("flprejuz", expr("NULL")) \
                   .withColumn("qtdiaatr", expr("NULL")) \
                   .withColumn("tpcartao", expr("NULL")) \
                   .withColumn("qtparcela", expr("NULL")) \
                   .withColumn("dtsaida", expr("NULL")) \
                   .withColumn("tpcontrato", expr("1")) \
                   .withColumn("dtproxima_parcela", expr("NULL")) \
                   .withColumn("vlproxima_parcela", expr("NULL")) \
                   .withColumn("nrcepcon", when(col("COOPERATIVA") == 1, '89010971')
                                           .when(col("COOPERATIVA") == 2, '89201260')
                                           .when(col("COOPERATIVA") == 3, '89041110')
                                           .when(col("COOPERATIVA") == 5, '88811700')
                                           .when(col("COOPERATIVA") == 6, '88034050')
                                           .when(col("COOPERATIVA") == 7, '88020020')
                                           .when(col("COOPERATIVA") == 8, '88020000')
                                           .when(col("COOPERATIVA") == 9, '88075301')
                                           .when(col("COOPERATIVA") == 10, '88508190')
                                           .when(col("COOPERATIVA") == 11, '88307326')
                                           .when(col("COOPERATIVA") == 12, '89270000')
                                           .when(col("COOPERATIVA") == 13, '89287440')
                                           .when(col("COOPERATIVA") == 14, '85601630')
                                           .when(col("COOPERATIVA") == 16, '89140000')
                                           .otherwise('89041110')) \
                   .withColumn("nrtaxeft", round(((pow(1 + (col("TAXAJUROSMES") / 100), 12) - 1) * 100), 2)) \
                   .withColumn("cdproduto_contabil", when(col("VALORUTILIZADO") > 0, '104').otherwise('103'))

df_transformed_pandas = df_transformed.toPandas()

conn = sqlite3.connect(":memory:")

df_transformed_pandas.to_sql('CREDITOGESTAO_LIMITES_CHESPECIAL', conn, if_exists='replace', index=False)

df_sqlite = pd.read_sql_query("SELECT * FROM CREDITOGESTAO_LIMITES_CHESPECIAL", conn)

print(df_sqlite)

conn.close()

#End step 1

windowSpec = Window.orderBy("NRCONTRATO")

df_with_row_number = df_transformed.select("NRCONTRATO").withColumn("cdcarga_operacao", row_number().over(windowSpec))

df_transformed = df_transformed.join(df_with_row_number, on="NRCONTRATO", how="left")

df_transformed = df.withColumn("idrisco_carga_central", lit("fff2777789d6481eaf20cfdaa5adadad"))

if 'codigoCIF' not in df_transformed.columns:
    df_transformed = df_transformed.withColumn("codigoCIF", lit(None).cast("int"))

if 'JUROSSUSPENSO' not in df_transformed.columns:
    df_transformed = df_transformed.withColumn("JUROSSUSPENSO", lit(0).cast("int"))

if 'FL_ATIVO_PROBLEMATICO' not in df_transformed.columns:
    df_transformed = df_transformed.withColumn("FL_ATIVO_PROBLEMATICO", lit(0).cast("int"))

df_bronze = df_transformed.filter(df_transformed.idrisco_carga_central == "fff2777789d6481eaf20cfdaa5adadad")

df_bronze = df_bronze.withColumn("VALORUTILIZADO", regexp_replace(col("VALORUTILIZADO"), ",", ".").cast("float"))
df_bronze = df_bronze.withColumn("VALORNAOUTILIZADO", regexp_replace(col("VALORNAOUTILIZADO"), ",", ".").cast("float"))

df_limite_utilizado = df_bronze.filter(col("VALORUTILIZADO") > 0) \
    .withColumn("cdmodalidade", expr("'0213'")) \
    .withColumn("cdrisco_refinanciamento", lit(None).cast("int")) \
    .withColumn("vlsaldo_limite", col("VALORUTILIZADO")) \
    .withColumn("idsistema_origem", lit(2)) \
    .withColumn("cdCif", col("codigoCIF")) \
    .withColumn("vlcontabil_bruto", col("VALORUTILIZADO") - col("JUROSSUSPENSO")) \
    .withColumn("qtparcela_paga", lit(None).cast("int")) \
    .withColumn("vlparcela_paga", lit(None).cast("double")) \
    .withColumn("nrversao_contrato", lit(None).cast("int")) \
    .withColumn("tprenegociacao", lit(None).cast("int")) \
    .withColumn("pedesconto_renegociacao", lit(None).cast("double")) \
    .withColumn("flativo_problematico", col("FL_ATIVO_PROBLEMATICO")) \
    .withColumn("vlperda_acumulada", lit(None).cast("double"))

#df_limite_utilizado.show(truncate=False)

df_limite_nao_utilizado = df_bronze.filter(col("VALORNAOUTILIZADO") > 0) \
    .withColumn("cdmodalidade", expr("'1902'")) \
    .withColumn("cdrisco_refinanciamento", lit(None).cast("int")) \
    .withColumn("vlsaldo_limite", col("VALORNAOUTILIZADO")) \
    .withColumn("idsistema_origem", lit(2)) \
    .withColumn("cdCif", col("codigoCIF")) \
    .withColumn("vlcontabil_bruto", col("VALORNAOUTILIZADO") - col("JUROSSUSPENSO")) \
    .withColumn("qtparcela_paga", lit(None).cast("int")) \
    .withColumn("vlparcela_paga", lit(None).cast("double")) \
    .withColumn("nrversao_contrato", lit(None).cast("int")) \
    .withColumn("tprenegociacao", lit(None).cast("int")) \
    .withColumn("pedesconto_renegociacao", lit(None).cast("double")) \
    .withColumn("flativo_problematico", col("FL_ATIVO_PROBLEMATICO")) \
    .withColumn("vlperda_acumulada", lit(None).cast("double"))

#df_limite_nao_utilizado.show(truncate=False)

df_silver = df_limite_utilizado.unionByName(df_limite_nao_utilizado)

#df_silver.show(truncate=False)

df_vencimento = df_silver.withColumn("cdvencimento",
    when(col("vlsaldo_limite") > 0,
         when(datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 30, 110)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 30) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 60), 120)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 60) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 90), 130)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 90) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 180), 140)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 180) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 360), 150)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 360) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 720), 160)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 720) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 1080), 165)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 1080) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 1440), 170)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 1440) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 1800), 175)
        .when((datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) > 1800) & (datediff(col("DATAFINALVIGENCIA"), to_date(lit(DTREFERENCIA))) <= 5400), 180)
        .otherwise(190)
    ).otherwise(
        when(datediff(col("DATAFINALVIGENCIA"), col("DATAINICIOVIGENCIA")) <= 360, 20)
        .otherwise(40)
    )
)

df_pandas = df_vencimento.toPandas()
df_pandas.to_csv('E:/Repositories/ailosTest/pythonDesenv/silver-vencimentos.csv', index=False)

print('silver-vencimentos.csv created!')

df_silver.withColumn("DHREGISTRO", current_timestamp())

df_silver_pandas = df_silver.toPandas()
df_silver_pandas.to_csv('E:/Repositories/ailosTest/pythonDesenv/silver-operacao.csv', index=False)

print('silver-operacao.csv created!')

spark.stop()
