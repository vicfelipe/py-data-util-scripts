# Inicializa findspark e sparksession
import findspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

findspark.init() 
spark = SparkSession.builder.master('local[*]').getOrCreate()

# Criar dataframe com os CSV's
raw_df = spark.read.option("delimiter",";")\
      .option("format","CSV")\
      .option("recursiveFileLookup", "true")\
      .csv('csv_notas_entrada/', header=True)#\

# Exibe se foi criado corretamente
raw_df.show()

# Split columns
raw_df = raw_df.withColumn("FILENAME", F.split(F.col("path"), "/")[7])
raw_df = raw_df.withColumn("ANO", F.split(F.col("path"), "/")[5])
raw_df = raw_df.withColumn("PATH", F.split(F.col("path"), "/")[6])
raw_df.show()


# Converte columns
x = F.col("DATA_VENDA")
raw_df = raw_df.withColumn("output_dt", F.coalesce(F.to_date(x, "dd/MM/yyyy"), F.to_date(x, "yyyy-MM-dd")))
raw_df = raw_df.withColumn("output_dt2", F.date_format(F.col("output_dt"), format="dd/MM/yyyy"))
raw_df = raw_df.withColumn("DATA_VENDA", F.col("output_dt2"))

# Exclui colunas que não são necessárias
raw_df = raw_df.drop('output_dt')
raw_df = raw_df.drop('output_dt2')

# Exibe alterações
raw_df.show()

# Escreve saída em csv
raw_df.coalesce(1).write.options(delimiter=';').mode("overwrite").csv("saidas_csv",header = 'true')
