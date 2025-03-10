from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName('UDF_Example') \
    .master('local[*]') \
    .getOrCreate()

data = [(1, 'Alice'), (2, 'Bob'), (3, 'John')]

df = spark.createDataFrame(data, ['id', 'name'])

def add_prefix(name: str) -> str:
    return f'Prefix_{name}'

# 1. Способ регистрации собственной функции через udf(user-defined functions)
add_prefix_udf = udf(add_prefix, StringType())

# Добавим новую колонку к DF
df_with_prefix = df.withColumn('prefix_name', add_prefix_udf(col('name')))

# 2. Способ регистрации собственной функции через udf(user-defined functions)
@udf(returnType=IntegerType())
def multiply2_id_udf(id: int) -> Optional[int]:
    if id is not None:
        return id * 2

# Создаем словарь с названием нового поля или заменяемого поля и применимую функцию к ней.
alter_col = {'prefix_name': add_prefix_udf(col('name')), # к полю name применяем префикс
             'multiply2_id': multiply2_id_udf(col('id')) # к полю id применяем умножение на 2
             }

df_with_udfs = df.withColumns(alter_col)

df_with_prefix.show()
df_with_udfs.show()

spark.stop()


