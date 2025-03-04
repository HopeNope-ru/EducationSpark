from py4j.protocol import NULL_TYPE
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName('JoinExample') \
    .master('local[*]') \
    .getOrCreate()

data_person = [
    ("John", 30, 1),
    ("Doe", 25, 2),
    ("Jane", 35, 1),
    ("Mark", 40, 2),
    ("Smith", 23, 3)
]

# Создаем схему для DataFrame
# Он состоит из StructType, который принимает в себя список StructField
schema_person = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('department_id', IntegerType(), True)
])

data_department = [
    (1, "HR"),
    (2, "Finance"),
    (3, "Engineering"),
    (4, "Marketing"),
    (5, None)
]

schema_department = StructType([
    StructField('id', IntegerType(), False),
    StructField('department_name', StringType(), True)
])

df_person = spark.createDataFrame(data_person, schema=schema_person)
df_departments = spark.createDataFrame(data_department, schema=schema_department)

print('person table:')
df_person.show()

print('department table:')
df_departments.show()

inner_join = df_person.join(df_departments,
                            df_person.department_id == df_departments.id,
                            'inner')

print('inner join:')
inner_join.show()

left_join = df_departments.join(df_person,
                                df_person.department_id == df_departments.id,
                                'left')
print('left join:')
left_join.show()

outer_join = df_person.join(df_departments,
                            df_person.department_id == df_departments.id,
                            'outer')

print('outer join:')
outer_join.show()

# Один из интересных join в spark с условием, по которому надо сделать соединение
cond_join = df_person.join(df_departments,
                           (df_person.department_id == df_departments.id) & (df_person.age > 30),
                           'inner')

print('condition join:')
cond_join.show()

spark.stop()