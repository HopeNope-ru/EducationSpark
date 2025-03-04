from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('SQLApiExample') \
    .master('local[*]') \
    .getOrCreate()

df_person = spark.read.json('../../content/person.json')
df_departments = spark.read.json('../../content/departments.json')

print('________RAW DF________')
df_person.show()
df_departments.show()

df_person.createOrReplaceTempView('person')
df_departments.createOrReplaceTempView('departments')

join_df = spark.sql("""
    select p.age, p.name, p.department_id
    from person p 
    join departments d on p.department_id = d.id
    """)

join_df.show()

join_df.write.csv('output.csv', header=True)