#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession, types
spark=SparkSession.builder.master('local').appName('modes').getOrCreate()
sc = spark.sparkContext


# In[3]:


df=spark.read.format('csv').option("mode","PERMISSIVE").load('Dataframe_mode.csv')


# In[4]:


df.show()


# In[5]:


df1=spark.read.format('csv').option("mode","FAILFAST").load('Dataframe_mode.csv')


# In[6]:


df1.show()


# In[7]:


df2_dropmalformed=spark.read.format('csv').option('mode','DROPMALFORMED').load('Dataframe_mode.csv')


# In[8]:


df2_dropmalformed.show()


# In[9]:


from pyspark.sql.types import *
schm=StructType ([
    StructField("Roll_Number",IntegerType(),True),
    StructField("Subject",StringType(),True),
    StructField("Marks",IntegerType(),True)
])


# In[10]:


df_permissive_schema=spark.read.option("mode","PERMISSIVE").csv('Dataframe_mode.csv',header=True,schema=schm)


# In[11]:


df_permissive_schema.show()


# In[12]:


df_dropmalformed=spark.read.option("mode","DROPMALFORMED").csv('Dataframe_mode.csv',header=True,schema=schm)


# In[13]:


df_dropmalformed.show()


# In[16]:


df_failfast=spark.read.option("mode","FAILFAST").csv('Dataframe_mode.csv',header=True,schema=schm)


# In[17]:


df_failfast.show()

