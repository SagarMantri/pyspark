#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[7]:


from pyspark.sql import SparkSession
spark=SparkSession.builder.master('local').appName('Transpose').getOrCreate()
sc=spark.sparkContext


# In[15]:


df=spark.read.format('csv').option('header',True).option('inferschema','True').load('students.csv')


# In[11]:


df.show()


# In[16]:


df.printSchema()


# In[19]:


tbl=df.groupby('Roll_no').pivot('Subject').sum('marks')


# In[20]:


tbl.show()


# In[22]:


tbl2=tbl.withColumn("total",tbl['English']+tbl['Maths'])


# In[24]:


tbl2.show()

