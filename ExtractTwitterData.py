
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


from pyspark.sql import functions as F


# In[3]:


import re
from pyspark.sql.functions import lower, col, concat_ws, split


# In[4]:


get_ipython().system('hadoop fs -count /user/gsood/Project/Tweets')


# In[5]:


bigdf = sqlContext.read.json("/user/gsood/Project/Tweets/*")


# In[6]:


columnName="text";
bigdf=bigdf.withColumn(columnName, lower(col(columnName)))


# In[7]:


bigdf.printSchema()


# In[8]:


bigdf = bigdf.withColumn("entities.hashtags",bigdf.entities.hashtags)                .withColumn("entities.user_mentions",bigdf.entities.user_mentions)                .withColumn("place.country", bigdf.place.country)                .withColumn("place.full_name", bigdf.place.full_name)                .withColumn("place.id", bigdf.place.id)                .withColumn("place.place_type", bigdf.place.place_type)                .withColumn("user.id",bigdf.user.id)                .withColumn("user.description",bigdf.user.description)                .withColumn("user.screen_name",bigdf.user.screen_name)                .withColumn("user.verified",bigdf.user.verified)                .withColumn("user.followers_count",bigdf.user.followers_count)                .withColumn("user.listed_count",bigdf.user.listed_count)                .withColumn("user.friends_count",bigdf.user.friends_count)                .withColumn("user.favourites_count",bigdf.user.favourites_count)                .withColumn("user.statuses_count",bigdf.user.statuses_count)                .withColumn("user.created_at",bigdf.user.created_at)                .withColumn("user.lang",bigdf.user.lang)                .withColumn("user.location",bigdf.user.location)


# 1) Identifying tweets related to UChicago, Northwestern, UIUC, Princeton, Purdue, and Harvard

# In[9]:


chicago = ['university of chicago' , 'uchicago' , 'booth']
nyu =   [ 'new york university' , 'nyuniversity']


# In[10]:


uchicago = bigdf.filter(bigdf.text.rlike('|'.join(chicago)))
nyu = bigdf.filter(bigdf.text.rlike('|'.join(nyu)))
harvard = bigdf.filter(bigdf.text.like('%harvard%'))
columbia = bigdf.filter(bigdf.text.like('%columbia%'))
cornell = bigdf.filter(bigdf.text.like('%cornell%'))
stanford = bigdf.filter(bigdf.text.like('%stanford%'))


# In[11]:


uchicago = uchicago.drop('entities', 'display_text_range', 'extended_entities', 'extended_tweet','place', 'quoted_status', 'retweeted_status', 'user', 'geo','withheld_in_countries', 'limit', 'scopes', '')
nyu = nyu.drop('entities', 'display_text_range', 'extended_entities', 'extended_tweet','place', 'quoted_status', 'retweeted_status', 'user', 'geo','withheld_in_countries', 'limit', 'scopes', '')
harvard = harvard.drop('entities', 'display_text_range', 'extended_entities', 'extended_tweet','place', 'quoted_status', 'retweeted_status', 'user', 'geo','withheld_in_countries', 'limit', 'scopes', '')
columbia = columbia.drop('entities', 'display_text_range', 'extended_entities', 'extended_tweet','place', 'quoted_status', 'retweeted_status', 'user', 'geo','withheld_in_countries', 'limit', 'scopes', '')
cornell = cornell.drop('entities', 'display_text_range', 'extended_entities', 'extended_tweet','place', 'quoted_status', 'retweeted_status', 'user', 'geo','withheld_in_countries', 'limit', 'scopes', '')
stanford = stanford.drop('entities', 'display_text_range', 'extended_entities', 'extended_tweet','place', 'quoted_status', 'retweeted_status', 'user', 'geo','withheld_in_countries', 'limit', 'scopes', '')


# In[ ]:


uchicagopd = uchicago.toPandas()
nyupd = nyu.toPandas()
harvardpd = harvard.toPandas()
columbiapd = columbia.toPandas()
cornellpd = cornell.toPandas()
stanfordpd = stanford.toPandas()


# In[ ]:


uchicagopd.to_csv("uchicago.csv")
nyupd.to_csv("nyu.csv")
harvardpd.to_csv("harvard.csv")
columbiapd.to_csv("columbia.csv")
cornellpd.to_csv("cornell.csv")
stanfordpd.to_csv("stanford.csv")


# In[ ]:


get_ipython().system('pwd')


# In[ ]:


bigdf.filter(bigdf.text.like('%new york university%' or '%nyu%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%university of chicago%' or '%uchicago%' or '%booth%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%yale%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%berkeley%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%massachusetts institute of technology%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%cornell%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%columbia%')).count()


# In[ ]:


bigdf.filter(bigdf.text.like('%stanford%')).count()

