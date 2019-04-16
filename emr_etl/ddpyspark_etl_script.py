from pyspark.sql import SparkSession, Row
from functools import reduce
import requests
import json
import pandas as pd
import boto3

s3_resource = boto3.resource('s3')  # w/ EMR change to download from S3
s3_bucket = s3_resource.Bucket('ddapi.data')

spark = SparkSession \
    .builder \
    .appName("pysparkDDapp") \
    .getOrCreate()

SEASON_1819 = 'https://pkgstore.datahub.io/sports-data/' \
              'english-premier-league/season-1819_json/data/' \
              '121aec954d44d69659e8da82196f0997/season-1819_json.json'

SEASON_1718 = 'https://pkgstore.datahub.io/sports-data/' \
              'english-premier-league/season-1718_json/data/' \
              'dbd8d3dc57caf91d39ffe964cf31401b/season-1718_json.json'

content_1718 = requests.get(SEASON_1718)
json1718 = json.loads(content_1718.content)

content_1819 = requests.get(SEASON_1819)
json1819 = json.loads(content_1819.content)

df_1718 = spark.createDataFrame(Row(**x) for x in json1718)
df_1819 = spark.createDataFrame(Row(**x) for x in json1819)

df = df_1819.union(df_1718)


df_1718 = pd.DataFrame(json1718)
df_1718.head()

df_1819 = pd.DataFrame(json1819)
df_1819.head()

df_1718_teams = set(df_1718.HomeTeam.unique().tolist())
df_1819_teams = set(df_1819.HomeTeam.unique().tolist())
df_set = df_1718_teams.union(df_1819_teams)
teamList = list(df_set)

df_total = pd.concat([df_1718, df_1819], ignore_index=True)
df_total['total_score'] = (df_total.FTHG + df_total.FTAG)
df_total['label'] = df_total['total_score'].apply(lambda x:
                                                  1 if x > 2.5 else 0)
df_total = df_total.sort_values(by=['Date'], ascending=True) \
                   .reset_index(drop=True)
df_total['idx_test'] = df_total.index


def last5_byteam(df=df_total, team='Arsenal'):
    dfteam = df.loc[((df.HomeTeam == team) | (df.AwayTeam == team)), :]
    dfteam = dfteam.drop(columns=['Referee'])
    dfteam.head()
    dfteam['team'] = team

    def __goals(row, home, away):
        if row["HomeTeam"] == team:
            return row[home]
        elif row["AwayTeam"] == team:
            return row[away]

    dfteam['goals'] = dfteam.apply(__goals, axis=1, home='FTHG', away='FTAG')
    dfteam['shots_on'] = dfteam.apply(__goals, axis=1, home='HST', away='AST')

    dfteam = dfteam.sort_values(by=['Date']).reset_index(drop=True)

    dfteam['last5goals'] = dfteam.goals.rolling(5, min_periods=1).sum() \
                                                                 .shift(1) \
                                                                 .fillna(0)

    dfteam['last5shots_on'] = dfteam.shots_on.rolling(5, min_periods=1) \
                                    .sum().shift(1).fillna(0)
    dfteam2 = dfteam[['Date', 'team', 'last5goals', 'last5shots_on']]

    return dfteam2


df_dict = {}
for team in teamList:
    df_dict[team] = last5_byteam(team=team)

# create SparkDataFrame from df_total
sp_df = spark.createDataFrame(df_total)
sp_df.show(3)

# build dict with spark DataFrames
sdf_dict = {}
for k, v in df_dict.items():
    sdf_dict[k] = spark.createDataFrame(v)

# create list of tempviews for spark sql
spsql_list = []
for team in teamList:
    sdf_dict[team].createOrReplaceTempView((str(team).replace(" ",
                                                              "_")+'_sql'))
    spsql_list.append(((str(team).replace(" ", "_")+'_sql')))

# build each teams "last 5" dataframe
spdf_dict = {}
for s in spsql_list:
    sp_df.createOrReplaceTempView("spdf_sql")
    query = """select a.*,
                 b.team,
                 (case when a.Date = b.Date and a.HomeTeam = b.team
                      then b.last5goals else 0 end) as homeLast5goals,
                 (case when a.Date = b.Date and a.AwayTeam = b.team
                      then b.last5goals else 0 end) as awayLast5goals,
                 (case when a.Date = b.Date and a.HomeTeam = b.team
                      then b.last5shots_on else 0 end) as homeLast5shots_on,
                 (case when a.Date = b.Date and a.AwayTeam = b.team
                      then b.last5shots_on else 0 end) as awayLast5shots_on
               from spdf_sql as a
                 left join {} as b
                   on a.Date = b.Date
                     and (a.HomeTeam = b.team OR a.AwayTeam = b.team)
                order by a.Date""".format(s)
    sqlDF = spark.sql(query)
    sqlDF[['label', 'Date', 'HomeTeam', 'AwayTeam', 'team',
           'total_score', 'homeLast5goals', 'awayLast5goals',
           'homeLast5shots_on', 'awayLast5shots_on']].show(20)
    type(sqlDF)
    spdf_dict[s] = sqlDF

pandas_dict = {}
for key, spark_sqlDF in spdf_dict.items():
    pandas_dict[key] = spdf_dict[key].toPandas()

# pandas_dict['Arsenal_sql'].head()
# pandas_dict['Arsenal_sql'].info()
# df_total.head()
# df_total.info()

# make list of dataframes to use in append
pds_list = []
for k, v in pandas_dict.items():
    pds_list.append(v)

df_total2 = (reduce(lambda df1, df2:
                    df1.append(df2,
                               ignore_index=True), pds_list)) \
                       .groupby(['idx_test',
                                 'Date'])['homeLast5goals',
                                          'awayLast5goals',
                                          'homeLast5shots_on',
                                          'awayLast5shots_on'].sum()

df_total3 = df_total2.reset_index(drop=False)

df_final = df_total.merge(df_total3, on=['Date', 'idx_test'])
df_final = df_final.drop(columns=['FTHG', 'FTAG', 'HTHG', 'HTAG',
                                  'HS', 'AS', 'HST', 'AST', 'HF',
                                  'AF', 'HC', 'AC', 'HY', 'AY',
                                  'HR', 'AR', 'FTR', 'HTR',
                                  'total_score', 'Referee',
                                  'idx_test'])

# filter above 5 gameweek so that we have enough data for last 5 fields
df_final = df_final[df_final.Date >= '2017-09-23'].reset_index(drop=True)
df_final = df_final.drop(columns=['Date'])  # then drop Date

# send json to s3 bucket
json = df_final.to_json(orient='records')
s3_bucket.put_object(Body=json, Key='modelDataFrame.json')
