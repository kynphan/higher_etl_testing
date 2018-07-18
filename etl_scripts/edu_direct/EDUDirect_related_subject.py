# Add related suibject stuff to lead fact table

import sys
import os
from datetime import datetime
import tempfile

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

import settings
import dynamic_frame_util as dfu
import date_util as du

glueContext = GlueContext(SparkContext.getOrCreate())
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENVIRONMENT', 'TYPE', 'START_DATE', 'END_DATE'])

JOB_NAME = args['JOB_NAME']
ENVIRONMENT = args['ENVIRONMENT']
DATABASE = args['DATABASE']
# possible values = ['highereducation-dw-edudirectdb-parquet-current', 'highereducation-dw-edudirectdb-parquet']
TYPE = None
START_DATE = args['START_DATE']
END_DATE = args['END_DATE']

fact_table_database = 'highereducation-dw-edudirectdb-staging-dev'
fact_table_v1= ''
fact_table_name = ''
database = DATABASE

if args['TYPE'] == 'current_day':
    fact_table_v1 = 'lead_fact_table_' + ENVIRONMENT + '_current_v1'
    fact_table_name = 'lead_fact_table_' + ENVIRONMENT + '_current'
else:
    fact_table_v1 = 'lead_fact_table_' + ENVIRONMENT + '_v1'
    fact_table_name = 'lead_fact_table_' + ENVIRONMENT

spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Required tables
lead = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_lead').toDF()
school_provider_program = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_school_provider_program').toDF()
school_program = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_school_program').toDF()
widget_degree = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_widget_degree').toDF()
widget_subject = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_widget_subject').toDF()
widget_category= dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_widget_category').toDF()
widget_degree_recomendation = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_widget_degree_recommendation').toDF()
widget_subject_recomendation = dfu.get_dyf_frame(database= database, tbl= 'cddirect_production_widget_subject_recommendation').toDF()

if du.is_valid_date(START_DATE) and du.is_valid_date(END_DATE):
    lead = lead.filter(lead['date_created'].between(lit(START_DATE), lit(END_DATE)))

def simple_join(fact_df, dim_df, fact_alias, dim_alias, fact_join_col, dim_join_col):
    fact_df = fact_df.alias(fact_alias)
    fact_join_column = fact_alias + '.' + fact_join_col

    dim_df = dim_df.alias(dim_alias)
    dim_join_column = dim_alias + '.' + dim_join_col

    dim_cols = map(
        lambda column: col(dim_alias + '.' + column).alias(dim_alias + '_' + column),
        dim_df.columns)

    result = fact_df.join(dim_df, col(fact_join_column) == col(dim_join_column), 'left') \
        .select([fact_alias + '.*'] + dim_cols)
    return result

def join_with_snapshot(fact_df, dim_df, fact_alias, dim_alias, fact_join_col, dim_join_col):
    fact_df = fact_df.alias(fact_alias)
    tmp_alias = 'tmp_' + dim_alias
    tmp_dim = dim_df.alias(tmp_alias)

    tmp_cols = map(
        lambda column: col(tmp_alias + '.' + column).alias(tmp_alias + '__' + column),
        dim_df.columns)

    definitive_cols = map(
        lambda column: col(dim_alias + '.' + column).alias(dim_alias + '_' + column),
        dim_df.columns)

    partial_join = fact_df.join(tmp_dim,
        [
            col(fact_alias + '.' + fact_join_col) == col(tmp_alias + '.' + dim_join_col),
            to_date(col(fact_alias + '.date_created')) == to_date(col(tmp_alias + '.snapshot_date'))
        ],
        'left') \
        .select([fact_alias + '.*'] + tmp_cols)

    newest_snapshot = dim_df.withColumn('rank',
            dense_rank().over(Window.partitionBy(dim_join_col).orderBy(desc('snapshot_date')))) \
        .filter('rank = 1') \
        .alias(dim_alias) \
        .drop(col('rank')) \
        .select(definitive_cols)

    result = partial_join.join(newest_snapshot,
        [
            col(fact_alias + '.' + fact_join_col) == col(dim_alias + '_' + dim_join_col),
            col(tmp_alias + '__snapshot_date').isNull()
        ],
        'left')

    dim_select_fields = []
    for column in dim_df.columns:
        result = result.withColumn(dim_alias + '_' + column,
                when(col(tmp_alias + '__' + column).isNull(), col(dim_alias + '_' + column)) \
                .otherwise(col(tmp_alias + '__' + column))) \
                .drop(tmp_alias + '__' + column)

    result = result.alias(fact_alias)
    return result

def construct_lead_fact_table(lead_df, alias):
    dimensions = [
        dict(df= school_provider_program, alias='spp_df', foreign_key='program_id', source_key='id'),
        dict(df= school_program, alias='sp_df', foreign_key='spp_df_school_program_id', source_key='id'),
        dict(df= widget_degree, alias='wd_df', foreign_key='sp_df_widget_degree_id', source_key='id'),
        dict(df= widget_subject, alias='ws_df', foreign_key='sp_df_widget_subject_id', source_key='id'),
        dict(df= widget_category, alias='wc_df', foreign_key='ws_df_category_id', source_key='id')
    ]
    for dim in dimensions:
        if 'snapshot_date' not in dim['df'].columns:
            lead_df = simple_join(
                lead_df, dim['df'], 'le_df', dim['alias'], dim['foreign_key'], dim['source_key'])
        else:
            lead_df = join_with_snapshot(
                lead_df, dim['df'], 'le_df', dim['alias'], dim['foreign_key'], dim['source_key'])

    lead_df = lead_df.select(['id', 'visitor_id', 'status_code', 'date_created', 'form_position',\
            col('wd_df_slug').alias('degree_slug'), col('wd_df_id').alias('degree_id'), col('wd_df_name').alias('degree_name'),\
            col('wc_df_slug').alias('category_slug'), col('wc_df_id').alias('category_id'), col('wc_df_name').alias('category_name'),\
            col('ws_df_slug').alias('subject_slug'), col('ws_df_id').alias('subject_id'), col('ws_df_name').alias('subject_name') ])
    return lead_df

def related_subject_simple(le_df, le_ff_df):
    wdr_df = widget_degree_recomendation.alias('wdr_df')
    wsr_df = widget_subject_recomendation.alias('wsr_df')

    result = le_df.join(le_ff_df, (le_df.visitor_id == le_ff_df.visitor_id)\
            & (le_df.date_created >= le_ff_df.from_date_created)\
            & (le_df.date_created < le_ff_df.to_date_created), 'left')\
        .join(wdr_df, ((le_ff_df.degree_id == wdr_df.degree_id) & (le_df.degree_id == wdr_df.recommended_id)), 'left')\
        .join(wsr_df, ((le_ff_df.subject_id == wsr_df.subject_id) & (le_df.subject_id == wsr_df.recommended_id)), 'left')\
        .select(['le_df.*',
            col('le_ff_df.degree_id').alias('ff_degree_id'), col('le_ff_df.degree_name').alias('ff_degree_name'), col('le_ff_df.degree_slug').alias('ff_degree_slug'),\
            col('le_ff_df.category_id').alias('ff_category_id'), col('le_ff_df.category_name').alias('ff_category_name'), col('le_ff_df.category_slug').alias('ff_category_slug'),\
            col('le_ff_df.subject_id').alias('ff_subject_id'), col('le_ff_df.subject_name').alias('ff_subject_name'), col('le_ff_df.subject_slug').alias('ff_subject_slug'),\
            col('wdr_df.recommended_id').alias('recc_degree_id'), col('wsr_df.recommended_id').alias('recc_subject_id')])

    result = result.alias('le_df')
    return result

def related_subject_with_snapshot(le_df, le_ff_df):
    le_df = le_df.alias('le_df')
    le_ff_df = le_ff_df.alias('le_ff_df')
    wdr_df = widget_degree_recomendation.alias('wdr_df')
    wsr_df = widget_subject_recomendation.alias('wsr_df')
    tmp_wdr = widget_degree_recomendation.alias('tmp_wdr')
    tmp_wsr = widget_subject_recomendation.alias('tmp_wsr')

    tmp_wdr_cols = map(
        lambda column: col('tmp_wdr.' + column).alias('tmp_wdr__' + column),
        tmp_wdr.columns)
    tmp_wsr_cols = map(
        lambda column: col('tmp_wsr.' + column).alias('tmp_wsr__' + column),
        tmp_wsr.columns)

    definitive_wdr_cols = map(
        lambda column: col('wdr_df.' + column).alias('wdr_df_' + column),
        widget_degree_recomendation.columns)
    definitive_wsr_cols = map(
        lambda column: col('wsr_df.' + column).alias('wsr_df_' + column),
        widget_subject_recomendation.columns)

    partial_join = le_df.join(le_ff_df, (le_df.visitor_id == le_ff_df.visitor_id)\
            & (le_df.date_created >= le_ff_df.from_date_created)\
            & (le_df.date_created < le_ff_df.to_date_created), 'left')\
        .join(tmp_wdr, ((le_ff_df.degree_id == tmp_wdr.degree_id)\
            & (le_df.degree_id == tmp_wdr.recommended_id)\
            & (to_date(le_df.date_created) == to_date(tmp_wdr.snapshot_date))), 'left')\
        .join(tmp_wsr, ((le_ff_df.subject_id == tmp_wsr.subject_id)\
            & (le_df.subject_id == tmp_wsr.recommended_id)\
            & (to_date(le_df.date_created) == to_date(tmp_wsr.snapshot_date))), 'left')\
        .select(['le_df.*'] + tmp_wdr_cols + tmp_wsr_cols)

    newest_snapshot_wdr = wdr_df.withColumn('rank',
            dense_rank().over(Window.partitionBy('id').orderBy(desc('snapshot_date')))) \
        .filter('rank = 1') \
        .alias('wdr_df') \
        .drop(col('rank')) \
        .select(definitive_wdr_cols) \
        .select(col('wdr_df_degree_id'), col('wdr_df_recommended_id')).distinct()

    newest_snapshot_wsr = wsr_df.withColumn('rank',
            dense_rank().over(Window.partitionBy('id').orderBy(desc('snapshot_date')))) \
        .filter('rank = 1') \
        .alias('wsr_df') \
        .drop(col('rank')) \
        .select(definitive_wsr_cols) \
        .select(col('wsr_df_subject_id'), col('wsr_df_recommended_id')).distinct()

    result = partial_join.join(le_ff_df, (le_df.visitor_id == le_ff_df.visitor_id)\
            & (le_df.date_created >= le_ff_df.from_date_created)\
            & (le_df.date_created < le_ff_df.to_date_created), 'left')\
        .join(newest_snapshot_wdr, ((le_ff_df.degree_id == newest_snapshot_wdr.wdr_df_degree_id)\
            & (le_df.degree_id == newest_snapshot_wdr.wdr_df_recommended_id)\
            & (partial_join.tmp_wdr__snapshot_date.isNull())), 'left')\
        .join(newest_snapshot_wsr, ((le_ff_df.subject_id == newest_snapshot_wsr.wsr_df_subject_id)\
            & (le_df.subject_id == newest_snapshot_wsr.wsr_df_recommended_id)\
            & (partial_join.tmp_wsr__snapshot_date.isNull())), 'left')\
        .withColumn('recc_degree_id', when(col('tmp_wdr__recommended_id').isNull(), col('wdr_df_recommended_id'))\
                .otherwise(col('tmp_wdr__recommended_id')))\
        .withColumn('recc_subject_id', when(col('tmp_wsr__recommended_id').isNull(), col('wsr_df_recommended_id'))\
                .otherwise(col('tmp_wsr__recommended_id')))\
        .select(['le_df.*',
            col('le_ff_df.degree_id').alias('ff_degree_id'), col('le_ff_df.degree_name').alias('ff_degree_name'), col('le_ff_df.degree_slug').alias('ff_degree_slug'),\
            col('le_ff_df.category_id').alias('ff_category_id'), col('le_ff_df.category_name').alias('ff_category_name'), col('le_ff_df.category_slug').alias('ff_category_slug'),\
            col('le_ff_df.subject_id').alias('ff_subject_id'), col('le_ff_df.subject_name').alias('ff_subject_name'), col('le_ff_df.subject_slug').alias('ff_subject_slug'),\
            col('recc_degree_id'), col('recc_subject_id')])

    result = result.alias('le_df')
    return result

# 1) filter the lead table and only extract the first form leads
le_df = lead.alias('le_df')
le_df = le_df.filter(le_df.form_position == 1)

# 2) construct lead fact table
le_df = construct_lead_fact_table(le_df, 'le_df')

# 3) rank the first form leads by created date partition by visitor id
# this will extract visitors that have multiple first form leads and compute a valid period
le_rnk_df = le_df.withColumn('rank', dense_rank().over(Window.partitionBy('visitor_id')\
    .orderBy(asc('date_created'), asc('id'))))
le_rnk_df = le_rnk_df.alias('le_rnk_df')

le_rnk2_df = le_rnk_df.withColumn('rank', le_rnk_df.rank - 1)
le_rnk2_df = le_rnk2_df.alias('le_rnk2_df')

le_ff_df = le_rnk_df.join(le_rnk2_df, ((le_rnk_df.visitor_id == le_rnk2_df.visitor_id)\
        & (le_rnk_df.rank == le_rnk2_df.rank)), 'left')\
    .select(['le_rnk_df.*', \
        col('le_rnk_df.date_created').alias('from_date_created'),\
        col('le_rnk2_df.date_created').alias('to_date_created')])\
    .withColumn('to_date_created', coalesce('to_date_created', lit(datetime.now()).cast('timestamp')))

# 4) construct lead fact table again
le_df = lead.alias('le_df')
le_df = construct_lead_fact_table(le_df, 'le_df')

le_df = le_df.alias('le_df')
le_ff_df = le_ff_df.alias('le_ff_df')

# 5) Join the results from the first form table with the lead fact table (type left)
if 'snapshot_date' not in widget_degree_recomendation.columns:
    le_df = related_subject_simple(le_df, le_ff_df)
else:
    le_df = related_subject_with_snapshot(le_df, le_ff_df)

le_df = le_df.alias('le_df')

# 6) Create the case statement that will classify the Form Relationship
tt_df = le_df.withColumn('subject_relation', when(le_df.form_position == 1, '1: First Lead')
    .when((le_df.subject_id == le_df.ff_subject_id) & (le_df.degree_id == le_df.ff_degree_id), '2: Same Subject & Degree')\
    .when((le_df.subject_id == le_df.ff_subject_id) & (le_df.recc_degree_id.isNotNull()), '3: Same Subject & Rel Degree')\
    .when((le_df.recc_subject_id.isNotNull()) & (le_df.degree_id == le_df.ff_degree_id), '4: Rel Subject & Same Degree')\
    .when((le_df.category_id == le_df.ff_category_id) & (le_df.degree_id == le_df.ff_degree_id), '5: Same Category')
    .when(le_df.degree_id == le_df.ff_degree_id, '6: Same Degree')
    .otherwise('7: No Relationship'))

tt_df = tt_df.alias('tt_df').select(
    ['id', 'subject_relation', 'ff_degree_id', 'ff_degree_name', 'ff_degree_slug',\
    'ff_category_id', 'ff_category_name', 'ff_category_slug', 'ff_subject_id', 'ff_subject_name', 'ff_subject_slug'])

# 7) Add subject relation column to fact table
fact_df = dfu.get_dyf_frame(database= fact_table_database, tbl= fact_table_v1).toDF()
fact_df = fact_df.alias('fact_df')

if du.is_valid_date(START_DATE) and du.is_valid_date(END_DATE):
    fact_df = fact_df.filter(fact_df['created_date'].between(lit(START_DATE), lit(END_DATE)))

# Remove subject relation columns to avoid problems in the future
related_subject_fields = ['subject_relation', 'first_form_degree_id', 'first_form_degree_name',\
    'first_form_degree_slug_name', 'first_form_category_id', 'first_form_category_name',\
    'first_form_category_slug_name', 'first_form_subject_id', 'first_form_subject_name',\
    'first_form_subject_slug_name']

for field in related_subject_fields:
    if ( field in fact_df.columns):
        fact_df = fact_df.drop(field)

fact_df = fact_df.join(tt_df, fact_df.lead_id == tt_df.id, 'left')\
    .select('fact_df.*', col('tt_df.subject_relation').alias('subject_relation'),\
        col('tt_df.ff_degree_id').alias('first_form_degree_id'),\
        col('tt_df.ff_degree_name').alias('first_form_degree_name'),\
        col('tt_df.ff_degree_slug').alias('first_form_degree_slug_name'),\
        col('tt_df.ff_category_id').alias('first_form_category_id'),\
        col('tt_df.ff_category_name').alias('first_form_category_name'),\
        col('tt_df.ff_category_slug').alias('first_form_category_slug_name'),\
        col('tt_df.ff_subject_id').alias('first_form_subject_id'),\
        col('tt_df.ff_subject_name').alias('first_form_subject_name'),\
        col('tt_df.ff_subject_slug').alias('first_form_subject_slug_name'))

fact_df = fact_df \
    .withColumn('month', month('created_date')) \
    .withColumn('day', dayofmonth('created_date')) \
    .withColumn('year', year('created_date'))
partition_keys= ['year', 'month', 'day']

if TYPE == 'current_day':
    fact_df.repartition(*partition_keys)

destination = os.path.join(settings.STAGING_BUCKET, 'EDUDirectDB', ENVIRONMENT, fact_table_name)
dfu.write_df_to_parq(
        df= fact_df, destination_path= destination, partition_keys=partition_keys, mode= 'overwrite')

job.commit()
