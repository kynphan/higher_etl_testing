# Job to construct fact table
import sys
import os
import re

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, concat, date_format, dayofmonth, hour, lit, lower, month, regexp_replace, to_date, when, year, udf
from pyspark.sql.types import StringType

import settings
import urlparse
import dynamic_frame_util as dfu

glueContext = GlueContext(SparkContext.getOrCreate())
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENVIRONMENT', 'TYPE'])

JOB_NAME = args['JOB_NAME']
ENVIRONMENT = args['ENVIRONMENT']
TYPE = args['TYPE']

spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

links_db = 'warehouse'
internal_ip_table = 'dev_internal_ip_csv_parquet'

if TYPE == 'current_day':
    platformevents_db = 'platform-events-parquet-current'
    edudirect_db = 'highereducation-dw-edudirectdb-parquet-current'
    result_table_name = 'fact_table_%s_current_v1' % ENVIRONMENT
else:
    platformevents_db = 'platform-events-parquet'
    edudirect_db = 'highereducation-dw-edudirectdb-parquet-current'
    result_table_name = 'fact_table_%s_v1' % ENVIRONMENT

marketing_db = 'marketing-parquet'
normalized_events = dfu.get_dyf_frame(database= platformevents_db, tbl= 'platform_events_public_normalized_events').toDF()
country = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_country').toDF()
state = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_state').toDF()
url = dfu.get_dyf_frame(database= marketing_db, tbl= 'dw_data_warehouse_url').toDF()
normalized_url = dfu.get_dyf_frame(database= marketing_db, tbl= 'dw_data_warehouse_normalized_url').toDF()
domain = dfu.get_dyf_frame(database= marketing_db, tbl= 'dw_data_warehouse_domain').toDF()
page_view = dfu.get_dyf_frame(database= marketing_db, tbl= 'dw_data_warehouse_page_view').toDF()
school = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_school').toDF()
school_program = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_school_program').toDF()
subject = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_widget_subject').toDF()
degree = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_widget_degree').toDF()
category = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_widget_category').toDF()
user = dfu.get_dyf_frame(database= edudirect_db, tbl= 'cddirect_production_user').toDF()

replace_null_name = 'missing_name'
default_status_value = 'accepted'
replace_null_numeric = 0
replace_null_id = 'missing_id'

def canonicalize_url(url, add_trailing_slash=False, remove_www=False, fail_gracefully=True):
    """
    This function will return the canonical form of an URL.
    Any URL that's introduced into the system has to go through this filter.

    Args:
        url: A valid url string. If a url scheme is provided, only http:// and https://
        should be used. If none is provided, http:// will be assumed.

        Note: If a non http/https schema is used, http:// will be prefixed anyway,
        likely causing problems and headaches for someone somewhere.
    """
    default_scheme = "http"
    url = url.strip()
    if not (url.startswith('http://') or url.startswith('https://')):
        url = '%s://%s' % (default_scheme, url)
    parsed_url = urlparse.urlsplit(url)
    scheme, netloc, path, query, fragment = parsed_url
    path = remove_consecutive_same_char(path, '/')
    # add trailing slash
    if add_trailing_slash and not path.endswith('/'):
        path = path + '/'
    # remove fragments
    fragment = ''
    # default to http if nothing exists
    if len(parsed_url.scheme) == 0:
        scheme = default_scheme
    # remove port 80 in url if it exists
    try:
        if parsed_url.port == 80:
            netloc = parsed_url.hostname
    except:
        pass

    # Not sure if www should be removed, so it's left in for now
    if remove_www and parsed_url.netloc.startswith('www.'):
        netloc = netloc.replace('www.', '')
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))

def remove_consecutive_same_char(string, remove=None):
    result = []
    prev_char = None
    for c in string:
        if c == prev_char and remove == c and remove or \
           c == prev_char and not remove:
            continue
        else:
            result.append(c)
        prev_char = c

    return ''.join(result)

# returns a tuple of stripped url and stripped subdomain
def strip_url(url):
    canonical_url = canonicalize_url(url)
    if canonical_url.endswith('/'):
        canonical_url = canonical_url[:-1]
    parsed_url = urlparse.urlsplit(canonical_url)
    scheme, netloc, path, query, fragment = parsed_url
    if parsed_url.netloc.startswith('www.'):
        netloc = parsed_url.netloc[4:]
    else:
        snl = netloc.split('.')
        if netloc.count('.') > 1 and snl[0].count('-') == 0:
            # s = re.compile(r'(?:\w*://)?(?:.*?\.)?(?:([a-zA-Z-1-9]*)\.)?([a-zA-Z-1-9]*\.[a-zA-Z]{1,})(.*)')
            # s = re.compile(r'(?:([a-zA-Z-1-9]*)\.)?([a-zA-Z-1-9]*\.[a-zA-Z]{1,}).*')
            # s = re.compile(r'(?:([a-zA-Z-0-9]*)\.)?(.*)')
            s = re.compile(r'(?:([w]+[0-9]*)\.)?(.*)')
            netloc = s.match(parsed_url.netloc).group(2)
    return (''.join([netloc, path, query, fragment]), netloc)

def select_fields(df, fields):
    return df.select(fields)

def clean_nulls(df, columns):
    return df.fillna(columns)

def apply_function(table_df, function, fields):
    for field in fields:
        if function == 'lower':
            try:
                table_df = table_df.withColumn(field, lower(col(field)))
            except:
                pass
        elif function == 'to_date':
            try:
                table_df = table_df.withColumn(field, to_date(col(field)))
            except:
                pass
        elif function == 'hour':
            try:
                table_df = table_df.withColumn(field, hour(col(field)))
            except:
                pass
    return table_df

# Fields that need to be selected and cleaned
fact_fields = [
    dict(
        name= 'id',
        alias= 'lead_id',
        replace_null= replace_null_id
    ),
    dict(
        name= 'session',
        alias= 'visitor_id',
        replace_null= replace_null_name
    ),
    dict(
        name= 'createdat',
        alias= 'created_time_date',
        replace_null= replace_null_name
    ),
    dict(
        name= 'createdat',
        alias= 'created_date',
        replace_null= replace_null_name
    ),
    dict(
        name= 'createdat',
        alias= 'created_hour_date',
        replace_null= replace_null_name
    ),
    dict(
        name= 'createdat',
        alias= 'modified_time_date',
        replace_null= replace_null_name,
        functions= []
    ),
    dict(
        name= 'createdat',
        alias= 'modified_date',
        replace_null= replace_null_name
    ),
    dict(
        name= 'createdat',
        alias= 'modified_hour_date',
        replace_null= replace_null_name
    ),
    dict(
        name= 'zipCode',
        alias= 'zipcode_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'browserMajor',
        alias= 'browser_major_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'browserName',
        alias= 'browser_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'browserVersion',
        alias= 'browser_version_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'deviceModel',
        alias= 'device_model_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'deviceType',
        alias= 'device_type_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'deviceVendor',
        alias= 'device_vendor_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'osName',
        alias= 'os_name',
        replace_null= replace_null_name
    ),
    dict(
        name= 'osVersion',
        alias= 'os_version',
        replace_null= replace_null_name
    ),
    dict(
        name= 'ip',
        alias= 'ip_address',
        replace_null= replace_null_name
    ),
]

#Add new fields to the final fact table
new_fields = [
    dict(name= 'data_source', value= 'platform-events'),
    dict(name= 'product_name', value= 'exclusive'),
    dict(name= 'product_code_name', value= 'EXCL'),
    dict(name= 'lead_cnt_base', value= 1),
    dict(name= 'lead_accepted_cnt_base', value= 1),
    dict(name= 'form_position_name', value= 1),
    dict(name= 'first_form_flag', value= 1),
    dict(name= 'education_level_id', value= replace_null_name),
    dict(name= 'education_level_name', value= replace_null_name),
    dict(name= 'age_name', value= replace_null_name),
    dict(name= 'grad_year_name', value= replace_null_name),
    dict(name= 'years_diff_grad_base', value= replace_null_name),
    dict(name= 'military_flag', value= replace_null_name),
    dict(name= 'status_name', value= default_status_value),
    dict(name= 'user_agent', value= replace_null_name),
    dict(name= 'engine_name', value= replace_null_name),
    dict(name= 'engine_version_name', value= replace_null_name),
    # added consolidation fields
    # numeric values
    dict(name= 'first_form_category_id', value= replace_null_numeric),
    dict(name= 'first_form_degree_id', value= replace_null_numeric),
    dict(name= 'first_form_subject_id', value= replace_null_numeric),
    dict(name= 'lead_dual_consent_cnt_base', value= replace_null_numeric),
    dict(name= 'lead_test_cnt_base', value= replace_null_numeric),
    # string values
    dict(name= 'first_form_category_name', value= replace_null_name),
    dict(name= 'first_form_category_slug_name', value= replace_null_name),
    dict(name= 'first_form_degree_name', value= replace_null_name),
    dict(name= 'first_form_degree_slug_name', value= replace_null_name),
    dict(name= 'first_form_subject_name', value= replace_null_name),
    dict(name= 'subject_relation', value= replace_null_name),
    dict(name= 'first_form_subject_slug_name', value= replace_null_name),
    dict(name= 'form_position_group_name', value='form 1'),
    dict(name= 'form_position_slug_name', value='form-1'),
]

fact_df = normalized_events.alias('fact_df')


fact_df = fact_df.filter(fact_df['eventKey'].isin(
    'WINKOUT_PRODUCT_VIEW','LINKOUT_PRODUCT_VIEW', 'LINKOUT_PRODUCT_CONVERSION', 'WINKOUT_PRODUCT_CONVERSION'))

final_fields = []

#### Country and state
country = country.alias('country')
state = state.alias('state')

fact_df = fact_df.join(country, fact_df.country == country.name, 'left') \
    .join(state, (fact_df.state == state.name) & (country.id == state.country_id), 'left')

fact_df = select_fields(fact_df, ['fact_df.*',
    col('country.name').alias('country_name'),
    col('country.id').alias('country_local_flag'),
    col('country.id').alias('country_id'),
    col('country.code').alias('country_code_name'),
    col('state.id').alias('state_id'),
    col('state.name').alias('state_name'),
    col('state.abbreviation').alias('state_code_name'),
    col('state.name').alias('state_local_name')])

final_fields.extend(['country_name', 'country_local_flag', 'country_id', 'country_code_name', 'state_id',
    'state_name', 'state_code_name', 'state_local_name'])


#### Pages
# fact_df.createOrReplaceTempView('fact_df')
# url.createOrReplaceTempView('url_dim')
# fact_df = spark.sql('select * from fact_df left join\
#         (select * from url_dim where id in (select max(id) from url_dim group by normalized_url_id)) as url\
#         on fact_df.referringUrl = url.url')
#normalized_url = normalized_url.alias('normalized_url')

fact_df = fact_df.alias('fact_df')
page_view = page_view.alias('page_view')
url = url.alias('url')

normalize_url_udf = udf(lambda x: strip_url(x)[0] if x else x, StringType())
subdomain_url_udf = udf(lambda x: strip_url(x)[1] if x else x, StringType())

fact_df = fact_df.withColumn('page_name', normalize_url_udf(col('referringUrl')))
fact_df = fact_df.withColumn('domain_name', subdomain_url_udf(col('referringUrl')))

fact_df = fact_df.alias('fact_df')

page_view = page_view.join(url, page_view.url_id == url.id, 'left')

page_view = select_fields(page_view, ['page_view.*', col('url.url')])
page_view = page_view.alias('page_view')

fact_df = fact_df.join(page_view, fact_df.page_name == page_view.url, 'left')

fact_df = select_fields(fact_df, ['fact_df.*',
        col('page_view.page_type').alias('page_type_name'),
        col('page_view.content_type').alias('page_content_name'),
        col('page_view.degree_type').alias('page_degree_name'),
        col('page_view.category_type').alias('page_category_name'),
        col('page_view.subject_type').alias('page_subject_name')])

final_fields.extend([col('referringUrl').alias('url_name'), 'page_name', 'domain_name',
    'page_type_name', 'page_content_name', 'page_degree_name', 'page_category_name', 'page_subject_name'])

#### School and program
fact_df.createOrReplaceTempView('fact_df')
school.createOrReplaceTempView('school')
school_program.createOrReplaceTempView('sp')
fact_df = spark.sql('select * from fact_df left join\
        school on fact_df.school = school.name left join\
        (select * from sp where id in (select max(id) from sp group by school_id, name)) as school_program\
        on fact_df.program = school_program.name and school.id = school_program.school_id')

## subject, category and degree
subject = subject.alias('subject')
degree = degree.alias('degree')
category = category.alias('category')

fact_df = fact_df.join(subject, school_program.widget_subject_id == subject.id, 'left') \
    .join(category, subject.category_id == category.id, 'left') \
    .join(degree, school_program.widget_degree_id == degree.id, 'left')

fact_df = select_fields(fact_df, ['fact_df.*',
        col('school_program.id').alias('program_id'), col('school_program.name').alias('program_name'),
        col('school.id').alias('school_id'), col('school.name').alias('school_name'), col('school.slug').alias('school_slug_name'),
        col('school.is_not_for_profit').alias('lead_program_school_nfp_flag'), col('school.is_enabled').alias('lead_program_school_enable_flag'),
        col('school.account_manager_id'),
        col('subject.id').alias('subject_id'), col('subject.name').alias('subject_name'), col('subject.slug').alias('subject_slug_name'),
        col('category.id').alias('category_id'), col('category.name').alias('category_name'), col('category.slug').alias('category_slug_name'),
        col('degree.id').alias('degree_id'), col('degree.name').alias('degree_name'), col('degree.slug').alias('degree_slug_name')])

final_fields.extend(['program_id', 'program_name', 'school_id', 'school_name', 'school_slug_name',
        'lead_program_school_nfp_flag', 'lead_program_school_enable_flag', 'account_manager_id', 'subject_id',
        'subject_name', 'subject_slug_name', 'category_id', 'category_name', 'category_slug_name', 'degree_id',
        'degree_name', 'degree_slug_name'])

#### User
fact_df = fact_df.alias('fact_df')
user = user.alias('user')

fact_df = fact_df.join(user, fact_df.account_manager_id == user.id, 'left')
fact_df = fact_df.withColumn('account_manager_name', concat(col('user.firstname'), lit(' '), col('user.lastname')))
fact_df = select_fields(fact_df, ['fact_df.*', 'account_manager_name'])

final_fields.extend(['account_manager_name'])

# Select definitive fields
fields_to_select = map(
    lambda field: col('fact_df.' + field['name']).alias(field['alias']),
    fact_fields)
fact_df = select_fields(fact_df, fields_to_select + final_fields)

# Clean nulls on fact fields
fact_df = clean_nulls(fact_df, { field['alias']: field['replace_null'] for field in fact_fields })

fields_to_clean = dict(
    country_name= replace_null_name,
    country_local_flag= replace_null_name,
    country_id= replace_null_id,
    country_code_name= replace_null_name,
    state_id= replace_null_id,
    state_name= replace_null_name,
    state_code_name= replace_null_name,
    state_local_name= replace_null_name,
#    page_id= replace_null_id,
    page_type_name= replace_null_name,
    page_content_name= replace_null_name,
    page_degree_name= replace_null_name,
    page_category_name= replace_null_name,
    page_subject_name= replace_null_name,
    domain_name= replace_null_id,
    program_id= replace_null_id,
    program_name= replace_null_name,
    school_id= replace_null_id,
    school_name= replace_null_name,
    school_slug_name= replace_null_name,
    lead_program_school_nfp_flag= 0,
    lead_program_school_enable_flag= 0,
    account_manager_id= replace_null_id,
    subject_id= replace_null_id,
    subject_name= replace_null_name,
    subject_slug_name= replace_null_name,
    category_id= replace_null_id,
    category_name= replace_null_name,
    category_slug_name= replace_null_name,
    degree_id= replace_null_id,
    degree_name= replace_null_name,
    degree_slug_name= replace_null_name,
    account_manager_name= replace_null_name
)
# Clean nulls on other fields
fact_df = clean_nulls(fact_df, fields_to_clean)

## apply functions to fields
fact_df = apply_function(fact_df, 'to_date', ['created_date', 'modified_date'])
fact_df = apply_function(fact_df, 'hour', ['created_hour_date', 'modified_hour_date'])
fact_df = apply_function(fact_df, 'lower', ['country_name', 'state_name', 'state_local_name', 'program_name',
        'school_name', 'subject_name', 'subject_slug_name', 'category_name', 'category_slug_name', 'degree_name',
        'degree_slug_name', 'account_manager_name'])

# Add extra fields to the fact table
for field in new_fields:
    fact_df = fact_df.withColumn(field['name'], lit(field['value']))

# added sufic for remaining tables
rename_mapping = dict(zip(
    ['data_source', 'data_source_name'],
    ['engine_version', 'engine_version_name']
))
fact_df.select([col(c).alias(rename_mapping.get(c, c)) for c in fact_df.columns])

# create a flag column to show that the access comes from a mobile device
fact_df = fact_df.withColumn('mobile_flag', when(col('device_type_name') == 'mobile', 1 ).otherwise(0))

# Join events table with list of internal IPs
internal_df = dfu.get_dyf_frame(database= links_db, tbl=internal_ip_table ).toDF()
fact_df.join(internal_df, fact_df.ip_address == internal_df.ip_address, how='left')

# create a new column to specify when the click it's internal(belongs to the specified offices list)
fact_df = fact_df.withColumn('internal_visit_flag', when(col('ip_address').isNotNull() , 1).otherwise(0))


# Cases
fact_df = fact_df.withColumn('country_local_flag',
        when(col('country_id') == 1, 1).otherwise(0))
fact_df = fact_df.withColumn('state_local_name',
        when(col('country_id') == 1, col('state_local_name')).otherwise('international'))
fact_df = fact_df.withColumn('zipcode_name',
        when(col('country_id') == 1, col('zipcode_name')).otherwise('international'))

fact_df = fact_df \
        .withColumn('month', month('created_date')) \
        .withColumn('day', dayofmonth('created_date')) \
        .withColumn('year', year('created_date'))

# Write fact table on staging folder
destination = os.path.join(settings.STAGING_BUCKET, 'PlatformEvents', 'tmp', result_table_name)
dfu.write_df_to_parq(
    df= fact_df,
    destination_path= destination,
    partition_keys= ['year', 'month', 'day'],
    mode= 'overwrite'
)

job.commit()
