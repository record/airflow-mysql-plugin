from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.S3_hook import S3Hook
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.file import TemporaryDirectory


class S3ToMySqlLoadOperator(BaseOperator):
    template_fields = ('s3_bucket',
                       's3_prefix',
                       'mysql_database',
                       'mysql_table',
                       'mysql_infields')
    template_ext = tuple()

    def __init__(self,
                 aws_conn_id='aws_default',
                 s3_bucket=None,
                 s3_prefix=None,
                 s3_delimiter='/',
                 mysql_conn_id='mysql_default',
                 mysql_database=None,
                 mysql_table=None,
                 mysql_infields=[],
                 mysql_inseps=(',', r'\"', r'\n'),
                 *argv, **kwargs):
        super(S3ToMySqlLoadOperator, self).__init__(*argv, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_delimiter = s3_delimiter

        self.mysql_conn_id = mysql_conn_id
        self.mysql_database = mysql_database
        self.mysql_table = mysql_table
        self.mysql_infields = mysql_infields
        self.mysql_inseps = mysql_inseps

    def execute(self, context):
        mysql_infields = ','.join('`{}`'.format(infield)
                                  for infield in self.mysql_infields)
        self.log.info('MySQL fields: %s', mysql_infields)

        s3_hook = S3Hook(self.aws_conn_id)
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        self.log.info('Listing files in s3://%s/%s', self.s3_bucket, self.s3_prefix)
        s3_infiles = s3_hook.list_keys(self.s3_bucket,
                                       prefix=self.s3_prefix,
                                       delimiter=self.s3_delimiter)
        if not s3_infiles:
            raise RuntimeError('no file to process')

        with TemporaryDirectory(prefix='airflow_mysqlloadop_') as tmp_dir:
            with NamedTemporaryFile('ab', dir=tmp_dir, delete=False) as tmp:
                for s3_infile in s3_infiles:
                    self.log.info('Download s3://%s/%s', self.s3_bucket, s3_infile)

                    s3_obj = s3_hook.get_key(s3_infile, self.s3_bucket)
                    if s3_obj.content_type == 'application/x-directory':
                        self.log.info('Skip directory: s3://%s/%s',
                                      self.s3_bucket, s3_infile)
                        continue

                    s3_obj.download_fileobj(tmp)

                mysql_infile = tmp.name

            self.log.info('MySQL infile: %s', mysql_infile)

            mysql_sql_fmt = '''
                LOAD DATA LOCAL INFILE '{file}'
                INTO TABLE `{database}`.`{table}`
                FIELDS TERMINATED BY '{seps[0]}'
                ENCLOSED BY '{seps[1]}'
                LINES TERMINATED BY '{seps[2]}'
                ({fields})
                ;
            '''
            mysql_sql = mysql_sql_fmt.format(file=mysql_infile,
                                             database=self.mysql_database,
                                             table=self.mysql_table,
                                             seps=self.mysql_inseps,
                                             fields=mysql_infields)

            self.log.info('Execute SQL')
            mysql_hook.run(mysql_sql)


class MySqlLoadPlugin(AirflowPlugin):
    name = "mysqlload_plugin"
    hooks = []
    operators = [S3ToMySqlLoadOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
