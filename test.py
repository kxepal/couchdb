#!/usr/bin/env python
# coding: utf-8
import gzip
import logging
import tempfile
import os
from airflow.operators import BaseOperator
from umworld.util import hdfs_lib
from dmp.air.operators import RunHQLQueryOperator
from dmp.utils.logging_subprocess_call import call
from dmp.utils.hadoop import fs_to_hdfs_file
from dmp import defaults, exceptions


class FillDitExportTable(RunHQLQueryOperator):
    top_n = 10
    dit_resourceid = defaults.DIT_PARTNER_ID
    forbidden_themes = [214, 321, 346, 351, 355, 356, 370]
    hql = [
        "SET hive.map.aggr = false",
        "ADD JAR /usr/local/hive/lib/facebook-udfs-1.0.3.jar",
        "CREATE TEMPORARY FUNCTION array_slice AS 'com.facebook.hive.udf.UDFArraySlice'",
        "CREATE TEMPORARY FUNCTION collect AS 'com.facebook.hive.udf.UDAFCollect'",

        """
        FROM (
            SELECT
                if (A.partner_uid IS NOT NULL, A.partner_uid, B.partner_uid) as partner_uid,
                array_slice(A.hostnames, 0, {top_n}) as top_hostnames,
                array_slice(B.themes, 0, {top_n}) as top_themes

            FROM (
                SELECT X.partner_uid, collect(X.hostname) as hostnames from (
                    SELECT
                      s.partner_uid AS partner_uid,
                      if(f.hostname LIKE "%.livejournal.com", "livejournal.com", f.hostname) AS hostname,
                      sum(f.pageviews) AS pageviews_

                    FROM factor_resourceid_hostname_month f
                    JOIN dmp_uid_sync s ON  (s.ruid = f.ruid)
                    WHERE
                        s.day = '{ds}'
                        AND f.day = '{ds}'
                        AND s.partner = '{dit_resourceid}'
                        AND f.resourceid NOT IN (
                            SELECT resourceid FROM top100_resources_themes WHERE themeid IN ({forbidden_themes})
                        )

                    GROUP BY
                        s.partner_uid,
                        if(f.hostname LIKE "%.livejournal.com", "livejournal.com", f.hostname)

                    DISTRIBUTE BY s.partner_uid
                    SORT BY s.partner_uid, pageviews_ DESC
                ) X
                GROUP BY X.partner_uid
            ) A

            FULL OUTER JOIN (
                SELECT Y.partner_uid, collect(Y.theme) AS themes from (
                    SELECT
                        s.partner_uid AS partner_uid,
                        t.themeid AS theme,
                        sum(f.pageviews) AS pageviews_
                    FROM factor_resourceid_hostname_month f
                    JOIN dmp_uid_sync s ON (s.ruid = f.ruid)
                    JOIN top100_resources_themes t ON (t.resourceid = f.resourceid)
                    WHERE
                        s.day = '{ds}'
                        AND f.day = '{ds}'
                        AND s.partner = '{dit_resourceid}'
                        AND t.themeid NOT IN ({forbidden_themes})
                    GROUP BY s.partner_uid, t.themeid
                    DISTRIBUTE BY s.partner_uid
                    SORT BY s.partner_uid, pageviews_ DESC
                ) Y
                GROUP BY Y.partner_uid
            ) B

            ON A.partner_uid = B.partner_uid
        ) SRC
        INSERT OVERWRITE TABLE dit_export_data PARTITION(day='{ds}')
        SELECT partner_uid, top_hostnames, top_themes
        """
    ]

    def prepare_hql_args(self, context):
        self.hql_args = {
            'ds': context['ds'],
            'forbidden_themes': ', '.join([str(t) for t in self.forbidden_themes]),
            'dit_resourceid': self.dit_resourceid,
            'top_n': self.top_n,
        }


class ExportDITDataToHDFS(RunHQLQueryOperator):
    def __init__(self, dst_dir, *args, **kwargs):
        self.dst_dir = dst_dir
        super(ExportDITDataToHDFS, self).__init__(*args, **kwargs)

    hql = [
        """
        INSERT OVERWRITE DIRECTORY "{dst_dir}"
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' COLLECTION ITEMS TERMINATED BY ',' LINES TERMINATED BY '\n' NULL DEFINED AS ''
            SELECT partner_uid, top_hostnames, top_themes FROM dit_export_data
            WHERE day='{ds}'
        """
    ]

    def prepare_hql_args(self, context):
        self.hql_args = {
            'ds': context['ds'],
            'dst_dir': self.dst_dir.format(**context),
        }


class HDFSMergeAndMove(BaseOperator):
    def __init__(self, src_dir, dst_file, *args, **kwargs):
        self.src_dir = src_dir
        self.dst_file = dst_file
        super(HDFSMergeAndMove, self).__init__(*args, **kwargs)

    def execute(self, context):
        src_dir = self.src_dir.format(**context)
        dst_file = self.dst_file.format(**context)

        with tempfile.NamedTemporaryFile() as tmp_file:
            tmp_path = tmp_file.name

            logging.info('Creating file %s ...', tmp_path)
            call('hadoop fs -text {src_dir}/* 1>{tmp_path}'
                 ''.format(src_dir=src_dir, tmp_path=tmp_path))

            logging.info('Ok. Creating archive %s ...', tmp_path)
            tmp_gzpath = tmp_path + '.gz'
            with gzip.open(tmp_gzpath, 'wb') as gz_file:
                # Assume our files are small enought
                gz_file.write(tmp_file.read())
                logging.info('Ok. Created file %s.', tmp_gzpath)

            logging.info('Moving %s to HDFS ...', dst_file)
            fs_to_hdfs_file(defaults.FETCH_LOGS_TMP_PATH,
                            tmp_gzpath,
                            os.path.dirname(dst_file),
                            os.path.basename(dst_file))
            logging.info('Done.')


class ExportQIWIDataToHDFS(RunHQLQueryOperator):
    hql = [
        """
        FROM (
            SELECT partner_uid, collect_set(partner_rubric) as rubric_set
            FROM (
                -- rubrics
                SELECT
                    partner_uid,
                    value
                FROM idsync_profile
                WHERE partner='qiwi'
                    AND day = '{ds}'
                    AND target = 'rubric'

                UNION

                -- predicted age
                SELECT
                    d.partner_uid as partner_uid,
                    p.class as value

                FROM predicted_targets_age_big_group p

                JOIN  dmp_uid_sync d on (
                    d.ruid = p.ruid
                    AND d.day = '{ds}'
                    AND p.day = '{ds}'
                    AND d.ns  = 'idsync'
                    AND d.partner = 'qiwi'
                )

                UNION

                -- predicted gender
                SELECT
                    d.partner_uid as partner_uid,
                    p.class as value

                FROM predicted_targets_sex p

                JOIN  dmp_uid_sync d on (
                    d.ruid = p.ruid
                    AND d.day = '{ds}'
                    AND p.day = '{ds}'
                    AND d.ns  = 'idsync'
                    AND d.partner = 'qiwi'
                )
            ) T

            JOIN (
                SELECT
                    ruid_rubric,
                    partner_rubric
                FROM  idsync_rubrics
                WHERE partner = 'qiwi'
            ) tmp_rubric_table

            ON (T.value = tmp_rubric_table.ruid_rubric)
            GROUP BY partner_uid
        ) SRC

        INSERT OVERWRITE DIRECTORY "{dst_dir}"
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY '\;' LINES TERMINATED BY '\n' NULL DEFINED AS ''
            SELECT SRC.partner_uid, SRC.rubric_set
        """
    ]

    def __init__(self, dst_dir, *args, **kwargs):
        self.dst_dir = dst_dir
        super(ExportQIWIDataToHDFS, self).__init__(*args, **kwargs)

    def prepare_hql_args(self, context):
        self.hql_args = {
            'ds': context['ds'],
            'dst_dir': self.dst_dir.format(**context),
        }


class RemoveFromHdfs(BaseOperator):
    def __init__(self, path, *args, **kwargs):
        self.path_to_remove = path
        super(RemoveFromHdfs, self).__init__(*args, **kwargs)

    def execute(self, context):
        path = self.path_to_remove.format(**context)

        logging.info('Removing {}'.format(path))
        if not hdfs_lib.exists(path):
            logging.info('Nothing to remove')
            return

        hdfs_lib.rmr(path)
        logging.info('Removed')
