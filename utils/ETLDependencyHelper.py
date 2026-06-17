import logging
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


class ETLDependencyHelper:
    def __init__(
            self,
            host,
            user,
            password,
            port,
            db,
            log_table="etl_table_dependency_check_log"
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db
        self.log_table = log_table

        try:
            self.engine = create_engine(f"mysql+pymysql://{user}:%s@{host}:{port}/{db}" % quote_plus(password))
            self.conn = self.engine.connect()
            logging.info("Database connection established")
        except Exception as e:
            logging.exception("Failed to connect to MySQL")
            raise

    def close(self):
        try:
            self.conn.close()
            self.engine.dispose()
            logging.info("Database connection closed")
        except Exception as e:
            logging.exception("Failed to close database connection")

    def get_uow_value(self, tgt_uow_id):
        try:
            query = text('''
                SELECT
                    uow_value
                FROM data_center.etl_uow_config
                WHERE uow_id = :tgt_uow_id
            ''')

            df = pd.read_sql_query(query, self.conn, params={"tgt_uow_id": tgt_uow_id})

            if df.empty:
                logging.warning(f"No uow_value found for {tgt_uow_id}")
                raise ValueError(f"No uow_value found for {tgt_uow_id}")

            return df["uow_value"].iat[0]

        except Exception:
            logging.exception(f"Failed getting uow_value for {tgt_uow_id}")
            raise

    def check_max_retry(self, tgt_uow_id):
        try:
            query = text('''
                    SELECT
                        MAX(max_retry) AS retry_times
                    FROM data_center.etl_table_dependency_config
                    WHERE tgt_uow_id = :tgt_uow_id
            ''')

            df = pd.read_sql_query(query, self.conn, params={"tgt_uow_id": tgt_uow_id})

            if df.empty:
                raise ValueError(f'No UOW ID found in dependency config for {tgt_uow_id}')

            return df["retry_times"].iat[0]

        except Exception:
            logging.exception(f"Failed checking max retry for {tgt_uow_id}")
            raise

    def check_src_ready(self, tgt_uow_id, src_offset_steps_in_mins=None):
        query = text("""
            SELECT
                dep.tgt_uow_id,
                tgt_cfg.uow_value AS tgt_uow_value,
                dep.src_uow_id,
                src_cfg.uow_value AS src_uow_value,
                TIMESTAMPADD(
                    MINUTE,
                    dep.src_offset_steps_in_mins,
                    tgt_cfg.uow_value
                ) AS expect_src_uow_value,
                CASE
                    WHEN dep.enable_flag = 1
                     AND TIMESTAMPDIFF(
                            MINUTE,
                            tgt_cfg.uow_value,
                            src_cfg.uow_value
                         ) >= COALESCE(
                                :src_offset_steps_in_mins,
                                dep.src_offset_steps_in_mins
                            )
                    THEN 1
                    ELSE 0
                END AS ready_flag,
                1 AS retry_times,
                CURRENT_TIMESTAMP AS cre_ts,
                NULL AS upd_ts
            FROM data_center.etl_table_dependency_config dep
            INNER JOIN data_center.etl_uow_config tgt_cfg
                ON dep.tgt_uow_id = tgt_cfg.uow_id
            LEFT JOIN data_center.etl_uow_config src_cfg
                ON dep.src_uow_id = src_cfg.uow_id
            WHERE dep.tgt_uow_id = :tgt_uow_id
              AND dep.enable_flag = 1
        """)

        try:
            df = pd.read_sql_query(query, self.conn, params={"tgt_uow_id": tgt_uow_id, "src_offset_steps_in_mins": src_offset_steps_in_mins})
            ready_flag = 0 if 0 in list(df["ready_flag"]) else 1
            return ready_flag, df

        except Exception:
            logging.exception(f"Failed checking dependency for {tgt_uow_id}")
            raise

    def update_check_log(self, df):
        try:
            if df.empty:
                logging.warning("No records to insert into check log")
                return

            df.to_sql(
                name=self.log_table,
                schema=self.db,
                con=self.conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

            self.conn.commit()

            logging.info(f"{len(df)} rows inserted into {self.log_table}")

        except Exception:
            logging.exception("Failed inserting dependency check log")
            raise

    def continue_check(self, tgt_uow_id, max_retry_reached_flag):
        try:
            continue_check_flag = 1
            ready_flag = 0

            if max_retry_reached_flag:
                logging.warning(f"Max retry reached for {tgt_uow_id}")
                return 0, 0

            ready_flag, df = self.check_src_ready(tgt_uow_id)

            if ready_flag:
                return 1, 0

            self.update_check_log(df)
            return 0, 1

        except Exception:
            logging.exception(f"Failed continue_check for {tgt_uow_id}")
            raise

    def update_uow_value(self, uow_id, uow_value=None, src_offset_steps_in_mins=None):
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text('''
                        UPDATE data_center.etl_uow_config
                        SET
                            uow_value =
                                TIMESTAMPADD(
                                    MINUTE,
                                    COALESCE(:src_offset_steps_in_mins, incremental_steps_in_mins),
                                    COALESCE(:uow_value, uow_value)
                                ),
                            upd_ts = CURRENT_TIMESTAMP
                        WHERE uow_id = :uow_id
                    '''),
                    {
                        "uow_id": uow_id,
                        "uow_value": uow_value,
                        "src_offset_steps_in_mins": src_offset_steps_in_mins
                    }
                )
            logging.info(f"Updated uow_id={uow_id}, uow_value={uow_value}, src_offset_steps_in_mins={src_offset_steps_in_mins}")

        except SQLAlchemyError:
            logging.exception(f"Failed updating uow_id={uow_id}")
            raise
