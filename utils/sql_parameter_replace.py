def insert_accounting_t_1_equity_report_delta(lnd_schema, tst_schema, snowflake_cur, dt_report, LOOKBACK_INTERVAL,
                                              tenant, servers_exclusion, crm_server_id_demo, crm_server_id_wallet,
                                              crm_server_id_iris, sub_ib_trn_type):
    if tenant.lower() == 'ttg':
        query = """
            INSERT INTO {4}_DATAWAREHOUSE.{2}.accounting_t_1_equity_report_delta
                WITH rsuh AS 
                (   
                    SELECT 
                        * 
                    FROM 
                    (
                    SELECT DISTINCT
                        IFF(MINUTE(rtz) >= 30,
                            DATE_TRUNC('HOUR', rtz) + INTERVAL '30 MINUTE',
                            DATE_TRUNC('HOUR', rtz) ) AS dt_report
                        ,*
                        ,ROW_NUMBER() OVER(PARTITION BY server,login order by utc_now desc) AS rn
                    FROM (
                        SELECT
                            *
                            ,TO_RTZ(rsuh.utc_now)  AS rtz
                        FROM {4}_DATAWAREHOUSE.{1}.report_server_users_history AS rsuh
                        WHERE server NOT IN {5}
                            AND DATE(CONCAT(CAST(rsuh.year AS VARCHAR),'-',CAST(rsuh.month AS VARCHAR),'-',CAST(rsuh.day AS VARCHAR)))
                                >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 DAY', 'yyyy-mm-dd'))
                            AND DATE(CONCAT(CAST(rsuh.year AS VARCHAR),'-',CAST(rsuh.month AS VARCHAR),'-',CAST(rsuh.day AS VARCHAR)))
                                <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    )
                    WHERE IFF(MINUTE(rtz) >= 30,
                        DATE_TRUNC('HOUR', rtz) + INTERVAL '30 MINUTE',
                        DATE_TRUNC('HOUR', rtz) ) <= DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP))
                )
                WHERE rn =1
                    AND login NOT IN (728888, 718888)
                    AND LOWER("GROUP") NOT LIKE '%test%'
                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT IN ('system', 'datacenter', 'oz-cbook-pr-u')
                    AND LOWER("GROUP") NOT LIKE '%manager%'
                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT LIKE 'hf%'
                    AND LOWER("GROUP") NOT LIKE '%mam%'
                    AND comment NOT LIKE '%ALLOC=%'
                    AND LOWER(comment) NOT LIKE '%test%'
                    AND LOWER(comment) NOT LIKE '%ia:0;%'
                    AND LOWER(comment) NOT LIKE '%trademax hedge%'
                    AND LOWER(comment) NOT LIKE '%(gcgau)%'
                    AND LOWER(comment) NOT LIKE '%[webapi]%'
                    AND LOWER(comment) NOT LIKE '%tst%'
                    AND name NOT LIKE '%test%'
            ),rsuh_recent AS
            (
                SELECT 
                    name
                    ,server
                    ,login
                    ,crm_server_id as rs_type
                FROM 
                    (
                        SELECT 
                            rsuh.*,
                            ROW_NUMBER() OVER(PARTITION BY rsuh.server, rsuh.login order by rsuh.utc_now desc) AS rn,
                            cs.crm_server_id
                        FROM {4}_DATAWAREHOUSE.{1}.report_server_users_history AS rsuh
                        LEFT join {4}_DATAWAREHOUSE.{2}.crm_servers AS cs
                            ON rsuh.server=cs.server
                        WHERE rsuh.server NOT IN {5}
                                    AND login NOT IN (728888, 718888)
                                    AND LOWER("GROUP") NOT LIKE '%test%'
                                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT IN ('system', 'datacenter', 'oz-cbook-pr-u')
                                    AND LOWER("GROUP") NOT LIKE '%manager%'
                                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT LIKE 'hf%'
                                    AND comment NOT LIKE '%ALLOC=%'
                                    AND LOWER(comment) NOT LIKE '%test%'
                                    AND LOWER(comment) NOT LIKE '%ia:0;%'
                                    AND LOWER(comment) NOT LIKE '%trademax hedge%'
                                    AND LOWER(comment) NOT LIKE '%(gcgau)%'
                                    AND LOWER(comment) NOT LIKE '%[webapi]%'
                                    AND LOWER(comment) NOT LIKE '%tst%'
                                    AND name NOT LIKE '%test%'
                                    AND lower(name) like '%tm%'
                                    AND (lower(name) like '%iress%' OR lower(name) like '%insight%')
                    )
                WHERE rn =1
            ),
            rsu AS (
                SELECT *
                FROM {4}_DATAWAREHOUSE.{1}.report_server_users
                WHERE server NOT IN {5}
                        AND login NOT IN (728888, 718888)
                        AND LOWER("GROUP") NOT LIKE '%test%'
                        AND SPLIT_PART("GROUP", '\\\\', -1) NOT IN ('system', 'datacenter', 'oz-cbook-pr-u')
                        AND LOWER("GROUP") NOT LIKE '%manager%'
                        AND SPLIT_PART("GROUP", '\\\\', -1) NOT LIKE 'hf%'
                        AND LOWER(comment) NOT LIKE '%test%'
                        AND LOWER(comment) NOT LIKE '%ia:0;%'
                        AND LOWER(comment) NOT LIKE '%trademax hedge%'
                        AND LOWER(comment) NOT LIKE '%(gcgau)%'
                        AND LOWER(comment) NOT LIKE '%[webapi]%'
                        AND LOWER(comment) NOT LIKE '%tst%'
                        AND name NOT LIKE '%test%'
            ),
            rstt AS (
                SELECT
                    DISTINCT *
                FROM (
                    SELECT
                            *
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn1
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_transactions
                    WHERE server NOT IN {5}
                        AND DATE(concat(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                            >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                        AND DATE(concat(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                            <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                            AND open_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                                AND DATE(CAST('{3}' AS TIMESTAMP))
                    ) AS rstt
                WHERE rn1 = 1
            ),ct AS 
            (
                SELECT 
                    *
                FROM
                    (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at desc) AS rn
                        FROM {4}_DATAWAREHOUSE.{1}.crm_transactions 
                        WHERE external_id like '%W%'
                            AND status =2 
                            AND deleted_at is null
                    )
                WHERE rn =1
            ),
            rs_trades_open AS (
                SELECT *
                FROM (
                    SELECT
                            server,login,ticket,symbol,cmd,volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_open
                    WHERE server NOT IN {5}
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR))) >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR))) <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    AND open_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                            AND date(CAST('{3}' AS TIMESTAMP))
                )
                WHERE rn = 1
            ),
            rs_trades_open_history AS (
                SELECT
                    server,login,ticket,symbol,cmd,volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time, 1
                FROM (
                    SELECT
                            *
                    FROM (
                        SELECT *
                            ,TO_RTZ(rsoh.utc_now) AS rtz
                        FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_open_history AS rsoh
                        WHERE server NOT IN {5}
                            AND DATE(CONCAT(CAST(rsoh.year AS VARCHAR),'-',CAST(rsoh.month AS VARCHAR),'-',CAST(rsoh.day AS VARCHAR)))
                                >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                            AND DATE(CONCAT(CAST(rsoh.year AS VARCHAR),'-',CAST(rsoh.month AS VARCHAR),'-',CAST(rsoh.day AS VARCHAR)))
                                <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    )
                )
                WHERE IFF(MINUTE(rtz) >= 30,
                        DATE_TRUNC('HOUR', rtz) + INTERVAL '30 MINUTE',
                        DATE_TRUNC('HOUR', rtz) ) = DATE_TRUNC('HOUR', CAST('{3}' AS TIMESTAMP))
            ),
            rs_trades_closed AS (
                SELECT *
                FROM (
                    SELECT
                            server,login,ticket,symbol,cmd,volume AS volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_closed
                    WHERE server NOT IN {5}
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                        >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                        <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    AND concat(CAST(ticket AS VARCHAR),server) not in (
                        SELECT concat(CAST(ticket AS VARCHAR),server) 
                            FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_open
                            WHERE server not in {5}
                                AND ticket IS NOT NULL
                        )
                )
                WHERE rn = 1
                    AND close_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                    AND DATE(CAST('{3}' AS TIMESTAMP))
            ),
            rs_trades_pending AS (
                SELECT *
                FROM (
                    SELECT
                            server,login,ticket,symbol,cmd,volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_pending_history
                    WHERE server NOT IN {5}
                    AND open_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                                AND date(CAST('{3}' AS TIMESTAMP))
                )
                WHERE rn = 1
            ),users_history AS (
                SELECT *
                FROM (
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY rtz_now DESC) as rnu
                    FROM {4}_DATAWAREHOUSE.{1}.crm_users_history
                    WHERE rtz_now < CAST('{3}' AS TIMESTAMP) + INTERVAL '20 MINUTES'
                )
                WHERE rnu=1
            ),
            users AS (
                SELECT cu.*
                    ,CASE
                        WHEN UPPER(cu.country) = ' OWITIWARAYA' THEN 'OWITIWARAYA'
                        WHEN UPPER(cu.country) = 'AU' THEN 'AUSTRALIA'
                        WHEN UPPER(cu.country) = 'DOMINICA' THEN 'DOMINICAN REPUBLIC'
                        WHEN UPPER(cu.country) = 'HONG KONG S.A.R.' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'HONG KONG SAR CHINA' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'HONGKONG' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'HONGKONG SAR CHINA' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'MYANMAR [BURMA]' THEN 'MYANMAR'
                        WHEN UPPER(cu.country) = 'UK' THEN 'UNITED KINGDOM'
                        WHEN UPPER(cu.country) = 'USA' THEN 'UNITED STATES'
                        WHEN UPPER(cu.country) = 'EN' THEN 'UNKNOWN OR INVALID REGION'
                        WHEN UPPER(cu.country) = 'SEF' THEN 'UNKNOWN OR INVALID REGION'
                        WHEN UPPER(cu.country) = 'TW' THEN 'TAIWAN'
                        WHEN UPPER(cu.country) = '中國' THEN 'CHINA'
                        WHEN UPPER(cu.country) = '中国' THEN 'CHINA'
                        ELSE (CASE WHEN UPPER(regexp_replace(cu.country, '\\\d','')) = ''
                            THEN 'UNKNOWN OR INVALID REGION'
                            ELSE UPPER(regexp_replace(cu.country, '\\\d',''))
                            END)
                        END AS adj_country
                    ,cmahl.name AS member_acct_type
                    ,aerj.accounting_equity_report_jurisdiction AS report_jurisdiction
                FROM(
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) as rn
                    FROM users_history
                ) AS cu
                LEFT JOIN (
                    SELECT *
                    FROM {4}_DATAWAREHOUSE.{1}.crm_member_account_has_languages
                    WHERE language = 'en'
                ) AS cmahl
                ON cu.member_account_type = cmahl.member_account_type_id
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.accounting_equity_report_jurisdiction AS aerj
                ON cu.jurisdiction = aerj.user_jurisdiction
                WHERE rn=1
            ),
            symbol_groups AS (
                SELECT
                    symbol
                    ,symbol_group
                    ,server
                FROM(
                    SELECT
                        crm_external_symbols.name AS symbol
                        , crm_external_symbol_groups.name AS symbol_group
                        ,crm_servers.server
                        , ROW_NUMBER() OVER(PARTITION BY crm_external_symbols.name, crm_external_symbols.trading_server_id ORDER BY crm_external_symbols.updated_at DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.crm_external_symbols
                    LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers
                        ON crm_external_symbols.trading_server_id=crm_servers.crm_server_id
                    LEFT JOIN {4}_DATAWAREHOUSE.{1}.crm_external_symbol_groups
                    ON crm_external_symbol_groups.id = crm_external_symbols.external_symbol_group_id
                )
                WHERE rn=1
            ),trading_accounts_history AS (
            SELECT *
            FROM
            (
                SELECT crm_trading_accounts.*
                    , ROW_NUMBER() OVER(PARTITION BY crm_trading_accounts.id ORDER BY crm_trading_accounts.rtz_now DESC) as rn_tah
                FROM {1}.crm_trading_accounts_history AS crm_trading_accounts
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers cs
                    ON cs.crm_server_id = crm_trading_accounts.type
                WHERE (lower(cs.type) LIKE '%live%' or lower(cs.type) LIKE '%internal%') --TMGM live servers and CRM internal
                AND crm_trading_accounts.rtz_now < CAST('{3}' AS TIMESTAMP) + INTERVAL '20 MINUTES'
            )
            WHERE rn_tah=1
            ),
            trading_accounts AS (
                SELECT cta.*
                    ,cs.server
                    ,CASE
                        WHEN cta.is_ib = 1 THEN 'IB'
                        WHEN cta.parent_ta_id IS NOT NULL OR cta.parent_ta_id != 0
                            THEN 'Clients With IB'
                        WHEN cta.parent_ta_id IS NULL OR cta.parent_ta_id = 0
                            THEN 'Retail Client'
                        END AS member_type
                FROM(
                    SELECT
                        ta.*
                        , crm_serv.name AS trading_server_name
                        , crm_serv.id AS trading_server_id
                        , ROW_NUMBER() OVER(PARTITION BY ta.id ORDER BY ta.updated_at DESC) as rn
                    FROM trading_accounts_history ta
                    LEFT JOIN {4}_DATAWAREHOUSE.{1}.crm_trading_servers crm_serv
                    ON ta.type = crm_serv.id
                    WHERE ta.type NOT IN ({6})
                        AND ta.deleted_at IS NULL
                ) AS cta
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers AS cs
                ON cta.type = cs.crm_server_id
                WHERE rn=1
            ),
            trades_volume AS (
                SELECT 
                    server
                    ,login
                    ,ROUND(SUM(swaps), 2) AS swaps
                    ,ROUND(SUM(commission), 2) AS commissions
                    ,ROUND(SUM(profit), 2) AS profit
                    ,ROUND(SUM(swaps+commission+profit), 2) AS sum_pnl
                    ,COALESCE(SUM(fx_vol) / 100.0,0) AS forex_volume
                    ,COALESCE(SUM(bullion_vol) / 100.0,0) AS bullion_volume
                    ,COALESCE(SUM(oil_vol) / 100.0,0) AS oil_volume
                    ,COALESCE(SUM(cfd_vol) / 100.0,0) AS cfd_volume
                    ,COALESCE(SUM(crypto_vol) / 100.0,0) AS crypto_volume
                    ,COALESCE(SUM(fx_exotics_vol) / 100.0,0) AS fx_exotics_vol
                FROM
                (
                    SELECT

                        server
                        ,login
                        ,CASE WHEN LOWER(symbol_group) = 'forex'
                                THEN volume
                        END AS fx_vol
                        ,CASE WHEN LOWER(symbol_group) = 'bullion'
                                THEN volume
                        END AS bullion_vol
                        ,CASE WHEN LOWER(symbol_group) = 'oil'
                                THEN volume
                        END AS oil_vol
                        ,CASE WHEN LOWER(symbol_group) = 'cfd'
                                THEN volume
                        END AS cfd_vol
                        ,CASE WHEN LOWER(symbol_group) = 'crypto'
                                THEN volume
                        END AS crypto_vol
                        ,CASE WHEN LOWER(symbol_group) = 'fx exotics'
                                THEN volume
                        END AS fx_exotics_vol
                        ,swaps
                        ,commission
                        ,profit
                    FROM
                        (
                            SELECT
                                rst.*
                                ,symbol_group
                            FROM rs_trades_closed AS rst
                            LEFT JOIN rs_trades_open AS rso
                            ON rst.server = rso.server
                                AND rst.ticket = rso.ticket
                            LEFT JOIN symbol_groups
                            ON symbol_groups.symbol = rst.symbol
                                AND symbol_groups.server=rst.server
                            WHERE rso.server IS NULL
                        )
                )
                GROUP BY  server,login
            ),
            orders_count AS (
                SELECT
                        server
                    ,login
                    ,COUNT(count_open_orders) AS count_open_orders
                    ,COUNT(count_pending_orders) AS count_pending_orders
                    ,COUNT(count_closed_orders) AS count_closed_orders
                    ,SUM(time_diff)/COUNT(count_closed_orders) AS average_trade_duration
                FROM
                (
                    SELECT
                        DISTINCT
                                server
                            ,login
                            ,CASE WHEN close_time = CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 0 OR cmd = 1)
                                    THEN ticket
                            END AS count_open_orders
                            ,CASE WHEN close_time = CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 2 OR cmd = 3 OR cmd = 4 OR cmd = 5)
                                    THEN ticket
                            END AS count_pending_orders
                            ,CASE WHEN close_time != CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 0 OR cmd = 1 OR cmd = 6 OR cmd = 7)
                                    THEN ticket
                            END AS count_closed_orders
                            ,CASE WHEN close_time != CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 0 OR cmd = 1)
                                    THEN DATEDIFF('seconds', open_time, close_time)
                            END AS time_diff
                    FROM
                    (
                        SELECT * FROM (
                            SELECT
                                *
                                ,ROW_NUMBER() OVER(PARTITION BY server, login, ticket ORDER BY modify_time DESC) AS rn_all
                            FROM (
                                SELECT * FROM rs_trades_open
                                UNION ALL
                                SELECT * FROM rs_trades_closed
                                UNION ALL
                                SELECT * FROM rs_trades_pending
                            )
                        )
                        WHERE rn_all = 1
                    )
                )
                GROUP BY server,login
            ),
            open_orders_count AS (
                SELECT server, login, count(distinct ticket) AS open_order_count
                FROM rs_trades_open_history
                GROUP BY server, login
            ),cu AS (
                    SELECT *
                    FROM (
                        SELECT
                            *
                            ,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS RN
                        FROM users_history
                    )
                    WHERE RN = 1
                ),ctran AS 
                    (
                        SELECT 
                            *
                        FROM
                            (
                                SELECT 
                                    ct.*,
                                    cu.sales_code AS user_salescode,
                                    ROW_NUMBER() OVER(PARTITION BY ct.id ORDER BY ct.updated_at desc) AS rn
                                FROM {4}_DATAWAREHOUSE.{1}.crm_transactions AS ct
                                LEFT JOIN cu 
                                    ON ct.user_id=cu.id
                                WHERE external_id like '%W%'
                                    AND ct.status =2 
                                    AND ct.deleted_at is null
                                    AND DATE(ct.completed_at) >=  IFF(DAY(CAST('{3}' AS TIMESTAMP)) = 1,
                                                                                        DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY',
                                                                                        DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY')
                                    AND DATE(ct.completed_at) < DATE(cast('{3}' as timestamp))
                            )
                        WHERE rn =1
                            AND lower(user_salescode) NOT LIKE '%9999%'
                            AND lower(user_salescode) NOT LIKE '%test%'
                    ),wallet_transactions AS (
                        SELECT
                                server
                            ,login
                            ,COALESCE(ROUND(SUM(sales_deposit),2),0) AS sales_deposit
                            ,COALESCE(ROUND(SUM(sales_withdrawal),2),0) AS sales_withdrawal
                            ,COALESCE(ROUND(SUM(sales_net_deposit),2),0) AS sales_net_deposit
                            ,COALESCE(ROUND(SUM(deposit),2),0) AS deposit
                            ,COALESCE(ROUND(SUM(withdrawal),2),0) AS withdrawal
                            ,COALESCE(ROUND(SUM(net_deposit),2),0) AS net_deposit
                            ,COALESCE(ROUND(SUM(credit_change),2),0) AS credit_change
                            ,COALESCE(ROUND(SUM(internal_transfer),2),0) AS internal_transfer
                            ,COALESCE(ROUND(SUM(internal_transfer_deposit_from_ib),2),0) AS internal_transfer_deposit_from_ib
                            ,COALESCE(ROUND(SUM(internal_transfer_withdrawal_to_ib),2),0) AS internal_transfer_withdrawal_to_ib
                            ,COALESCE(ROUND(SUM(internal_transfer_from_same_category),2),0) AS internal_transfer_from_same_category
                            ,COALESCE(ROUND(SUM(internal_transfer_to_same_category),2),0) AS internal_transfer_to_same_category
                            ,COALESCE(ROUND(SUM(credit),2),0) AS credit
                            ,COALESCE(ROUND(SUM(points),2),0) AS points
                            ,COALESCE(ROUND(SUM(CFD),2),0) AS CFD
                            ,COALESCE(ROUND(SUM(rollover),2),0) AS rollover
                            ,COALESCE(ROUND(SUM(MISC_OP),2),0) AS MISC_OP
                            ,COALESCE(ROUND(SUM("SPLIT"),2),0) AS "SPLIT"
                            ,COALESCE(ROUND(SUM(Compensation),2),0) AS Compensation
                            ,COALESCE(ROUND(SUM(MISC_Clear),2),0) AS MISC_Clear
                            ,COALESCE(ROUND(SUM(MISC_TR),2),0) AS MISC_TR
                            ,COALESCE(ROUND(SUM(MISC_FUNDING),2),0) AS MISC_FUNDING
                            ,COALESCE(ROUND(SUM(promo),2),0) AS promo
                            ,COALESCE(ROUND(SUM(ib_rebate),2),0) AS ib_rebate
                            ,COALESCE(ROUND(SUM(auto_rebate),2),0) AS auto_rebate
                            ,COALESCE(ROUND(SUM(CPA_rebate),2),0) AS CPA_rebate
                            ,COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                                + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_rebate
                            ,COALESCE(ROUND(SUM(net_deposit),2),0) + COALESCE(ROUND(SUM(internal_transfer),2),0)
                                + COALESCE(ROUND(SUM(credit),2),0) + COALESCE(ROUND(SUM(points),2),0)
                                + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(rollover),2),0)
                                + COALESCE(ROUND(SUM(MISC_OP),2),0) + COALESCE(ROUND(SUM("SPLIT"),2),0)
                                -- + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(compensation),2),0)
                                + COALESCE(ROUND(SUM(compensation),2),0)
                                + COALESCE(ROUND(SUM(MISC_Clear),2),0) + COALESCE(ROUND(SUM(MISC_TR),2),0)
                                + COALESCE(ROUND(SUM(MISC_FUNDING),2),0) + COALESCE(ROUND(SUM(promo),2),0)
                                + COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                                + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_txn
                        FROM
                        (
                            SELECT
                                    'wallet' AS server
                                ,external_id AS login
                                ,CASE WHEN type in (1)
                                        THEN fund_amount
                                    END AS sales_deposit
                                ,CASE WHEN type in (2)
                                        THEN fund_amount
                                    END AS sales_withdrawal
                                ,CASE WHEN type in (1,2)
                                        THEN fund_amount
                                    END AS sales_net_deposit
                                ,CASE WHEN type in (1) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) != 'Internal_Transfer'
                                        THEN fund_amount
                                    END AS deposit
                                ,CASE WHEN type in (2) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) != 'Internal_Transfer'
                                        THEN fund_amount
                                    END AS withdrawal
                                ,CASE WHEN type in (1,2) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) != 'Internal_Transfer'
                                        THEN fund_amount
                                    END AS net_deposit
                                ,CASE WHEN type = 4
                                        THEN fund_amount
                                    END AS credit_change
                                ,CASE WHEN type in (3,{9})
                                        THEN fund_amount
                                    END AS internal_transfer
                                ,CASE WHEN type in (3,{9}) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_DEPOSIT_FROM_IB'
                                        THEN fund_amount
                                    END AS internal_transfer_deposit_from_ib
                                ,CASE WHEN type in (3,{9}) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_WITHDRAWAL_TO_IB'
                                        THEN fund_amount
                                    END AS internal_transfer_withdrawal_to_ib
                                ,CASE WHEN type in (3) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_FROM_SAME_CATEGORY'
                                        THEN fund_amount
                                    END AS internal_transfer_from_same_category
                                ,CASE WHEN type in (3) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_TO_SAME_CATEGORY'
                                        THEN fund_amount
                                    END AS internal_transfer_to_same_category
                                ,CASE WHEN type in (4) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Credit'
                                        THEN fund_amount
                                    ELSE 0 END AS credit
                                ,CASE WHEN type in (5) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Points'
                                        THEN fund_amount
                                    ELSE 0 END AS points
                                ,CASE WHEN type in (8) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'CFD'
                                        THEN fund_amount
                                    ELSE 0 END AS CFD
                                ,CASE WHEN type in (9) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Rollover'
                                        THEN fund_amount
                                    ELSE 0 END AS rollover
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC OP'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_OP
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Split'
                                        THEN fund_amount
                                    ELSE 0 END AS "SPLIT"
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Compensation'
                                        THEN fund_amount
                                    ELSE 0 END AS Compensation
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC Clear'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_Clear
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC TR'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_TR
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC FUNDING'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_FUNDING
                                -- promo not in txn types ！！！
                                -- need to be confirmed
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Promo'
                                        THEN fund_amount
                                    ELSE 0 END AS promo
                                ,CASE WHEN type in (11) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'IB Rebate'
                                        THEN fund_amount
                                    ELSE 0 END AS ib_rebate
                                ,CASE WHEN type in (11) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'Auto Rebate'
                                        THEN fund_amount
                                    ELSE 0 END AS auto_rebate
                                ,CASE WHEN type in (11) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'CPA Rebate'
                                        THEN fund_amount
                                    ELSE 0 END AS CPA_rebate
                            FROM
                                ctran
                            LEFT JOIN {4}_DATAWAREHOUSE.{2}.TRANSACTION_COMMENT_MAPPING AS tcm
                            ON LOWER(ctran.trading_server_comment) LIKE LOWER(tcm.COMMENT_TEMPLATE)
                        )
                GROUP BY server,login
            ),
            transactions AS (
                SELECT
                        server
                    ,login
                    ,COALESCE(ROUND(SUM(sales_deposit),2),0) AS sales_deposit
                    ,COALESCE(ROUND(SUM(sales_withdrawal),2),0) AS sales_withdrawal
                    ,COALESCE(ROUND(SUM(sales_net_deposit),2),0) AS sales_net_deposit
                    ,COALESCE(ROUND(SUM(deposit),2),0) AS deposit
                    ,COALESCE(ROUND(SUM(withdrawal),2),0) AS withdrawal
                    ,COALESCE(ROUND(SUM(net_deposit),2),0) AS net_deposit
                    ,COALESCE(ROUND(SUM(credit_change),2),0) AS credit_change
                    ,COALESCE(ROUND(SUM(internal_transfer),2),0) AS internal_transfer
                    ,COALESCE(ROUND(SUM(internal_transfer_deposit_from_ib),2),0) AS internal_transfer_deposit_from_ib
                    ,COALESCE(ROUND(SUM(internal_transfer_withdrawal_to_ib),2),0) AS internal_transfer_withdrawal_to_ib
                    ,COALESCE(ROUND(SUM(internal_transfer_from_same_category),2),0) AS internal_transfer_from_same_category
                    ,COALESCE(ROUND(SUM(internal_transfer_to_same_category),2),0) AS internal_transfer_to_same_category
                    ,COALESCE(ROUND(SUM(credit),2),0) AS credit
                    ,COALESCE(ROUND(SUM(points),2),0) AS points
                    ,COALESCE(ROUND(SUM(CFD),2),0) AS CFD
                    ,COALESCE(ROUND(SUM(rollover),2),0) AS rollover
                    ,COALESCE(ROUND(SUM(MISC_OP),2),0) AS MISC_OP
                    ,COALESCE(ROUND(SUM("SPLIT"),2),0) AS "SPLIT"
                    ,COALESCE(ROUND(SUM(Compensation),2),0) AS Compensation
                    ,COALESCE(ROUND(SUM(MISC_Clear),2),0) AS MISC_Clear
                    ,COALESCE(ROUND(SUM(MISC_TR),2),0) AS MISC_TR
                    ,COALESCE(ROUND(SUM(MISC_FUNDING),2),0) AS MISC_FUNDING
                    ,COALESCE(ROUND(SUM(promo),2),0) AS promo
                    ,COALESCE(ROUND(SUM(ib_rebate),2),0) AS ib_rebate
                    ,COALESCE(ROUND(SUM(auto_rebate),2),0) AS auto_rebate
                    ,COALESCE(ROUND(SUM(CPA_rebate),2),0) AS CPA_rebate
                    ,COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                        + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_rebate
                    ,COALESCE(ROUND(SUM(net_deposit),2),0) + COALESCE(ROUND(SUM(internal_transfer),2),0)
                        + COALESCE(ROUND(SUM(credit),2),0) + COALESCE(ROUND(SUM(points),2),0)
                        + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(rollover),2),0)
                        + COALESCE(ROUND(SUM(MISC_OP),2),0) + COALESCE(ROUND(SUM("SPLIT"),2),0)
                        -- + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(compensation),2),0)
                        + COALESCE(ROUND(SUM(compensation),2),0)
                        + COALESCE(ROUND(SUM(MISC_Clear),2),0) + COALESCE(ROUND(SUM(MISC_TR),2),0)
                        + COALESCE(ROUND(SUM(MISC_FUNDING),2),0) + COALESCE(ROUND(SUM(promo),2),0)
                        + COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                        + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_txn
                FROM
                (
                    SELECT
                        server
                        ,login
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                THEN profit
                                ELSE 0 END AS deposit
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                    OR tcm.SALES_TYPE = 'Deposit')
                                THEN profit
                                ELSE 0 END AS sales_deposit
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal'
                                THEN profit
                                ELSE 0 END AS withdrawal
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal'
                                    OR tcm.SALES_TYPE = 'Withdrawal')
                                THEN profit
                                ELSE 0 END AS sales_withdrawal
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                    OR SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal')
                                THEN profit
                                ELSE 0 END AS net_deposit
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                    OR tcm.SALES_TYPE = 'Deposit'
                                    OR SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal'
                                    OR tcm.SALES_TYPE = 'Withdrawal')
                                THEN profit
                                ELSE 0 END AS sales_net_deposit
                        ,CASE WHEN cmd = 7
                                THEN profit
                                ELSE 0 END AS credit_change
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Internal_Transfer'
                                THEN profit
                                ELSE 0 END AS internal_transfer
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_DEPOSIT_FROM_IB'
                                THEN profit
                                ELSE 0 END AS internal_transfer_deposit_from_ib
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_WITHDRAWAL_TO_IB'
                                THEN profit
                                ELSE 0 END AS internal_transfer_withdrawal_to_ib
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_FROM_SAME_CATEGORY'
                                THEN profit
                                ELSE 0 END AS internal_transfer_from_same_category
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_TO_SAME_CATEGORY'
                                THEN profit
                                ELSE 0 END AS internal_transfer_to_same_category
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Credit'
                                THEN profit
                                ELSE 0 END AS credit
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Points'
                                THEN profit
                                ELSE 0 END AS points
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'CFD'
                                THEN profit
                                ELSE 0 END AS CFD
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Rollover'
                                THEN profit
                                ELSE 0 END AS rollover
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC OP'
                                THEN profit
                                ELSE 0 END AS MISC_OP
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Split'
                                THEN profit
                                ELSE 0 END AS "SPLIT"
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Compensation'
                                THEN profit
                                ELSE 0 END AS compensation
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC Clear'
                                THEN profit
                                ELSE 0 END AS MISC_Clear
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC TR'
                                THEN profit
                                ELSE 0 END AS MISC_TR
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC FUNDING'
                                THEN profit
                                ELSE 0 END AS MISC_FUNDING
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Promo'
                                THEN profit
                                ELSE 0 END AS promo
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'IB Rebate'
                                THEN profit
                                ELSE 0 END AS ib_rebate
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'Auto Rebate'
                                THEN profit
                                ELSE 0 END AS auto_rebate
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'CPA Rebate'
                                THEN profit
                                ELSE 0 END AS CPA_rebate
                    FROM
                        rstt
                    LEFT JOIN {4}_DATAWAREHOUSE.{2}.TRANSACTION_COMMENT_MAPPING AS tcm
                    ON LOWER(rstt.COMMENT) LIKE LOWER(tcm.COMMENT_TEMPLATE)
                    )
                GROUP BY server,login
            ),
                rebate_rate AS (
                    SELECT
                        fx.trading_server_group_id
                        ,rbc AS forex_rebate
                        ,bul.bul_rebate AS bul_rebate
                        ,oil.oil_rebate AS oil_rebate
                        ,cfd.cfd_rebate AS cfd_rebate
                        ,crypto.crypto_rebate AS crypto_rebate
                        ,fx_exotics.fx_exotics_rebate AS fx_exotics_rebate
                    FROM {1}.crm_trading_server_group_has_commission_limits AS fx
                    LEFT JOIN (
                        SELECT
                            trading_server_group_id
                            ,rbc AS bul_rebate
                        FROM {1}.crm_trading_server_group_has_commission_limits
                        WHERE external_symbol_group_id in (7)
                    ) AS bul
                    ON fx.trading_server_group_id = bul.trading_server_group_id
                    LEFT JOIN (
                        SELECT
                            trading_server_group_id
                            ,rbc AS oil_rebate
                        FROM {1}.crm_trading_server_group_has_commission_limits
                        WHERE external_symbol_group_id in (8)
                    ) AS oil
                    ON fx.trading_server_group_id = oil.trading_server_group_id
                    LEFT JOIN (
                        SELECT
                            trading_server_group_id
                            ,rbc AS cfd_rebate
                        FROM {1}.crm_trading_server_group_has_commission_limits
                        WHERE external_symbol_group_id = 10
                    ) AS cfd
                    ON fx.trading_server_group_id = cfd.trading_server_group_id
                    LEFT JOIN (
                        SELECT
                            trading_server_group_id
                            ,rbc AS crypto_rebate
                        FROM {1}.crm_trading_server_group_has_commission_limits
                        WHERE external_symbol_group_id = 11
                    ) AS crypto
                    ON fx.trading_server_group_id = crypto.trading_server_group_id
                    LEFT JOIN (
                        SELECT
                            trading_server_group_id
                            ,rbc AS fx_exotics_rebate
                        FROM {1}.crm_trading_server_group_has_commission_limits
                        WHERE external_symbol_group_id = 13
                    ) AS fx_exotics
                    ON fx.trading_server_group_id = fx_exotics.trading_server_group_id
                    WHERE external_symbol_group_id in (6)
                ),wallet_balance AS
            (
                SELECT 
                    external_id,
                    type,
                    balance AS wallet_balance
                FROM  {4}_DATAWAREHOUSE.{1}.crm_trading_accounts_daily 
                WHERE external_id LIKE '%W%'
                    AND deleted_at IS NULL
                AND DATE(utc_now) = DATE(cast('{3}' as timestamp) - INTERVAL '1 DAY')
            ),
            first_ta AS
            (
                SELECT 
                    COALESCE(mfta.id,mibta.id) AS user_id,
                    member_first_ta_registration_date,
                    member_first_ib_ta_registration_date
                FROM
                    (
                        SELECT 
                            cu.id,
                            MIN(ta.created_at) AS member_first_ta_registration_date
                        FROM cu 
                        LEFT JOIN trading_accounts AS ta
                            ON cu.id = ta.user_id
                        WHERE ta.salescode NOT LIKE '%9999%'
                            AND lower(ta.salescode) NOT LIKE '%test%'
                            AND external_id NOT LIKE '%W%'
                            AND ta.is_ib =0
                        GROUP BY cu.id
                    ) AS mfta 
                    FULL OUTER JOIN
                    (
                        SELECT 
                            cu.id,
                            MIN(ta.created_at) AS member_first_ib_ta_registration_date
                        FROM cu 
                        LEFT JOIN trading_accounts AS ta
                            ON cu.id = ta.user_id
                        WHERE ta.salescode NOT LIKE '%9999%'
                            AND lower(ta.salescode) NOT LIKE '%test%'
                            AND external_id NOT LIKE '%W%'
                            AND ta.is_ib =1
                        GROUP BY cu.id    
                    ) AS mibta
                    ON mfta.id=mibta.id
            ),
            all_accounts_info AS (
                SELECT
                    IFF(all_ta.login LIKE 'W%', ta.server, all_ta.server) AS server
                    ,all_ta.login AS login
                    ,IFF(all_ta.login LIKE 'W%', ta.trading_server_name, cs.crm_server_name) AS trading_server_name
                    ,COALESCE(cs.crm_server_id, ta.type) AS type
                    ,rsu."GROUP"
                    ,ta.user_id AS user_id
                    ,ta.salescode AS salescode
                    ,ta.balance AS points_balance
                    ,CASE WHEN ta.is_ib = 1
                            THEN 'True'
                            ELSE 'False'
                            END AS is_IB
                    ,users.sales_code AS member_sales_code
                    ,users.lead_source AS member_lead_source
                    ,ta2.external_id AS agent
                    ,jurisdiction
                    ,users.created_at AS member_registration_date
                    ,ft.member_first_ta_registration_date
                    ,ft.member_first_ib_ta_registration_date
                    ,rsu.credit
                    ,CASE WHEN rsu.credit > 0 AND rsu.equity < 0
                            THEN rsu.credit * -1
                        WHEN rsu.credit > 0 AND rsu.equity >= 0 AND rsu.equity < rsu.credit
                            THEN rsu.equity - rsu.credit
                        ELSE 0
                    END AS used_credit
                    ,rsu.equity
                    ,CASE WHEN rsu.equity - rsu.balance < 0 AND rsu.balance < 0
                            THEN 0
                        WHEN rsu.equity - rsu.credit < 0
                            THEN 0
                        ELSE ROUND(rsu.equity - rsu.credit, 2)
                    END AS positive_equity
                    ,rsu.balance
                    ,CASE WHEN rsu.balance > 0
                            THEN rsu.balance
                        ELSE 0
                    END AS positive_balance
                    ,CASE WHEN rsu.balance < 0
                        THEN rsu.balance
                        ELSE 0
                    END AS negative_balance
                    ,rsu.currency
                    ,users.adj_country AS country
                    ,rsu.regdate AS ta_registration_date
                    ,rsu.margin
                    ,rsu.margin_free
                    ,rsu.margin_level
                    ,rsu.leverage
                    ,rsu.comment
                    ,rsu.lead_source AS ta_lead_source
                    ,rebate_rate.forex_rebate AS fx_rebate
                    ,rebate_rate.bul_rebate AS bul_rebate
                    ,rebate_rate.oil_rebate AS oil_rebate
                    ,rebate_rate.cfd_rebate AS cfd_rebate
                    ,rebate_rate.crypto_rebate AS crypto_rebate
                    ,rebate_rate.fx_exotics_rebate AS fx_exotics_rebate
                    ,users.member_acct_type AS member_account_type
                    ,ta.member_type
                    ,IFF(rsu."GROUP" LIKE '%va%', 'Vanuatu', users.report_jurisdiction) AS report_jurisdiction
                FROM (
                    SELECT DISTINCT server, CAST(login AS VARCHAR) AS login
                    FROM rsuh
                    UNION ALL
                    SELECT null AS server, external_id AS login
                    FROM trading_accounts
                    WHERE type IN ({7})
                ) AS all_ta
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers AS cs
                ON all_ta.server = cs.server
                LEFT JOIN trading_accounts ta
                ON all_ta.login = ta.external_id
                    AND (all_ta.server = ta.server OR all_ta.login LIKE 'W%')
                LEFT JOIN trading_accounts ta2
                ON ta.parent_ta_id = ta2.id
                LEFT JOIN rsuh AS rsu
                ON all_ta.login = CAST(rsu.login AS VARCHAR(50))
                    AND rsu.server = all_ta.server
                LEFT JOIN {4}_DATAWAREHOUSE.{1}.crm_trading_server_groups ctsg
                ON ta.trading_server_id = ctsg.trading_server_id
                    AND rsu."GROUP"=ctsg."group"
                LEFT JOIN users
                ON ta.user_id = users.id
                LEFT JOIN rebate_rate
                ON ctsg.id = rebate_rate.trading_server_group_id
                LEFT JOIN first_ta AS ft
                    ON ta.user_id = ft.user_id
                WHERE ((LOWER(users.sales_code) NOT LIKE '%test%'
                    AND LOWER(users.sales_code) NOT LIKE '%hedge%'
                    AND LOWER(users.sales_code) NOT LIKE '%nosales%'
                    AND LOWER(users.sales_code) NOT LIKE '%9999%')
                    OR users.sales_code IS NULL)
            ),
            -- daily level symbol/currency USD conversion ratio
            ex_rate AS (
                SELECT DISTINCT
                   DATE(pricing.timereceived_server) AS dt_report -- every day will only have 1 close
                  ,pricing.core_symbol
                  ,symbol_spec.symbol
                  ,CASE
                     WHEN symbol_spec.symbol = 'USDCNH' THEN 'CNH'
                     WHEN symbol_spec.symbol = 'EURUSD' THEN 'EUR'
                     WHEN symbol_spec.symbol = 'NZDUSD' THEN 'NZD'
                     WHEN symbol_spec.symbol = 'GBPUSD' THEN 'GBP'
                     WHEN symbol_spec.symbol = 'USDHKD' THEN 'HKD'
                     WHEN symbol_spec.symbol = 'AUDUSD' THEN 'AUD'
                     WHEN symbol_spec.symbol = 'USDCAD' THEN 'CAD'
                     WHEN symbol_spec.symbol = 'USDCHF' THEN 'CHF'
                     WHEN symbol_spec.symbol = 'USDJPY' THEN 'JPY'
                     ELSE 'UNK'
                   END AS ac
                  ,FIRST_VALUE(pricing.close) OVER(PARTITION BY pricing.core_symbol, DATE(pricing.timereceived_server)
                      ORDER BY pricing.timereceived_server DESC) AS close
                  ,symbol_spec.contractsize
                  ,symbol_spec.basecurrency
                  ,symbol_spec.type
                FROM {4}_DATAWAREHOUSE.{1}.pricing_ohlc_minute  AS pricing
                LEFT JOIN {4}_DATAWAREHOUSE.{1}.symbol_settings AS symbol_spec -- get the symbol specification bases on received time
                  ON replace(pricing.core_symbol,'/','') = symbol_spec.symbol
                 AND pricing.TIMERECEIVED_SERVER::DATE >= symbol_spec.START_DATE
                 AND pricing.TIMERECEIVED_SERVER::DATE <= symbol_spec.LAST_ACTIVE
                WHERE pricing.core_symbol IN ('EUR/USD','AUD/USD','GBP/USD','USD/CAD','USD/HKD','NZD/USD','USD/CNH','USD/CHF','USD/JPY') -- limit to forex symbol
                  AND symbol_spec.symbol IS NOT NULL
                  AND pricing.TIMERECEIVED_SERVER::DATE >=  DATE('{3}') - INTERVAL '14 DAY' -- need check it later
            ), 
            last_report AS (
                -- first intraday report from t-1
                SELECT *
                FROM {4}_DATAWAREHOUSE.{2}.accounting_t_1_equity_report
                WHERE dt_report = DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
            ),
            -- first record for each login to get equity at TA open
            first_report AS (
                SELECT
                   user_id -- some user_id is null
                  ,login
                  ,trading_server
                  ,positive_equity
                  ,ROW_NUMBER() OVER (PARTITION BY user_id, login, trading_server ORDER BY dt_report) AS rn
                FROM {4}_DATAWAREHOUSE.{2}.accounting_t_1_equity_report
                --WHERE dt_report <= DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                WHERE user_id IS NOT NULL
                QUALIFY rn = 1
            ),
            trades_txn AS (
                SELECT 
                   login
                  ,server
                  ,sum(profit) as profit_sum
                  ,DATE(max(open_time)) AS dt_report -- might not consider this date, but just get the latest day of USD exchange rate
                FROM (
                    SELECT 
                       ticket
                      ,login
                      ,server
                      ,profit
                      ,cmd
                      ,open_time
                      ,ROW_NUMBER() OVER(PARTITION BY server, ticket ORDER BY utc_now DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_transactions
                    WHERE server NOT IN {5}
                      AND DATE(concat(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR))) <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                      AND open_time <= DATE(CAST('{3}' AS TIMESTAMP))
                      AND LOWER(comment) NOT like '%from master trade%'
                    QUALIFY rn = 1
                        AND cmd != 7  -- credit
                        --AND open_time < IFF(MINUTE(TO_RTZ(SYSDATE())) >= 30, DATE_TRUNC('HOUR',TO_RTZ(SYSDATE())) + INTERVAL '30 MINUTE', DATE_TRUNC('HOUR',TO_RTZ(SYSDATE())))
                )
                GROUP BY 1, 2
            ),
            -- local currency
            t_1_equity_lc AS (
                SELECT
                    DISTINCT
                    IFF(MINUTE(CAST('{3}' AS TIMESTAMP)) >= 30,
                        DATE_TRUNC('HOUR', CAST('{3}' AS TIMESTAMP)) + INTERVAL '30 MINUTE',
                        DATE_TRUNC('HOUR', CAST('{3}' AS TIMESTAMP)) ) AS dt_report
                    ,TO_VARCHAR(TO_RTZ(SYSDATE())::TIMESTAMP, 'yyyy-mm-dd hh:mm:ss') AS dt_report_insert
                    ,all_accounts_info.jurisdiction AS report_jurisdiction
                    ,all_accounts_info.login AS login
                    ,all_accounts_info.is_IB AS is_ib
                    ,all_accounts_info."GROUP" AS "GROUP"
                    ,all_accounts_info.user_id AS user_id
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.deposit, transactions.deposit),0) AS deposit
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.withdrawal, transactions.withdrawal),0) AS withdraw
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.net_deposit, transactions.net_deposit),0) AS net_deposit
                    ,COALESCE(all_accounts_info.credit,0) AS credit
                    ,COALESCE(transactions.credit_change,wt.credit_change,0) AS credit_change
                    ,COALESCE(trades_volume.swaps,0) AS swap
                    ,COALESCE(trades_volume.commissions,0) AS commission
                    ,all_accounts_info.agent AS agent
                --         ,all_accounts_info.agent_copy AS "AGENT(COPY)"
                    ,COALESCE(all_accounts_info.fx_rebate,0) AS forex_rebate
                    ,COALESCE(all_accounts_info.bul_rebate,0) AS bullion_rebate
                    ,COALESCE(all_accounts_info.oil_rebate,0) AS oil_rebate
                    ,COALESCE(all_accounts_info.cfd_rebate,0) AS cfd_rebate
                    ,COALESCE(all_accounts_info.crypto_rebate,0) AS crypto_rebate
                    ,COALESCE(all_accounts_info.fx_exotics_rebate,0) AS fx_exotics_rebate
                    ,COALESCE(ROUND(IFF(all_accounts_info.type = 3, 0,
                        trades_volume.forex_volume*all_accounts_info.fx_rebate + trades_volume.bullion_volume*all_accounts_info.bul_rebate + trades_volume.oil_volume*all_accounts_info.oil_rebate
                        + trades_volume.cfd_volume*all_accounts_info.cfd_rebate + trades_volume.crypto_volume*all_accounts_info.crypto_rebate + trades_volume.fx_exotics_vol*all_accounts_info.fx_exotics_rebate), 2), 0) AS trade_cost
                    -- eq change + total_cost
                    ,COALESCE(ROUND(IFF(all_accounts_info.type = 3, 0,
                        all_accounts_info.positive_equity - COALESCE(lr.positive_equity, 0) - COALESCE(transactions.tot_txn,0) + COALESCE(trades_volume.forex_volume*all_accounts_info.fx_rebate + trades_volume.bullion_volume*all_accounts_info.bul_rebate + trades_volume.oil_volume*all_accounts_info.oil_rebate
                        + trades_volume.cfd_volume*all_accounts_info.cfd_rebate + trades_volume.crypto_volume*all_accounts_info.crypto_rebate + trades_volume.fx_exotics_vol*all_accounts_info.fx_exotics_rebate, 0)), 2), 0) AS cost_to_loss
                    ,COALESCE(trades_volume.forex_volume,0) AS forex_vol
                    ,COALESCE(trades_volume.bullion_volume,0) AS bullion_vol
                    ,COALESCE(trades_volume.oil_volume,0) AS oil_vol
                    ,COALESCE(trades_volume.cfd_volume,0) AS cfd_vol
                    ,COALESCE(trades_volume.crypto_volume,0) AS crypto_vol
                    ,COALESCE(trades_volume.fx_exotics_vol,0) AS fx_exotics_vol
                    ,COALESCE(trades_volume.profit,0)  AS profit
                    ,COALESCE(IFF(all_accounts_info.type = 3, 0, trades_volume.sum_pnl), 0) AS pnl
                    ,COALESCE(all_accounts_info.equity,0) AS equity
                    ,COALESCE(all_accounts_info.positive_equity ,0) AS positive_equity
                    -- last report
                    ,COALESCE(ROUND(IFF(all_accounts_info.type = 3, 0, all_accounts_info.positive_equity - COALESCE(transactions.tot_txn,0) - COALESCE(lr.positive_equity, 0)), 2), 0) AS equity_change
                    ,COALESCE(IFF(all_accounts_info.type = 3, wtb.wallet_balance, all_accounts_info.balance) , 0) AS last_balance
                    ,COALESCE(IFF(all_accounts_info.type = 3, wtb.wallet_balance, all_accounts_info.positive_balance), 0) AS positive_balance
                    ,COALESCE(IFF(all_accounts_info.type = 3, 0, all_accounts_info.used_credit) ,0) AS used_credit
                    ,COALESCE(IFF(all_accounts_info.type = 3, 0, all_accounts_info.negative_balance) ,0) AS negative_balance
                    ,IFF(all_accounts_info.type = 3, 'POINTS', all_accounts_info.currency) AS currency
                    ,all_accounts_info.country AS country
                    ,all_accounts_info.member_sales_code AS member_sales_code
                    ,all_accounts_info.salescode AS ta_sales_code
                    ,COALESCE(all_accounts_info.margin,0) AS margin
                    ,COALESCE(all_accounts_info.margin_free,0) AS margin_free
                    ,COALESCE(all_accounts_info.margin_level,0) AS margin_level
                    ,COALESCE(all_accounts_info.leverage,0) AS leverage
                    -- last report
                    ,ROUND(COALESCE(IFF(all_accounts_info.type = 3, wtb.wallet_balance, all_accounts_info.used_credit) ,0) - COALESCE(lr.used_credit, 0), 2) AS used_credit_change
                    -- last report
                    ,ROUND(COALESCE(IFF(all_accounts_info.type = 3, wtb.wallet_balance, all_accounts_info.negative_balance) ,0) - COALESCE(lr.negative_balance, 0), 2) AS negative_balance_change
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.internal_transfer, transactions.internal_transfer),0) AS internal_transfer
                    -- remove
                    ,0 AS rebate
                    ,0 AS instant_rebate
                    ,0 AS adjustment
                    ,0 AS misc
                    ,all_accounts_info.member_registration_date AS member_register_date
                    ,all_accounts_info.ta_registration_date AS ta_register_date
                    ,all_accounts_info.member_first_ta_registration_date AS member_first_ta_registration_date
                    ,all_accounts_info.member_first_ib_ta_registration_date AS member_first_ib_ta_registration_date
                    ,all_accounts_info.jurisdiction AS jurisdiction
                    ,CASE WHEN all_accounts_info."GROUP" LIKE '%va%' THEN 'Vanuatu'
                        ELSE all_accounts_info.jurisdiction END AS final_jurisdiction
                    ,all_accounts_info.trading_server_name AS trading_server
                    ,all_accounts_info.comment AS comment
                    ,all_accounts_info.member_lead_source AS member_lead_source
                    ,all_accounts_info.ta_lead_source AS ta_lead_source
                    ,COALESCE(open_orders_count.open_order_count,0) AS count_open_orders
                    ,COALESCE(orders_count.count_pending_orders,0) AS count_pending_orders
                    ,COALESCE(orders_count.count_closed_orders,0) AS count_closed_orders
                    ,COALESCE(orders_count.average_trade_duration,0) AS average_trade_duration
                    ,COALESCE(all_accounts_info.member_account_type,'') AS member_account_type
                    ,COALESCE(all_accounts_info.member_type,'') AS member_type
                    ,CASE WHEN rs.name IS NULL THEN 0 ELSE 1 END AS is_cimb
                    ,CASE WHEN rs.name IS NULL THEN '' 
                        WHEN rs.name like '%iress%' THEN 'iress' 
                        WHEN rs.name like '%insight%' THEN 'insights' END AS cimb_name 
                    ,CASE WHEN rs.name IS NULL THEN '' 
                        ELSE CONCAT('TM',split(LOWER(rs.name),'tm')[1]) END AS cimb_cfd
                    ,TO_VARCHAR('{3}'::TIMESTAMP, 'yyyy') AS year
                    ,TO_VARCHAR('{3}'::TIMESTAMP, 'mm') AS month
                    ,TO_VARCHAR('{3}'::TIMESTAMP, 'dd') AS day
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.sales_deposit, transactions.sales_deposit),0) AS sales_deposit
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.sales_withdrawal, transactions.sales_withdrawal),0) AS sales_withdraw
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.sales_net_deposit, transactions.sales_net_deposit),0) AS sales_net_deposit
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.internal_transfer_deposit_from_ib, transactions.internal_transfer_deposit_from_ib),0) AS internal_transfer_deposit_from_ib
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.internal_transfer_withdrawal_to_ib, transactions.internal_transfer_withdrawal_to_ib),0) AS internal_transfer_withdrawal_to_ib
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.internal_transfer_from_same_category, transactions.internal_transfer_from_same_category),0) AS internal_transfer_from_same_category
                    ,COALESCE(IFF(all_accounts_info.type = 3, wt.internal_transfer_to_same_category, transactions.internal_transfer_to_same_category),0) AS internal_transfer_to_same_category
                    -- new category
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.credit, transactions.credit),0) AS credit_txn
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.points, transactions.points),0) AS points
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.CFD, transactions.CFD),0) AS CFD
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.rollover, transactions.rollover),0) AS rollover
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_OP, transactions.MISC_OP),0) AS MISC_OP
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt."SPLIT", transactions."SPLIT"),0) AS "SPLIT"
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.compensation, transactions.compensation),0) AS compensation
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_Clear, transactions.MISC_Clear),0) AS MISC_Clear
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_TR, transactions.MISC_TR),0) AS MISC_TR
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_FUNDING, transactions.MISC_FUNDING),0) AS MISC_FUNDING
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.promo, transactions.promo),0) AS promo
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.ib_rebate, transactions.ib_rebate),0) AS ib_rebate
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.auto_rebate, transactions.auto_rebate),0) AS auto_rebate
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.CPA_rebate, transactions.CPA_rebate),0) AS CPA_rebate
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.tot_rebate, transactions.tot_rebate),0) AS total_rebate
                    -- TA open equity 
                    ,COALESCE(frst.positive_equity, 0) AS ta_open_positive_equity
                    ,COALESCE(txn.profit_sum, 0) AS profit_sum
                FROM all_accounts_info
                LEFT JOIN trades_volume
                ON all_accounts_info.server = trades_volume.server
                    AND all_accounts_info.login = CAST(trades_volume.login AS VARCHAR)
                LEFT JOIN orders_count
                ON all_accounts_info.server = orders_count.server
                    AND all_accounts_info.login = CAST(orders_count.login AS VARCHAR)
                LEFT JOIN open_orders_count
                ON all_accounts_info.server = open_orders_count.server
                    AND all_accounts_info.login = CAST(open_orders_count.login AS VARCHAR)
                LEFT JOIN transactions
                ON all_accounts_info.server = transactions.server
                    AND all_accounts_info.login = CAST(transactions.login AS VARCHAR)
                LEFT JOIN last_report AS lr
                ON all_accounts_info.login = lr.login
                    AND all_accounts_info.trading_server_name = lr.trading_server
                LEFT JOIN wallet_transactions AS wt
                ON all_accounts_info.login = CAST(wt.login AS VARCHAR)
                LEFT JOIN wallet_balance AS wtb
                ON all_accounts_info.login = CAST(wtb.external_id AS VARCHAR)
                    AND all_accounts_info.type = wtb.type
                LEFT JOIN rsuh_recent as rs
                    ON all_accounts_info.login=cast(rs.login AS VARCHAR)
                    AND all_accounts_info.type = rs.rs_type
                LEFT JOIN first_report AS frst
                    ON all_accounts_info.login = frst.login
                    AND all_accounts_info.trading_server_name = frst.trading_server
                LEFT JOIN trades_txn AS txn
                    ON all_accounts_info.login = txn.login
                    AND all_accounts_info.server = txn.server
                WHERE (UPPER(all_accounts_info.jurisdiction) NOT LIKE '%TEST%' OR all_accounts_info.jurisdiction IS NULL)
            )
            SELECT
                   dt_report
                  ,dt_report_insert
                  ,report_jurisdiction
                  ,login
                  ,is_ib
                  ,"GROUP"
                  ,user_id
                  ,deposit
                  ,withdraw
                  ,net_deposit
                  ,credit
                  ,credit_change
                  ,swap
                  ,commission
                  ,agent
                  ,forex_rebate
                  ,bullion_rebate
                  ,oil_rebate
                  ,cfd_rebate
                  ,crypto_rebate
                  ,fx_exotics_rebate
                  ,trade_cost
                  ,cost_to_loss
                  ,forex_vol
                  ,bullion_vol
                  ,oil_vol
                  ,cfd_vol
                  ,crypto_vol
                  ,fx_exotics_vol
                  ,profit
                  ,pnl
                  ,equity
                  ,positive_equity
                  ,equity_change
                  ,last_balance
                  ,positive_balance
                  ,used_credit
                  ,negative_balance
                  ,currency
                  ,country
                  ,member_sales_code
                  ,ta_sales_code
                  ,margin
                  ,margin_free
                  ,margin_level
                  ,leverage
                  ,used_credit_change
                  ,negative_balance_change
                  ,internal_transfer
                  ,rebate
                  ,instant_rebate
                  ,adjustment
                  ,misc
                  ,member_register_date
                  ,ta_register_date
                  ,member_first_ta_registration_date
                  ,member_first_ib_ta_registration_date
                  ,jurisdiction
                  ,final_jurisdiction
                  ,trading_server
                  ,comment
                  ,member_lead_source
                  ,ta_lead_source
                  ,count_open_orders
                  ,count_pending_orders
                  ,count_closed_orders
                  ,average_trade_duration
                  ,member_account_type
                  ,member_type
                  ,is_cimb
                  ,cimb_name
                  ,cimb_cfd
                  ,year
                  ,month
                  ,day
                  ,sales_deposit
                  ,sales_withdraw
                  ,sales_net_deposit
                  ,internal_transfer_deposit_from_ib
                  ,internal_transfer_withdrawal_to_ib
                  ,internal_transfer_from_same_category
                  ,internal_transfer_to_same_category
                  ,credit_txn
                  ,points
                  ,cfd
                  ,rollover
                  ,misc_op
                  ,split
                  ,compensation
                  ,misc_clear
                  ,misc_tr
                  ,misc_funding
                  ,promo
                  ,ib_rebate
                  ,auto_rebate
                  ,cpa_rebate
                  ,total_rebate
                  ,ROUND(CASE
                            WHEN currency = 'USD' THEN lifetime_equity_change_lc * 1.0
                            WHEN SUBSTRING(symbol,1,3) = currency THEN lifetime_equity_change_lc * close
                            WHEN SUBSTRING(symbol,-3) = currency THEN lifetime_equity_change_lc / close
                            ELSE 0.0
                          END, 2) AS lifetime_equity_change_usd
                FROM (
                    SELECT
                       eq.dt_report
                      ,eq.dt_report_insert
                      ,eq.report_jurisdiction
                      ,eq.login
                      ,eq.is_ib
                      ,eq."GROUP"
                      ,eq.user_id
                      ,eq.deposit
                      ,eq.withdraw
                      ,eq.net_deposit
                      ,eq.credit
                      ,eq.credit_change
                      ,eq.swap
                      ,eq.commission
                      ,eq.agent
                      ,eq.forex_rebate
                      ,eq.bullion_rebate
                      ,eq.oil_rebate
                      ,eq.cfd_rebate
                      ,eq.crypto_rebate
                      ,eq.fx_exotics_rebate
                      ,eq.trade_cost
                      ,eq.cost_to_loss
                      ,eq.forex_vol
                      ,eq.bullion_vol
                      ,eq.oil_vol
                      ,eq.cfd_vol
                      ,eq.crypto_vol
                      ,eq.fx_exotics_vol
                      ,eq.profit
                      ,eq.pnl
                      ,eq.equity
                      ,eq.positive_equity
                      ,eq.equity_change
                      ,eq.last_balance
                      ,eq.positive_balance
                      ,eq.used_credit
                      ,eq.negative_balance
                      ,eq.currency
                      ,eq.country
                      ,eq.member_sales_code
                      ,eq.ta_sales_code
                      ,eq.margin
                      ,eq.margin_free
                      ,eq.margin_level
                      ,eq.leverage
                      ,eq.used_credit_change
                      ,eq.negative_balance_change
                      ,eq.internal_transfer
                      ,eq.rebate
                      ,eq.instant_rebate
                      ,eq.adjustment
                      ,eq.misc
                      ,eq.member_register_date
                      ,eq.ta_register_date
                      ,eq.member_first_ta_registration_date
                      ,eq.member_first_ib_ta_registration_date
                      ,eq.jurisdiction
                      ,eq.final_jurisdiction
                      ,eq.trading_server
                      ,eq.comment
                      ,eq.member_lead_source
                      ,eq.ta_lead_source
                      ,eq.count_open_orders
                      ,eq.count_pending_orders
                      ,eq.count_closed_orders
                      ,eq.average_trade_duration
                      ,eq.member_account_type
                      ,eq.member_type
                      ,eq.is_cimb
                      ,eq.cimb_name
                      ,eq.cimb_cfd
                      ,eq.year
                      ,eq.month
                      ,eq.day
                      ,eq.sales_deposit
                      ,eq.sales_withdraw
                      ,eq.sales_net_deposit
                      ,eq.internal_transfer_deposit_from_ib
                      ,eq.internal_transfer_withdrawal_to_ib
                      ,eq.internal_transfer_from_same_category
                      ,eq.internal_transfer_to_same_category
                      ,eq.credit_txn
                      ,eq.points
                      ,eq.cfd
                      ,eq.rollover
                      ,eq.misc_op
                      ,eq.split
                      ,eq.compensation
                      ,eq.misc_clear
                      ,eq.misc_tr
                      ,eq.misc_funding
                      ,eq.promo
                      ,eq.ib_rebate
                      ,eq.auto_rebate
                      ,eq.cpa_rebate
                      ,eq.total_rebate
                      ,eq.positive_equity - eq.ta_open_positive_equity - eq.profit_sum as lifetime_equity_change_lc
                      ,ex.symbol
                      ,ex.close
                      ,ROW_NUMBER() OVER (PARTITION BY eq.dt_report, eq.user_id, eq.login, eq.trading_server, eq.currency ORDER BY ex.dt_report DESC) AS rn
                    FROM t_1_equity_lc  eq
                    LEFT JOIN ex_rate   ex
                      ON eq.currency = ex.ac
                     AND eq.dt_report <= DATE(eq.dt_report)
                     AND eq.dt_report >= DATE(eq.dt_report) - INTERVAL '7 DAY' -- will 1:m join, so need dedup, look back for 7 days
                    QUALIFY rn = 1
            )
            """.format(LOOKBACK_INTERVAL, lnd_schema, tst_schema, dt_report, tenant, servers_exclusion,
                       crm_server_id_demo, crm_server_id_wallet, crm_server_id_iris, sub_ib_trn_type)
    else:
        query = """
            INSERT INTO {4}_DATAWAREHOUSE.{2}.accounting_t_1_equity_report_delta
                WITH rsuh AS 
                (   
                    SELECT 
                        * 
                    FROM 
                    (
                    SELECT DISTINCT
                        IFF(MINUTE(rtz) >= 30,
                            DATE_TRUNC('HOUR', rtz) + INTERVAL '30 MINUTE',
                            DATE_TRUNC('HOUR', rtz) ) AS dt_report
                        ,*
                        ,ROW_NUMBER() OVER(PARTITION BY server,login order by utc_now desc) AS rn
                    FROM (
                        SELECT
                            *
                            ,TO_RTZ(rsuh.utc_now)  AS rtz
                        FROM {4}_DATAWAREHOUSE.{1}.report_server_users_history AS rsuh
                        WHERE server NOT IN {5}
                            AND DATE(CONCAT(CAST(rsuh.year AS VARCHAR),'-',CAST(rsuh.month AS VARCHAR),'-',CAST(rsuh.day AS VARCHAR)))
                                >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 DAY', 'yyyy-mm-dd'))
                            AND DATE(CONCAT(CAST(rsuh.year AS VARCHAR),'-',CAST(rsuh.month AS VARCHAR),'-',CAST(rsuh.day AS VARCHAR)))
                                <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    )
                    WHERE IFF(MINUTE(rtz) >= 30,
                        DATE_TRUNC('HOUR', rtz) + INTERVAL '30 MINUTE',
                        DATE_TRUNC('HOUR', rtz) ) <= DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP))
                )
                WHERE rn =1
                    AND login NOT IN (728888, 718888)
                    AND LOWER("GROUP") NOT LIKE '%test%'
                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT IN ('system', 'datacenter', 'oz-cbook-pr-u')
                    AND LOWER("GROUP") NOT LIKE '%manager%'
                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT LIKE 'hf%'
                    AND LOWER("GROUP") NOT LIKE '%mam%'
                    AND comment NOT LIKE '%ALLOC=%'
                    AND LOWER(comment) NOT LIKE '%test%'
                    AND LOWER(comment) NOT LIKE '%ia:0;%'
                    AND LOWER(comment) NOT LIKE '%trademax hedge%'
                    AND LOWER(comment) NOT LIKE '%(gcgau)%'
                    AND LOWER(comment) NOT LIKE '%[webapi]%'
                    AND LOWER(comment) NOT LIKE '%tst%'
                    AND name NOT LIKE '%test%'
            ),rsuh_recent AS
            (
                SELECT 
                    name
                    ,server
                    ,login
                    ,crm_server_id as rs_type
                FROM 
                    (
                        SELECT 
                            rsuh.*,
                            ROW_NUMBER() OVER(PARTITION BY rsuh.server, rsuh.login order by rsuh.utc_now desc) AS rn,
                            cs.crm_server_id
                        FROM {4}_DATAWAREHOUSE.{1}.report_server_users_history AS rsuh
                        LEFT join {4}_DATAWAREHOUSE.{2}.crm_servers AS cs
                            ON rsuh.server=cs.server
                        WHERE rsuh.server NOT IN {5}
                                    AND login NOT IN (728888, 718888)
                                    AND LOWER("GROUP") NOT LIKE '%test%'
                                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT IN ('system', 'datacenter', 'oz-cbook-pr-u')
                                    AND LOWER("GROUP") NOT LIKE '%manager%'
                                    AND SPLIT_PART("GROUP", '\\\\', -1) NOT LIKE 'hf%'
                                    AND LOWER("GROUP") NOT LIKE '%mam%'
                                    AND comment NOT LIKE '%ALLOC=%'
                                    AND LOWER(comment) NOT LIKE '%test%'
                                    AND LOWER(comment) NOT LIKE '%ia:0;%'
                                    AND LOWER(comment) NOT LIKE '%trademax hedge%'
                                    AND LOWER(comment) NOT LIKE '%(gcgau)%'
                                    AND LOWER(comment) NOT LIKE '%[webapi]%'
                                    AND LOWER(comment) NOT LIKE '%tst%'
                                    AND name NOT LIKE '%test%'
                                    AND lower(name) like '%tm%'
                                    AND (lower(name) like '%iress%' OR lower(name) like '%insight%')
                    )
                WHERE rn =1
            ),
            rsu AS (
                SELECT *
                FROM {4}_DATAWAREHOUSE.{1}.report_server_users
                WHERE server NOT IN {5}
                        AND login NOT IN (728888, 718888)
                        AND LOWER("GROUP") NOT LIKE '%test%'
                        AND SPLIT_PART("GROUP", '\\\\', -1) NOT IN ('system', 'datacenter', 'oz-cbook-pr-u')
                        AND LOWER("GROUP") NOT LIKE '%manager%'
                        AND SPLIT_PART("GROUP", '\\\\', -1) NOT LIKE 'hf%'
                        AND LOWER(comment) NOT LIKE '%test%'
                        AND LOWER(comment) NOT LIKE '%ia:0;%'
                        AND LOWER(comment) NOT LIKE '%trademax hedge%'
                        AND LOWER(comment) NOT LIKE '%(gcgau)%'
                        AND LOWER(comment) NOT LIKE '%[webapi]%'
                        AND LOWER(comment) NOT LIKE '%tst%'
                        AND name NOT LIKE '%test%'
            ),
            rstt AS (
                SELECT
                    DISTINCT *
                FROM (
                    SELECT
                            *
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn1
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_transactions
                    WHERE server NOT IN {5}
                        AND DATE(concat(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                            >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                        AND DATE(concat(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                            <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                            AND open_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                                AND DATE(CAST('{3}' AS TIMESTAMP))
                    ) AS rstt
                WHERE rn1 = 1
            ),ct AS 
            (
                SELECT 
                    *
                FROM
                    (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at desc) AS rn
                        FROM {4}_DATAWAREHOUSE.{1}.crm_transactions 
                        WHERE external_id like '%W%'
                            AND status =2 
                            AND deleted_at is null
                    )
                WHERE rn =1
            ),
            rs_trades_open AS (
                SELECT *
                FROM (
                    SELECT
                            server,login,ticket,symbol,cmd,volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_open
                    WHERE server NOT IN {5}
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR))) >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR))) <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    AND open_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                            AND date(CAST('{3}' AS TIMESTAMP))
                )
                WHERE rn = 1
            ),
            rs_trades_open_history AS (
                SELECT
                    server,login,ticket,symbol,cmd,volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time, 1
                FROM (
                    SELECT
                            *
                    FROM (
                        SELECT *
                            ,TO_RTZ(rsoh.utc_now) AS rtz
                        FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_open_history AS rsoh
                        WHERE server NOT IN {5}
                            AND DATE(CONCAT(CAST(rsoh.year AS VARCHAR),'-',CAST(rsoh.month AS VARCHAR),'-',CAST(rsoh.day AS VARCHAR)))
                                >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                            AND DATE(CONCAT(CAST(rsoh.year AS VARCHAR),'-',CAST(rsoh.month AS VARCHAR),'-',CAST(rsoh.day AS VARCHAR)))
                                <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    )
                )
                WHERE IFF(MINUTE(rtz) >= 30,
                        DATE_TRUNC('HOUR', rtz) + INTERVAL '30 MINUTE',
                        DATE_TRUNC('HOUR', rtz) ) = DATE_TRUNC('HOUR', CAST('{3}' AS TIMESTAMP))
            ),
            rs_trades_closed AS (
                SELECT *
                FROM (
                    SELECT
                            server,login,ticket,symbol,cmd,volume AS volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_closed
                    WHERE server NOT IN {5}
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                        >= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP)  - INTERVAL '1 MONTH', 'yyyy-mm-dd'))
                    AND DATE(CONCAT(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR)))
                        <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                    AND concat(CAST(ticket AS VARCHAR),server) not in (
                        SELECT concat(CAST(ticket AS VARCHAR),server) 
                            FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_open
                            WHERE server not in {5}
                                AND ticket IS NOT NULL
                        )
                )
                WHERE rn = 1
                    AND close_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                    AND DATE(CAST('{3}' AS TIMESTAMP))
            ),
            rs_trades_pending AS (
                SELECT *
                FROM (
                    SELECT
                            server,login,ticket,symbol,cmd,volume,open_time,close_time,swaps,commission,commission_agent,profit,modify_time
                        ,ROW_NUMBER() OVER(PARTITION BY login,server,ticket ORDER BY modify_time DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_pending_history
                    WHERE server NOT IN {5}
                    AND open_time BETWEEN DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                                AND date(CAST('{3}' AS TIMESTAMP))
                )
                WHERE rn = 1
            ),users_history AS (
                SELECT *
                FROM (
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY rtz_now DESC) as rnu
                    FROM {4}_DATAWAREHOUSE.{1}.crm_users_history
                    WHERE rtz_now < CAST('{3}' AS TIMESTAMP) + INTERVAL '20 MINUTES'
                )
                WHERE rnu=1
            ),
            users AS (
                SELECT cu.*
                    ,CASE
                        WHEN UPPER(cu.country) = ' OWITIWARAYA' THEN 'OWITIWARAYA'
                        WHEN UPPER(cu.country) = 'AU' THEN 'AUSTRALIA'
                        WHEN UPPER(cu.country) = 'DOMINICA' THEN 'DOMINICAN REPUBLIC'
                        WHEN UPPER(cu.country) = 'HONG KONG S.A.R.' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'HONG KONG SAR CHINA' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'HONGKONG' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'HONGKONG SAR CHINA' THEN 'HONG KONG SAR CHINA'
                        WHEN UPPER(cu.country) = 'MYANMAR [BURMA]' THEN 'MYANMAR'
                        WHEN UPPER(cu.country) = 'UK' THEN 'UNITED KINGDOM'
                        WHEN UPPER(cu.country) = 'USA' THEN 'UNITED STATES'
                        WHEN UPPER(cu.country) = 'EN' THEN 'UNKNOWN OR INVALID REGION'
                        WHEN UPPER(cu.country) = 'SEF' THEN 'UNKNOWN OR INVALID REGION'
                        WHEN UPPER(cu.country) = 'TW' THEN 'TAIWAN'
                        WHEN UPPER(cu.country) = '中國' THEN 'CHINA'
                        WHEN UPPER(cu.country) = '中国' THEN 'CHINA'
                        ELSE (CASE WHEN UPPER(regexp_replace(cu.country, '\\\d','')) = ''
                            THEN 'UNKNOWN OR INVALID REGION'
                            ELSE UPPER(regexp_replace(cu.country, '\\\d',''))
                            END)
                        END AS adj_country
                    ,cmahl.name AS member_acct_type
                    ,aerj.accounting_equity_report_jurisdiction AS report_jurisdiction
                FROM(
                    SELECT *
                        , ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) as rn
                    FROM users_history
                ) AS cu
                LEFT JOIN (
                    SELECT *
                    FROM {4}_DATAWAREHOUSE.{1}.crm_member_account_has_languages
                    WHERE language = 'en'
                ) AS cmahl
                ON cu.member_account_type = cmahl.member_account_type_id
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.accounting_equity_report_jurisdiction AS aerj
                ON cu.jurisdiction = aerj.user_jurisdiction
                WHERE rn=1
            ),
            symbol_groups AS (
                SELECT
                    symbol
                    ,symbol_group
                    ,server
                FROM(
                    SELECT
                        crm_external_symbols.name AS symbol
                        , crm_external_symbol_groups.name AS symbol_group
                        ,crm_servers.server
                        , ROW_NUMBER() OVER(PARTITION BY crm_external_symbols.name, crm_external_symbols.trading_server_id ORDER BY crm_external_symbols.updated_at DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.crm_external_symbols
                    LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers
                        ON crm_external_symbols.trading_server_id=crm_servers.crm_server_id
                    LEFT JOIN {4}_DATAWAREHOUSE.{1}.crm_external_symbol_groups
                    ON crm_external_symbol_groups.id = crm_external_symbols.external_symbol_group_id
                )
                WHERE rn=1
            ),trading_accounts_history AS (
            SELECT *
            FROM
            (
                SELECT crm_trading_accounts.*
                    , ROW_NUMBER() OVER(PARTITION BY crm_trading_accounts.id ORDER BY crm_trading_accounts.rtz_now DESC) as rn_tah
                FROM {1}.crm_trading_accounts_history AS crm_trading_accounts
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers cs
                    ON cs.crm_server_id = crm_trading_accounts.type
                WHERE (lower(cs.type) LIKE '%live%' or lower(cs.type) LIKE '%internal%') --TMGM live servers and CRM internal
                AND crm_trading_accounts.rtz_now < CAST('{3}' AS TIMESTAMP) + INTERVAL '20 MINUTES'
            )
            WHERE rn_tah=1
            ),
            trading_accounts AS (
                SELECT cta.*
                    ,cs.server
                    ,CASE
                        WHEN cta.is_ib = 1 THEN 'IB'
                        WHEN cta.parent_ta_id IS NOT NULL OR cta.parent_ta_id != 0
                            THEN 'Clients With IB'
                        WHEN cta.parent_ta_id IS NULL OR cta.parent_ta_id = 0
                            THEN 'Retail Client'
                        END AS member_type
                FROM(
                    SELECT
                        ta.*
                        , crm_serv.name AS trading_server_name
                        , crm_serv.id AS trading_server_id
                        , ROW_NUMBER() OVER(PARTITION BY ta.id ORDER BY ta.updated_at DESC) as rn
                    FROM trading_accounts_history ta
                    LEFT JOIN {4}_DATAWAREHOUSE.{1}.crm_trading_servers crm_serv
                    ON ta.type = crm_serv.id
                    WHERE ta.type NOT IN ({6}, {8})
                        AND ta.deleted_at IS NULL
                ) AS cta
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers AS cs
                ON cta.type = cs.crm_server_id
                WHERE rn=1
            ),
            trades_volume AS (
                SELECT
                        server
                        ,login
                        ,ROUND(SUM(swaps), 2) AS swaps
                        ,ROUND(SUM(commission), 2) AS commissions
                        ,ROUND(SUM(profit), 2) AS profit
                        ,ROUND(SUM(swaps+commission+profit), 2) AS sum_pnl
                        ,COALESCE(SUM(fx_vol) / 100.0,0) AS forex_volume
                        ,COALESCE(SUM(bullion_vol) / 100.0,0) AS bullion_volume
                        ,COALESCE(SUM(oil_vol) / 100.0,0) AS oil_volume
                        ,COALESCE(SUM(cfd_vol) / 100.0,0) AS cfd_volume
                        ,COALESCE(SUM(crypto_vol) / 100.0,0) AS crypto_volume
                        ,COALESCE(SUM(shares_vol) / 100.0,0) AS shares_volume
                FROM
                (
                    SELECT
                            server
                        ,login
                        ,CASE WHEN symbol_group = 'forex'
                                THEN volume
                            END AS fx_vol
                        ,CASE WHEN symbol_group = 'bullion'
                                THEN volume
                            END AS bullion_vol
                        ,CASE WHEN symbol_group = 'oil'
                                THEN volume
                            END AS oil_vol
                        ,CASE WHEN symbol_group = 'cfd'
                                THEN volume
                            END AS cfd_vol
                        ,CASE WHEN symbol_group = 'crypto'
                                THEN volume
                            END AS crypto_vol
                        ,CASE WHEN symbol_group = 'shares'
                                THEN volume
                            END AS shares_vol
                        ,swaps
                        ,commission
                        ,profit
                    FROM
                        (
                            SELECT
                                    rst.*
                                ,symbol_group
                            FROM rs_trades_closed AS rst
                            LEFT JOIN rs_trades_open AS rso
                            ON rst.server = rso.server
                                AND rst.ticket = rso.ticket
                            LEFT JOIN symbol_groups
                            ON symbol_groups.symbol = rst.symbol
                                AND symbol_groups.server=rst.server
                            WHERE rso.server IS NULL
                        )
                )
                GROUP BY server,login
            ),
            orders_count AS (
                SELECT
                        server
                    ,login
                    ,COUNT(count_open_orders) AS count_open_orders
                    ,COUNT(count_pending_orders) AS count_pending_orders
                    ,COUNT(count_closed_orders) AS count_closed_orders
                    ,SUM(time_diff)/COUNT(count_closed_orders) AS average_trade_duration
                FROM
                (
                    SELECT
                        DISTINCT
                                server
                            ,login
                            ,CASE WHEN close_time = CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 0 OR cmd = 1)
                                    THEN ticket
                            END AS count_open_orders
                            ,CASE WHEN close_time = CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 2 OR cmd = 3 OR cmd = 4 OR cmd = 5)
                                    THEN ticket
                            END AS count_pending_orders
                            ,CASE WHEN close_time != CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 0 OR cmd = 1 OR cmd = 6 OR cmd = 7)
                                    THEN ticket
                            END AS count_closed_orders
                            ,CASE WHEN close_time != CAST('1970-01-01 00:00:00.000' AS TIMESTAMP) AND (cmd = 0 OR cmd = 1)
                                    THEN DATEDIFF('seconds', open_time, close_time)
                            END AS time_diff
                    FROM
                    (
                        SELECT * FROM (
                            SELECT
                                *
                                ,ROW_NUMBER() OVER(PARTITION BY server, login, ticket ORDER BY modify_time DESC) AS rn_all
                            FROM (
                                SELECT * FROM rs_trades_open
                                UNION ALL
                                SELECT * FROM rs_trades_closed
                                UNION ALL
                                SELECT * FROM rs_trades_pending
                            )
                        )
                        WHERE rn_all = 1
                    )
                )
                GROUP BY server,login
            ),
            open_orders_count AS (
                SELECT server, login, count(distinct ticket) AS open_order_count
                FROM rs_trades_open_history
                GROUP BY server, login
            ),cu AS (
                    SELECT *
                    FROM (
                        SELECT
                            *
                            ,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS RN
                        FROM users_history
                    )
                    WHERE RN = 1
                ),ctran AS 
                    (
                        SELECT 
                            *
                        FROM
                            (
                                SELECT 
                                    ct.*,
                                    cu.sales_code AS user_salescode,
                                    ROW_NUMBER() OVER(PARTITION BY ct.id ORDER BY ct.updated_at desc) AS rn
                                FROM {4}_DATAWAREHOUSE.{1}.crm_transactions AS ct
                                LEFT JOIN cu 
                                    ON ct.user_id=cu.id
                                WHERE external_id like '%W%'
                                    AND ct.status =2 
                                    AND ct.deleted_at is null
                                    AND DATE(ct.completed_at) >=  IFF(DAY(CAST('{3}' AS TIMESTAMP)) = 1,
                                                                                        DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY',
                                                                                        DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY')
                                    AND DATE(ct.completed_at) < DATE(cast('{3}' as timestamp))
                            )
                        WHERE rn =1
                            AND lower(user_salescode) NOT LIKE '%9999%'
                            AND lower(user_salescode) NOT LIKE '%test%'
                    ),wallet_transactions AS (
                        SELECT
                                server
                            ,login
                            ,COALESCE(ROUND(SUM(sales_deposit),2),0) AS sales_deposit
                            ,COALESCE(ROUND(SUM(sales_withdrawal),2),0) AS sales_withdrawal
                            ,COALESCE(ROUND(SUM(sales_net_deposit),2),0) AS sales_net_deposit
                            ,COALESCE(ROUND(SUM(deposit),2),0) AS deposit
                            ,COALESCE(ROUND(SUM(withdrawal),2),0) AS withdrawal
                            ,COALESCE(ROUND(SUM(net_deposit),2),0) AS net_deposit
                            ,COALESCE(ROUND(SUM(credit_change),2),0) AS credit_change
                            ,COALESCE(ROUND(SUM(internal_transfer),2),0) AS internal_transfer
                            ,COALESCE(ROUND(SUM(internal_transfer_deposit_from_ib),2),0) AS internal_transfer_deposit_from_ib
                            ,COALESCE(ROUND(SUM(internal_transfer_withdrawal_to_ib),2),0) AS internal_transfer_withdrawal_to_ib
                            ,COALESCE(ROUND(SUM(internal_transfer_from_same_category),2),0) AS internal_transfer_from_same_category
                            ,COALESCE(ROUND(SUM(internal_transfer_to_same_category),2),0) AS internal_transfer_to_same_category
                            ,COALESCE(ROUND(SUM(credit),2),0) AS credit
                            ,COALESCE(ROUND(SUM(points),2),0) AS points
                            ,COALESCE(ROUND(SUM(CFD),2),0) AS CFD
                            ,COALESCE(ROUND(SUM(rollover),2),0) AS rollover
                            ,COALESCE(ROUND(SUM(MISC_OP),2),0) AS MISC_OP
                            ,COALESCE(ROUND(SUM("SPLIT"),2),0) AS "SPLIT"
                            ,COALESCE(ROUND(SUM(Compensation),2),0) AS Compensation
                            ,COALESCE(ROUND(SUM(MISC_Clear),2),0) AS MISC_Clear
                            ,COALESCE(ROUND(SUM(MISC_TR),2),0) AS MISC_TR
                            ,COALESCE(ROUND(SUM(MISC_FUNDING),2),0) AS MISC_FUNDING
                            ,COALESCE(ROUND(SUM(promo),2),0) AS promo
                            ,COALESCE(ROUND(SUM(ib_rebate),2),0) AS ib_rebate
                            ,COALESCE(ROUND(SUM(auto_rebate),2),0) AS auto_rebate
                            ,COALESCE(ROUND(SUM(CPA_rebate),2),0) AS CPA_rebate
                            ,COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                                + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_rebate
                            ,COALESCE(ROUND(SUM(net_deposit),2),0) + COALESCE(ROUND(SUM(internal_transfer),2),0)
                                + COALESCE(ROUND(SUM(credit),2),0) + COALESCE(ROUND(SUM(points),2),0)
                                + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(rollover),2),0)
                                + COALESCE(ROUND(SUM(MISC_OP),2),0) + COALESCE(ROUND(SUM("SPLIT"),2),0)
                                -- + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(compensation),2),0)
                                + COALESCE(ROUND(SUM(compensation),2),0)
                                + COALESCE(ROUND(SUM(MISC_Clear),2),0) + COALESCE(ROUND(SUM(MISC_TR),2),0)
                                + COALESCE(ROUND(SUM(MISC_FUNDING),2),0) + COALESCE(ROUND(SUM(promo),2),0)
                                + COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                                + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_txn
                        FROM
                        (
                            SELECT
                                    'wallet' AS server
                                ,external_id AS login
                                ,CASE WHEN type in (1,6)
                                        THEN fund_amount
                                    END AS sales_deposit
                                ,CASE WHEN type in (2,5)
                                        THEN fund_amount
                                    END AS sales_withdrawal
                                ,CASE WHEN type in (1,2,5,6)
                                        THEN fund_amount
                                    END AS sales_net_deposit
                                ,CASE WHEN type in (1,6) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) != 'Internal_Transfer'
                                        THEN fund_amount
                                    END AS deposit
                                ,CASE WHEN type in (2,5) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) != 'Internal_Transfer'
                                        THEN fund_amount
                                    END AS withdrawal
                                ,CASE WHEN type in (1,2,5,6) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) != 'Internal_Transfer'
                                        THEN fund_amount
                                    END AS net_deposit
                                ,CASE WHEN type = 4
                                        THEN fund_amount
                                    END AS credit_change
                                ,CASE WHEN type in (3,{9})
                                        THEN fund_amount
                                    END AS internal_transfer
                                ,CASE WHEN type in (3,{9}) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_DEPOSIT_FROM_IB'
                                        THEN fund_amount
                                    END AS internal_transfer_deposit_from_ib
                                ,CASE WHEN type in (3,{9}) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_WITHDRAWAL_TO_IB'
                                        THEN fund_amount
                                    END AS internal_transfer_withdrawal_to_ib
                                ,CASE WHEN type in (3) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_FROM_SAME_CATEGORY'
                                        THEN fund_amount
                                    END AS internal_transfer_from_same_category
                                ,CASE WHEN type in (3) AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_TO_SAME_CATEGORY'
                                        THEN fund_amount
                                    END AS internal_transfer_to_same_category
                                ,CASE WHEN type in (4) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Credit'
                                        THEN fund_amount
                                    ELSE 0 END AS credit
                                ,CASE WHEN type in (13) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Points'
                                        THEN fund_amount
                                    ELSE 0 END AS points
                                ,CASE WHEN type in (7) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'CFD'
                                        THEN fund_amount
                                    ELSE 0 END AS CFD
                                ,CASE WHEN type in (9) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Rollover'
                                        THEN fund_amount
                                    ELSE 0 END AS rollover
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC OP'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_OP
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Split'
                                        THEN fund_amount
                                    ELSE 0 END AS "SPLIT"
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Compensation'
                                        THEN fund_amount
                                    ELSE 0 END AS Compensation
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC Clear'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_Clear
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC TR'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_TR
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC FUNDING'
                                        THEN fund_amount
                                    ELSE 0 END AS MISC_FUNDING
                                -- promo not in txn types ！！！
                                -- need to be confirmed
                                ,CASE WHEN type in (10) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Promo'
                                        THEN fund_amount
                                    ELSE 0 END AS promo
                                ,CASE WHEN type in (11) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'IB Rebate'
                                        THEN fund_amount
                                    ELSE 0 END AS ib_rebate
                                ,CASE WHEN type in (11) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'Auto Rebate'
                                        THEN fund_amount
                                    ELSE 0 END AS auto_rebate
                                ,CASE WHEN type in (11) AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'CPA Rebate'
                                        THEN fund_amount
                                    ELSE 0 END AS CPA_rebate
                            FROM
                                ctran
                            LEFT JOIN {4}_DATAWAREHOUSE.{2}.TRANSACTION_COMMENT_MAPPING AS tcm
                            ON LOWER(ctran.trading_server_comment) LIKE LOWER(tcm.COMMENT_TEMPLATE)
                        )
                GROUP BY server,login
            ),
            transactions AS (
                SELECT
                        server
                    ,login
                    ,COALESCE(ROUND(SUM(sales_deposit),2),0) AS sales_deposit
                    ,COALESCE(ROUND(SUM(sales_withdrawal),2),0) AS sales_withdrawal
                    ,COALESCE(ROUND(SUM(sales_net_deposit),2),0) AS sales_net_deposit
                    ,COALESCE(ROUND(SUM(deposit),2),0) AS deposit
                    ,COALESCE(ROUND(SUM(withdrawal),2),0) AS withdrawal
                    ,COALESCE(ROUND(SUM(net_deposit),2),0) AS net_deposit
                    ,COALESCE(ROUND(SUM(credit_change),2),0) AS credit_change
                    ,COALESCE(ROUND(SUM(internal_transfer),2),0) AS internal_transfer
                    ,COALESCE(ROUND(SUM(internal_transfer_deposit_from_ib),2),0) AS internal_transfer_deposit_from_ib
                    ,COALESCE(ROUND(SUM(internal_transfer_withdrawal_to_ib),2),0) AS internal_transfer_withdrawal_to_ib
                    ,COALESCE(ROUND(SUM(internal_transfer_from_same_category),2),0) AS internal_transfer_from_same_category
                    ,COALESCE(ROUND(SUM(internal_transfer_to_same_category),2),0) AS internal_transfer_to_same_category
                    ,COALESCE(ROUND(SUM(credit),2),0) AS credit
                    ,COALESCE(ROUND(SUM(points),2),0) AS points
                    ,COALESCE(ROUND(SUM(CFD),2),0) AS CFD
                    ,COALESCE(ROUND(SUM(rollover),2),0) AS rollover
                    ,COALESCE(ROUND(SUM(MISC_OP),2),0) AS MISC_OP
                    ,COALESCE(ROUND(SUM("SPLIT"),2),0) AS "SPLIT"
                    ,COALESCE(ROUND(SUM(Compensation),2),0) AS Compensation
                    ,COALESCE(ROUND(SUM(MISC_Clear),2),0) AS MISC_Clear
                    ,COALESCE(ROUND(SUM(MISC_TR),2),0) AS MISC_TR
                    ,COALESCE(ROUND(SUM(MISC_FUNDING),2),0) AS MISC_FUNDING
                    ,COALESCE(ROUND(SUM(promo),2),0) AS promo
                    ,COALESCE(ROUND(SUM(ib_rebate),2),0) AS ib_rebate
                    ,COALESCE(ROUND(SUM(auto_rebate),2),0) AS auto_rebate
                    ,COALESCE(ROUND(SUM(CPA_rebate),2),0) AS CPA_rebate
                    ,COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                        + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_rebate
                    ,COALESCE(ROUND(SUM(net_deposit),2),0) + COALESCE(ROUND(SUM(internal_transfer),2),0)
                        + COALESCE(ROUND(SUM(credit),2),0) + COALESCE(ROUND(SUM(points),2),0)
                        + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(rollover),2),0)
                        + COALESCE(ROUND(SUM(MISC_OP),2),0) + COALESCE(ROUND(SUM("SPLIT"),2),0)
                        -- + COALESCE(ROUND(SUM(CFD),2),0) + COALESCE(ROUND(SUM(compensation),2),0)
                        + COALESCE(ROUND(SUM(compensation),2),0)
                        + COALESCE(ROUND(SUM(MISC_Clear),2),0) + COALESCE(ROUND(SUM(MISC_TR),2),0)
                        + COALESCE(ROUND(SUM(MISC_FUNDING),2),0) + COALESCE(ROUND(SUM(promo),2),0)
                        + COALESCE(ROUND(SUM(ib_rebate),2),0) + COALESCE(ROUND(SUM(auto_rebate),2),0)
                        + COALESCE(ROUND(SUM(CPA_rebate),2),0) AS tot_txn
                FROM
                (
                    SELECT
                        server
                        ,login
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                THEN profit
                                ELSE 0 END AS deposit
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                    OR tcm.SALES_TYPE = 'Deposit')
                                THEN profit
                                ELSE 0 END AS sales_deposit
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal'
                                THEN profit
                                ELSE 0 END AS withdrawal
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal'
                                    OR tcm.SALES_TYPE = 'Withdrawal')
                                THEN profit
                                ELSE 0 END AS sales_withdrawal
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                    OR SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal')
                                THEN profit
                                ELSE 0 END AS net_deposit
                        ,CASE WHEN cmd = 6 AND (SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Deposit'
                                    OR tcm.SALES_TYPE = 'Deposit'
                                    OR SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Withdrawal'
                                    OR tcm.SALES_TYPE = 'Withdrawal')
                                THEN profit
                                ELSE 0 END AS sales_net_deposit
                        ,CASE WHEN cmd = 7
                                THEN profit
                                ELSE 0 END AS credit_change
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Internal_Transfer'
                                THEN profit
                                ELSE 0 END AS internal_transfer
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_DEPOSIT_FROM_IB'
                                THEN profit
                                ELSE 0 END AS internal_transfer_deposit_from_ib
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_WITHDRAWAL_TO_IB'
                                THEN profit
                                ELSE 0 END AS internal_transfer_withdrawal_to_ib
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_FROM_SAME_CATEGORY'
                                THEN profit
                                ELSE 0 END AS internal_transfer_from_same_category
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', -1) = 'INTERNAL_TRANSFER_TO_SAME_CATEGORY'
                                THEN profit
                                ELSE 0 END AS internal_transfer_to_same_category
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Credit'
                                THEN profit
                                ELSE 0 END AS credit
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Points'
                                THEN profit
                                ELSE 0 END AS points
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'CFD'
                                THEN profit
                                ELSE 0 END AS CFD
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Rollover'
                                THEN profit
                                ELSE 0 END AS rollover
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC OP'
                                THEN profit
                                ELSE 0 END AS MISC_OP
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Split'
                                THEN profit
                                ELSE 0 END AS "SPLIT"
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Compensation'
                                THEN profit
                                ELSE 0 END AS compensation
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC Clear'
                                THEN profit
                                ELSE 0 END AS MISC_Clear
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC TR'
                                THEN profit
                                ELSE 0 END AS MISC_TR
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'MISC FUNDING'
                                THEN profit
                                ELSE 0 END AS MISC_FUNDING
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 1) = 'Promo'
                                THEN profit
                                ELSE 0 END AS promo
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'IB Rebate'
                                THEN profit
                                ELSE 0 END AS ib_rebate
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'Auto Rebate'
                                THEN profit
                                ELSE 0 END AS auto_rebate
                        ,CASE WHEN cmd = 6 AND SPLIT_PART(tcm.REPORT_TYPE, '-', 2) = 'CPA Rebate'
                                THEN profit
                                ELSE 0 END AS CPA_rebate
                    FROM
                        rstt
                    LEFT JOIN {4}_DATAWAREHOUSE.{2}.TRANSACTION_COMMENT_MAPPING AS tcm
                    ON LOWER(rstt.COMMENT) LIKE LOWER(tcm.COMMENT_TEMPLATE)
                    )
                GROUP BY server,login
            ),
            rebate_rate AS (
                SELECT
                    fx.trading_server_group_id
                    ,rbc AS forex_rebate
                    ,bul.bul_rebate AS bul_rebate
                    ,oil.oil_rebate AS oil_rebate
                    ,cfd.cfd_rebate AS cfd_rebate
                    ,crypto.crypto_rebate AS crypto_rebate
                    ,shares.shares_rebate AS shares_rebate
                FROM {4}_DATAWAREHOUSE.{1}.crm_trading_server_group_has_commission_limits AS fx
                LEFT JOIN (
                    SELECT
                        trading_server_group_id
                        ,rbc AS bul_rebate
                    FROM {4}_DATAWAREHOUSE.{1}.crm_trading_server_group_has_commission_limits
                    WHERE external_symbol_group_id = 2
                ) AS bul
                ON fx.trading_server_group_id = bul.trading_server_group_id
                LEFT JOIN (
                    SELECT
                        trading_server_group_id
                        ,rbc AS oil_rebate
                    FROM {4}_DATAWAREHOUSE.{1}.crm_trading_server_group_has_commission_limits
                    WHERE external_symbol_group_id = 3
                ) AS oil
                ON fx.trading_server_group_id = oil.trading_server_group_id
                LEFT JOIN (
                    SELECT
                        trading_server_group_id
                        ,rbc AS cfd_rebate
                    FROM {4}_DATAWAREHOUSE.{1}.crm_trading_server_group_has_commission_limits
                    WHERE external_symbol_group_id = 4
                ) AS cfd
                ON fx.trading_server_group_id = cfd.trading_server_group_id
                LEFT JOIN (
                    SELECT
                        trading_server_group_id
                        ,rbc AS crypto_rebate
                    FROM {4}_DATAWAREHOUSE.{1}.crm_trading_server_group_has_commission_limits
                    WHERE external_symbol_group_id = 5
                ) AS crypto
                ON fx.trading_server_group_id = crypto.trading_server_group_id
                LEFT JOIN (
                    SELECT
                        trading_server_group_id
                        ,rbc AS shares_rebate
                    FROM {4}_DATAWAREHOUSE.{1}.crm_trading_server_group_has_commission_limits
                    WHERE external_symbol_group_id = 17
                ) AS shares
                ON fx.trading_server_group_id = shares.trading_server_group_id
                WHERE external_symbol_group_id = 1
            ),wallet_balance AS
            (
                SELECT 
                    external_id,
                    type,
                    balance AS wallet_balance
                FROM  {4}_DATAWAREHOUSE.{1}.crm_trading_accounts_daily 
                WHERE external_id LIKE '%W%'
                    AND deleted_at IS NULL
                AND DATE(utc_now) = DATE(cast('{3}' as timestamp) - INTERVAL '1 DAY')
            ),
            first_ta AS
            (
                SELECT 
                    COALESCE(mfta.id,mibta.id) AS user_id,
                    member_first_ta_registration_date,
                    member_first_ib_ta_registration_date
                FROM
                    (
                        SELECT 
                            cu.id,
                            MIN(ta.created_at) AS member_first_ta_registration_date
                        FROM cu 
                        LEFT JOIN trading_accounts AS ta
                            ON cu.id = ta.user_id
                        WHERE ta.salescode NOT LIKE '%9999%'
                            AND lower(ta.salescode) NOT LIKE '%test%'
                            AND external_id NOT LIKE '%W%'
                            AND ta.is_ib =0
                        GROUP BY cu.id
                    ) AS mfta 
                    FULL OUTER JOIN
                    (
                        SELECT 
                            cu.id,
                            MIN(ta.created_at) AS member_first_ib_ta_registration_date
                        FROM cu 
                        LEFT JOIN trading_accounts AS ta
                            ON cu.id = ta.user_id
                        WHERE ta.salescode NOT LIKE '%9999%'
                            AND lower(ta.salescode) NOT LIKE '%test%'
                            AND external_id NOT LIKE '%W%'
                            AND ta.is_ib =1
                        GROUP BY cu.id    
                    ) AS mibta
                    ON mfta.id=mibta.id
            ),
            all_accounts_info AS (
                SELECT
                    IFF(all_ta.login LIKE 'W%', ta.server, all_ta.server) AS server
                    ,all_ta.login AS login
                    ,IFF(all_ta.login LIKE 'W%', ta.trading_server_name, cs.crm_server_name) AS trading_server_name
                    ,COALESCE(cs.crm_server_id, ta.type) AS type
                    ,rsu."GROUP"
                    ,ta.user_id AS user_id
                    ,ta.salescode AS salescode
                    ,ta.balance AS points_balance
                    ,CASE WHEN ta.is_ib = 1
                            THEN 'True'
                            ELSE 'False'
                            END AS is_IB
                    ,users.sales_code AS member_sales_code
                    ,users.lead_source AS member_lead_source
                    ,ta2.external_id AS agent
                    ,jurisdiction
                    ,users.created_at AS member_registration_date
                    ,ft.member_first_ta_registration_date
                    ,ft.member_first_ib_ta_registration_date
                    ,rsu.credit
                    ,CASE WHEN rsu.credit > 0 AND rsu.equity < 0
                            THEN rsu.credit * -1
                        WHEN rsu.credit > 0 AND rsu.equity >= 0 AND rsu.equity < rsu.credit
                            THEN rsu.equity - rsu.credit
                        ELSE 0
                    END AS used_credit
                    ,rsu.equity
                    ,CASE WHEN rsu.equity - rsu.balance < 0 AND rsu.balance < 0
                            THEN 0
                        WHEN rsu.equity - rsu.credit < 0
                            THEN 0
                        ELSE ROUND(rsu.equity - rsu.credit, 2)
                    END AS positive_equity
                    ,rsu.balance
                    ,CASE WHEN rsu.balance > 0
                            THEN rsu.balance
                        ELSE 0
                    END AS positive_balance
                    ,CASE WHEN rsu.balance < 0
                        THEN rsu.balance
                        ELSE 0
                    END AS negative_balance
                    ,rsu.currency
                    ,users.adj_country AS country
                    ,rsu.regdate AS ta_registration_date
                    ,rsu.margin
                    ,rsu.margin_free
                    ,rsu.margin_level
                    ,rsu.leverage
                    ,rsu.comment
                    ,rsu.lead_source AS ta_lead_source
                    ,rebate_rate.forex_rebate AS fx_rebate
                    ,rebate_rate.bul_rebate AS bul_rebate
                    ,rebate_rate.oil_rebate AS oil_rebate
                    ,rebate_rate.cfd_rebate AS cfd_rebate
                    ,rebate_rate.crypto_rebate AS crypto_rebate
                    ,rebate_rate.shares_rebate AS shares_rebate
                    ,users.member_acct_type AS member_account_type
                    ,ta.member_type
                    ,IFF(rsu."GROUP" LIKE '%va%', 'Vanuatu', users.report_jurisdiction) AS report_jurisdiction
                FROM (
                    SELECT DISTINCT server, CAST(login AS VARCHAR) AS login
                    FROM rsuh
                    UNION ALL
                    SELECT null AS server, external_id AS login
                    FROM trading_accounts
                    WHERE type IN ({7})
                ) AS all_ta
                LEFT JOIN {4}_DATAWAREHOUSE.{2}.crm_servers AS cs
                ON all_ta.server = cs.server
                LEFT JOIN trading_accounts ta
                ON all_ta.login = ta.external_id
                    AND (all_ta.server = ta.server OR all_ta.login LIKE 'W%')
                LEFT JOIN trading_accounts ta2
                ON ta.parent_ta_id = ta2.id
                LEFT JOIN rsuh AS rsu
                ON all_ta.login = CAST(rsu.login AS VARCHAR(50))
                    AND rsu.server = all_ta.server
                LEFT JOIN {4}_DATAWAREHOUSE.{1}.crm_trading_server_groups ctsg
                ON ta.trading_server_id = ctsg.trading_server_id
                    AND rsu."GROUP"=ctsg."group"
                LEFT JOIN users
                ON ta.user_id = users.id
                LEFT JOIN rebate_rate
                ON ctsg.id = rebate_rate.trading_server_group_id
                LEFT JOIN first_ta AS ft
                    ON ta.user_id = ft.user_id
                WHERE ((LOWER(users.sales_code) NOT LIKE '%test%'
                    AND LOWER(users.sales_code) NOT LIKE '%hedge%'
                    AND LOWER(users.sales_code) NOT LIKE '%nosales%'
                    AND LOWER(users.sales_code) NOT LIKE '%9999%')
                    OR users.sales_code IS NULL)
            ),
            -- daily level symbol/currency USD conversion ratio
            ex_rate AS (
                SELECT DISTINCT
                   DATE(pricing.timereceived_server) AS dt_report -- every day will only have 1 close
                  ,pricing.core_symbol
                  ,symbol_spec.symbol
                  ,CASE
                     WHEN symbol_spec.symbol = 'USDCNH' THEN 'CNH'
                     WHEN symbol_spec.symbol = 'EURUSD' THEN 'EUR'
                     WHEN symbol_spec.symbol = 'NZDUSD' THEN 'NZD'
                     WHEN symbol_spec.symbol = 'GBPUSD' THEN 'GBP'
                     WHEN symbol_spec.symbol = 'USDHKD' THEN 'HKD'
                     WHEN symbol_spec.symbol = 'AUDUSD' THEN 'AUD'
                     WHEN symbol_spec.symbol = 'USDCAD' THEN 'CAD'
                     WHEN symbol_spec.symbol = 'USDCHF' THEN 'CHF'
                     WHEN symbol_spec.symbol = 'USDJPY' THEN 'JPY'
                     ELSE 'UNK'
                   END AS ac
                  ,FIRST_VALUE(pricing.close) OVER(PARTITION BY pricing.core_symbol, DATE(pricing.timereceived_server)
                      ORDER BY pricing.timereceived_server DESC) AS close
                  ,symbol_spec.contractsize
                  ,symbol_spec.basecurrency
                  ,symbol_spec.type
                FROM {4}_DATAWAREHOUSE.{1}.pricing_ohlc_minute  AS pricing
                LEFT JOIN {4}_DATAWAREHOUSE.{1}.symbol_settings AS symbol_spec -- get the symbol specification bases on received time
                  ON replace(pricing.core_symbol,'/','') = symbol_spec.symbol
                 AND pricing.TIMERECEIVED_SERVER::DATE >= symbol_spec.START_DATE
                 AND pricing.TIMERECEIVED_SERVER::DATE <= symbol_spec.LAST_ACTIVE
                WHERE pricing.core_symbol IN ('EUR/USD','AUD/USD','GBP/USD','USD/CAD','USD/HKD','NZD/USD','USD/CNH','USD/CHF','USD/JPY') -- limit to forex symbol
                  AND symbol_spec.symbol IS NOT NULL
                  AND pricing.TIMERECEIVED_SERVER::DATE >=  DATE('{3}') - INTERVAL '14 DAY' -- need check it later
            ), 
            last_report AS (
                -- first intraday report from t-1
                SELECT *
                FROM {4}_DATAWAREHOUSE.{2}.accounting_t_1_equity_report
                WHERE dt_report = DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
            ),
            -- first record for each login to get equity at TA open
            first_report AS (
                SELECT
                   user_id -- some user_id is null
                  ,login
                  ,trading_server
                  ,positive_equity
                  ,ROW_NUMBER() OVER (PARTITION BY user_id, login, trading_server ORDER BY dt_report) AS rn
                FROM {4}_DATAWAREHOUSE.{2}.accounting_t_1_equity_report
                --WHERE dt_report <= DATE_TRUNC('DAY', CAST('{3}' AS TIMESTAMP)) - INTERVAL '1 DAY'
                WHERE user_id IS NOT NULL
                QUALIFY rn = 1
            ),
            trades_txn AS (
                SELECT 
                   login
                  ,server
                  ,sum(profit) as profit_sum
                  ,DATE(max(open_time)) AS dt_report -- might not consider this date, but just get the latest day of USD exchange rate
                FROM (
                    SELECT 
                       ticket
                      ,login
                      ,server
                      ,profit
                      ,cmd
                      ,open_time
                      ,ROW_NUMBER() OVER(PARTITION BY server, ticket ORDER BY utc_now DESC) as rn
                    FROM {4}_DATAWAREHOUSE.{1}.report_server_trades_transactions
                    WHERE server NOT IN {5}
                      AND DATE(concat(CAST(year AS VARCHAR),'-',CAST(month AS VARCHAR),'-',CAST(day AS VARCHAR))) <= DATE(TO_VARCHAR(CAST('{3}' AS TIMESTAMP), 'yyyy-mm-dd'))
                      AND open_time <= DATE(CAST('{3}' AS TIMESTAMP))
                      AND LOWER(comment) NOT like '%from master trade%'
                    QUALIFY rn = 1
                        AND cmd != 7  -- credit
                        --AND open_time < IFF(MINUTE(TO_RTZ(SYSDATE())) >= 30, DATE_TRUNC('HOUR',TO_RTZ(SYSDATE())) + INTERVAL '30 MINUTE', DATE_TRUNC('HOUR',TO_RTZ(SYSDATE())))
                )
                GROUP BY 1, 2
            ),
            -- local currency
            t_1_equity_lc AS (
                SELECT
                    DISTINCT
                    IFF(MINUTE(CAST('{3}' AS TIMESTAMP)) >= 30,
                        DATE_TRUNC('HOUR', CAST('{3}' AS TIMESTAMP)) + INTERVAL '30 MINUTE',
                        DATE_TRUNC('HOUR', CAST('{3}' AS TIMESTAMP)) ) AS dt_report
                    ,TO_VARCHAR(TO_RTZ(SYSDATE())::TIMESTAMP, 'yyyy-mm-dd hh:mm:ss') AS dt_report_insert
                    ,all_accounts_info.jurisdiction AS report_jurisdiction
                    ,all_accounts_info.login AS login
                    ,all_accounts_info.is_IB AS is_ib
                    ,all_accounts_info."GROUP" AS "GROUP"
                    ,all_accounts_info.user_id AS user_id
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.deposit, transactions.deposit),0) AS deposit
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.withdrawal, transactions.withdrawal),0) AS withdraw
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.net_deposit, transactions.net_deposit),0) AS net_deposit
                    ,COALESCE(all_accounts_info.credit,0) AS credit
                    ,COALESCE(transactions.credit_change,wt.credit_change,0) AS credit_change
                    ,COALESCE(trades_volume.swaps,0) AS swap
                    ,COALESCE(trades_volume.commissions,0) AS commission
                    ,all_accounts_info.agent AS agent
                --         ,all_accounts_info.agent_copy AS "AGENT(COPY)"
                    ,COALESCE(all_accounts_info.fx_rebate,0) AS forex_rebate
                    ,COALESCE(all_accounts_info.bul_rebate,0) AS bullion_rebate
                    ,COALESCE(all_accounts_info.oil_rebate,0) AS oil_rebate
                    ,COALESCE(all_accounts_info.cfd_rebate,0) AS cfd_rebate
                    ,COALESCE(all_accounts_info.crypto_rebate,0) AS crypto_rebate
                    ,COALESCE(all_accounts_info.shares_rebate,0) AS shares_rebate
                    ,COALESCE(ROUND(IFF(all_accounts_info.type = 9, 0,
                        trades_volume.forex_volume*all_accounts_info.fx_rebate + trades_volume.bullion_volume*all_accounts_info.bul_rebate + trades_volume.oil_volume*all_accounts_info.oil_rebate
                        + trades_volume.cfd_volume*all_accounts_info.cfd_rebate + trades_volume.crypto_volume*all_accounts_info.crypto_rebate + trades_volume.shares_volume*all_accounts_info.shares_rebate), 2), 0) AS trade_cost
                    -- eq change + total_cost
                    ,COALESCE(ROUND(IFF(all_accounts_info.type = 9, 0,
                        all_accounts_info.positive_equity - COALESCE(lr.positive_equity, 0) - COALESCE(transactions.tot_txn,0) + COALESCE(trades_volume.forex_volume*all_accounts_info.fx_rebate + trades_volume.bullion_volume*all_accounts_info.bul_rebate + trades_volume.oil_volume*all_accounts_info.oil_rebate
                        + trades_volume.cfd_volume*all_accounts_info.cfd_rebate + trades_volume.crypto_volume*all_accounts_info.crypto_rebate + trades_volume.shares_volume*all_accounts_info.shares_rebate, 0)), 2), 0) AS cost_to_loss
                    ,COALESCE(trades_volume.forex_volume,0) AS forex_vol
                    ,COALESCE(trades_volume.bullion_volume,0) AS bullion_vol
                    ,COALESCE(trades_volume.oil_volume,0) AS oil_vol
                    ,COALESCE(trades_volume.cfd_volume,0) AS cfd_vol
                    ,COALESCE(trades_volume.crypto_volume,0) AS crypto_vol
                    ,COALESCE(trades_volume.shares_volume,0) AS shares_vol
                    ,COALESCE(trades_volume.profit,0)  AS profit
                    ,COALESCE(IFF(all_accounts_info.type = 9, 0, trades_volume.sum_pnl), 0) AS pnl
                    ,COALESCE(all_accounts_info.equity,0) AS equity
                    ,COALESCE(all_accounts_info.positive_equity ,0) AS positive_equity
                    -- last report
                    ,COALESCE(ROUND(IFF(all_accounts_info.type = 9, 0, all_accounts_info.positive_equity - COALESCE(transactions.tot_txn,0) - COALESCE(lr.positive_equity, 0)), 2), 0) AS equity_change
                    ,COALESCE(IFF(all_accounts_info.type = 9, wtb.wallet_balance, all_accounts_info.balance) , 0) AS last_balance
                    ,COALESCE(IFF(all_accounts_info.type = 9, wtb.wallet_balance, all_accounts_info.positive_balance), 0) AS positive_balance
                    ,COALESCE(IFF(all_accounts_info.type = 9, 0, all_accounts_info.used_credit) ,0) AS used_credit
                    ,COALESCE(IFF(all_accounts_info.type = 9, 0, all_accounts_info.negative_balance) ,0) AS negative_balance
                    ,IFF(all_accounts_info.type = 9, 'POINTS', all_accounts_info.currency) AS currency
                    ,all_accounts_info.country AS country
                    ,all_accounts_info.member_sales_code AS member_sales_code
                    ,all_accounts_info.salescode AS ta_sales_code
                    ,COALESCE(all_accounts_info.margin,0) AS margin
                    ,COALESCE(all_accounts_info.margin_free,0) AS margin_free
                    ,COALESCE(all_accounts_info.margin_level,0) AS margin_level
                    ,COALESCE(all_accounts_info.leverage,0) AS leverage
                    -- last report
                    ,ROUND(COALESCE(IFF(all_accounts_info.type = 9, wtb.wallet_balance, all_accounts_info.used_credit) ,0) - COALESCE(lr.used_credit, 0), 2) AS used_credit_change
                    -- last report
                    ,ROUND(COALESCE(IFF(all_accounts_info.type = 9, wtb.wallet_balance, all_accounts_info.negative_balance) ,0) - COALESCE(lr.negative_balance, 0), 2) AS negative_balance_change
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.internal_transfer, transactions.internal_transfer),0) AS internal_transfer
                    -- remove
                    ,0 AS rebate
                    ,0 AS instant_rebate
                    ,0 AS adjustment
                    ,0 AS misc
                    ,all_accounts_info.member_registration_date AS member_register_date
                    ,all_accounts_info.ta_registration_date AS ta_register_date
                    ,all_accounts_info.member_first_ta_registration_date AS member_first_ta_registration_date
                    ,all_accounts_info.member_first_ib_ta_registration_date AS member_first_ib_ta_registration_date
                    ,all_accounts_info.jurisdiction AS jurisdiction
                    ,CASE WHEN all_accounts_info."GROUP" LIKE '%va%' THEN 'Vanuatu'
                        ELSE all_accounts_info.jurisdiction END AS final_jurisdiction
                    ,all_accounts_info.trading_server_name AS trading_server
                    ,all_accounts_info.comment AS comment
                    ,all_accounts_info.member_lead_source AS member_lead_source
                    ,all_accounts_info.ta_lead_source AS ta_lead_source
                    ,COALESCE(open_orders_count.open_order_count,0) AS count_open_orders
                    ,COALESCE(orders_count.count_pending_orders,0) AS count_pending_orders
                    ,COALESCE(orders_count.count_closed_orders,0) AS count_closed_orders
                    ,COALESCE(orders_count.average_trade_duration,0) AS average_trade_duration
                    ,COALESCE(all_accounts_info.member_account_type,'') AS member_account_type
                    ,COALESCE(all_accounts_info.member_type,'') AS member_type
                    ,CASE WHEN rs.name IS NULL THEN 0 ELSE 1 END AS is_cimb
                    ,CASE WHEN rs.name IS NULL THEN '' 
                        WHEN rs.name like '%iress%' THEN 'iress' 
                        WHEN rs.name like '%insight%' THEN 'insights' END AS cimb_name 
                    ,CASE WHEN rs.name IS NULL THEN '' 
                        ELSE CONCAT('TM',split(LOWER(rs.name),'tm')[1]) END AS cimb_cfd
                    ,TO_VARCHAR('{3}'::TIMESTAMP, 'yyyy') AS year
                    ,TO_VARCHAR('{3}'::TIMESTAMP, 'mm') AS month
                    ,TO_VARCHAR('{3}'::TIMESTAMP, 'dd') AS day
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.sales_deposit, transactions.sales_deposit),0) AS sales_deposit
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.sales_withdrawal, transactions.sales_withdrawal),0) AS sales_withdraw
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.sales_net_deposit, transactions.sales_net_deposit),0) AS sales_net_deposit
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.internal_transfer_deposit_from_ib, transactions.internal_transfer_deposit_from_ib),0) AS internal_transfer_deposit_from_ib
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.internal_transfer_withdrawal_to_ib, transactions.internal_transfer_withdrawal_to_ib),0) AS internal_transfer_withdrawal_to_ib
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.internal_transfer_from_same_category, transactions.internal_transfer_from_same_category),0) AS internal_transfer_from_same_category
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.internal_transfer_to_same_category, transactions.internal_transfer_to_same_category),0) AS internal_transfer_to_same_category
                    -- new category
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.credit, transactions.credit),0) AS credit_txn
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.points, transactions.points),0) AS points
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.CFD, transactions.CFD),0) AS CFD
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.rollover, transactions.rollover),0) AS rollover
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_OP, transactions.MISC_OP),0) AS MISC_OP
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt."SPLIT", transactions."SPLIT"),0) AS "SPLIT"
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.compensation, transactions.compensation),0) AS compensation
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_Clear, transactions.MISC_Clear),0) AS MISC_Clear
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_TR, transactions.MISC_TR),0) AS MISC_TR
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.MISC_FUNDING, transactions.MISC_FUNDING),0) AS MISC_FUNDING
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.promo, transactions.promo),0) AS promo
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.ib_rebate, transactions.ib_rebate),0) AS ib_rebate
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.auto_rebate, transactions.auto_rebate),0) AS auto_rebate
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.CPA_rebate, transactions.CPA_rebate),0) AS CPA_rebate
                    ,COALESCE(IFF(all_accounts_info.type = 9, wt.tot_rebate, transactions.tot_rebate),0) AS total_rebate
                    -- TA open equity 
                    ,COALESCE(frst.positive_equity, 0) AS ta_open_positive_equity
                    ,COALESCE(txn.profit_sum, 0) AS profit_sum
                FROM all_accounts_info
                LEFT JOIN trades_volume
                ON all_accounts_info.server = trades_volume.server
                    AND all_accounts_info.login = CAST(trades_volume.login AS VARCHAR)
                LEFT JOIN orders_count
                ON all_accounts_info.server = orders_count.server
                    AND all_accounts_info.login = CAST(orders_count.login AS VARCHAR)
                LEFT JOIN open_orders_count
                ON all_accounts_info.server = open_orders_count.server
                    AND all_accounts_info.login = CAST(open_orders_count.login AS VARCHAR)
                LEFT JOIN transactions
                ON all_accounts_info.server = transactions.server
                    AND all_accounts_info.login = CAST(transactions.login AS VARCHAR)
                LEFT JOIN last_report AS lr
                ON all_accounts_info.login = lr.login
                    AND all_accounts_info.trading_server_name = lr.trading_server
                LEFT JOIN wallet_transactions AS wt
                ON all_accounts_info.login = CAST(wt.login AS VARCHAR)
                LEFT JOIN wallet_balance AS wtb
                ON all_accounts_info.login = CAST(wtb.external_id AS VARCHAR)
                    AND all_accounts_info.type = wtb.type
                LEFT JOIN rsuh_recent as rs
                    ON all_accounts_info.login=cast(rs.login AS VARCHAR)
                    AND all_accounts_info.type = rs.rs_type
                LEFT JOIN first_report AS frst
                    ON all_accounts_info.login = frst.login
                    AND all_accounts_info.trading_server_name = frst.trading_server
                LEFT JOIN trades_txn AS txn
                    ON all_accounts_info.login = txn.login
                    AND all_accounts_info.server = txn.server
                WHERE (UPPER(all_accounts_info.jurisdiction) NOT LIKE '%TEST%' OR all_accounts_info.jurisdiction IS NULL)
            )
            SELECT
                   dt_report
                  ,dt_report_insert
                  ,report_jurisdiction
                  ,login
                  ,is_ib
                  ,"GROUP"
                  ,user_id
                  ,deposit
                  ,withdraw
                  ,net_deposit
                  ,credit
                  ,credit_change
                  ,swap
                  ,commission
                  ,agent
                  ,forex_rebate
                  ,bullion_rebate
                  ,oil_rebate
                  ,cfd_rebate
                  ,crypto_rebate
                  ,shares_rebate
                  ,trade_cost
                  ,cost_to_loss
                  ,forex_vol
                  ,bullion_vol
                  ,oil_vol
                  ,cfd_vol
                  ,crypto_vol
                  ,shares_vol
                  ,profit
                  ,pnl
                  ,equity
                  ,positive_equity
                  ,equity_change
                  ,last_balance
                  ,positive_balance
                  ,used_credit
                  ,negative_balance
                  ,currency
                  ,country
                  ,member_sales_code
                  ,ta_sales_code
                  ,margin
                  ,margin_free
                  ,margin_level
                  ,leverage
                  ,used_credit_change
                  ,negative_balance_change
                  ,internal_transfer
                  ,rebate
                  ,instant_rebate
                  ,adjustment
                  ,misc
                  ,member_register_date
                  ,ta_register_date
                  ,member_first_ta_registration_date
                  ,member_first_ib_ta_registration_date
                  ,jurisdiction
                  ,final_jurisdiction
                  ,trading_server
                  ,comment
                  ,member_lead_source
                  ,ta_lead_source
                  ,count_open_orders
                  ,count_pending_orders
                  ,count_closed_orders
                  ,average_trade_duration
                  ,member_account_type
                  ,member_type
                  ,is_cimb
                  ,cimb_name
                  ,cimb_cfd
                  ,year
                  ,month
                  ,day
                  ,sales_deposit
                  ,sales_withdraw
                  ,sales_net_deposit
                  ,internal_transfer_deposit_from_ib
                  ,internal_transfer_withdrawal_to_ib
                  ,internal_transfer_from_same_category
                  ,internal_transfer_to_same_category
                  ,credit_txn
                  ,points
                  ,cfd
                  ,rollover
                  ,misc_op
                  ,split
                  ,compensation
                  ,misc_clear
                  ,misc_tr
                  ,misc_funding
                  ,promo
                  ,ib_rebate
                  ,auto_rebate
                  ,cpa_rebate
                  ,total_rebate
                  ,ROUND(CASE
                               WHEN currency = 'USD' THEN lifetime_equity_change_lc * 1.0
                               WHEN SUBSTRING(symbol,1,3) = currency THEN lifetime_equity_change_lc * close -- e.g. AUDUSD:0.66, USDAUD:1.55
                               WHEN SUBSTRING(symbol,-3) = currency THEN lifetime_equity_change_lc / close
                               ELSE 0.0
                            END, 2) AS lifetime_equity_change_usd
                FROM (
                    SELECT
                       eq.dt_report
                      ,eq.dt_report_insert
                      ,eq.report_jurisdiction
                      ,eq.login
                      ,eq.is_ib
                      ,eq."GROUP"
                      ,eq.user_id
                      ,eq.deposit
                      ,eq.withdraw
                      ,eq.net_deposit
                      ,eq.credit
                      ,eq.credit_change
                      ,eq.swap
                      ,eq.commission
                      ,eq.agent
                      ,eq.forex_rebate
                      ,eq.bullion_rebate
                      ,eq.oil_rebate
                      ,eq.cfd_rebate
                      ,eq.crypto_rebate
                      ,eq.shares_rebate
                      ,eq.trade_cost
                      ,eq.cost_to_loss
                      ,eq.forex_vol
                      ,eq.bullion_vol
                      ,eq.oil_vol
                      ,eq.cfd_vol
                      ,eq.crypto_vol
                      ,eq.shares_vol
                      ,eq.profit
                      ,eq.pnl
                      ,eq.equity
                      ,eq.positive_equity
                      ,eq.equity_change
                      ,eq.last_balance
                      ,eq.positive_balance
                      ,eq.used_credit
                      ,eq.negative_balance
                      ,eq.currency
                      ,eq.country
                      ,eq.member_sales_code
                      ,eq.ta_sales_code
                      ,eq.margin
                      ,eq.margin_free
                      ,eq.margin_level
                      ,eq.leverage
                      ,eq.used_credit_change
                      ,eq.negative_balance_change
                      ,eq.internal_transfer
                      ,eq.rebate
                      ,eq.instant_rebate
                      ,eq.adjustment
                      ,eq.misc
                      ,eq.member_register_date
                      ,eq.ta_register_date
                      ,eq.member_first_ta_registration_date
                      ,eq.member_first_ib_ta_registration_date
                      ,eq.jurisdiction
                      ,eq.final_jurisdiction
                      ,eq.trading_server
                      ,eq.comment
                      ,eq.member_lead_source
                      ,eq.ta_lead_source
                      ,eq.count_open_orders
                      ,eq.count_pending_orders
                      ,eq.count_closed_orders
                      ,eq.average_trade_duration
                      ,eq.member_account_type
                      ,eq.member_type
                      ,eq.is_cimb
                      ,eq.cimb_name
                      ,eq.cimb_cfd
                      ,eq.year
                      ,eq.month
                      ,eq.day
                      ,eq.sales_deposit
                      ,eq.sales_withdraw
                      ,eq.sales_net_deposit
                      ,eq.internal_transfer_deposit_from_ib
                      ,eq.internal_transfer_withdrawal_to_ib
                      ,eq.internal_transfer_from_same_category
                      ,eq.internal_transfer_to_same_category
                      ,eq.credit_txn
                      ,eq.points
                      ,eq.cfd
                      ,eq.rollover
                      ,eq.misc_op
                      ,eq.split
                      ,eq.compensation
                      ,eq.misc_clear
                      ,eq.misc_tr
                      ,eq.misc_funding
                      ,eq.promo
                      ,eq.ib_rebate
                      ,eq.auto_rebate
                      ,eq.cpa_rebate
                      ,eq.total_rebate
                      ,eq.positive_equity - eq.ta_open_positive_equity - eq.profit_sum as lifetime_equity_change_lc
                      ,ex.close
                      ,ex.symbol
                      ,ROW_NUMBER() OVER (PARTITION BY eq.dt_report, eq.user_id, eq.login, eq.trading_server, eq.currency ORDER BY ex.dt_report DESC) AS rn
                    FROM t_1_equity_lc  eq
                    LEFT JOIN ex_rate   ex
                      ON eq.currency = ex.ac
                     AND eq.dt_report <= DATE(eq.dt_report)
                     AND eq.dt_report >= DATE(eq.dt_report) - INTERVAL '7 DAY' -- will 1:m join, so need dedup, look back for 7 days
                    QUALIFY rn = 1
            )
            """.format(LOOKBACK_INTERVAL, lnd_schema, tst_schema, dt_report, tenant, servers_exclusion,
                       crm_server_id_demo, crm_server_id_wallet, crm_server_id_iris, sub_ib_trn_type)

    return query




if __name__ == '__main__':
    lnd_schema = 'lnd'
    tst_schema = 'tst'
    snowflake_cur = ""
    dt_report = '2024-07-16'
    LOOKBACK_INTERVAL = 2
    tenant = 'ttg'
    servers_exclusion = "('mt4_ttg_demo','mt5_ttg_demo')"
    crm_server_id_demo = '2,6'
    crm_server_id_wallet = 3
    crm_server_id_iris = -1
    sub_ib_trn_type = 13
    query_replaced = insert_accounting_t_1_equity_report_delta(lnd_schema, tst_schema,snowflake_cur, dt_report, LOOKBACK_INTERVAL,
                                                  tenant, servers_exclusion, crm_server_id_demo, crm_server_id_wallet,
                                                  crm_server_id_iris, sub_ib_trn_type)
    print(query_replaced)
