from sqlglot import parse_one, exp, lineage
# from sqlglot.lineage import LineageAnalyzer

# Sample SQL
sample_query = """
INSERT INTO analytics.sales_summary (region, total_sales)
SELECT region, SUM(sales)
FROM staging.sales_data
GROUP BY region
"""

# Parse SQL into AST (Abstract Syntax Tree)
expression = parse_one(sample_query, read='snowflake')

# AST
# print(repr(expression))


# tables
# tables = expression.find_all(exp.Table)
# for table in tables:
#     print(table.name)




lineage_result = lineage.lineage(column='region', sql=sample_query)
print(lineage_result.to_html())

# print("Source tables:", lineage_result.source)
# print("Target tables:", lineage_result.written)