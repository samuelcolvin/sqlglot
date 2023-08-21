import sqlglot.dataframe.sql.functions as F
from tests.dataframe.unit.dataframe_sql_validator import DataFrameSQLNoSchemaValidator


class TestDataFrameNoSchema(DataFrameSQLNoSchemaValidator):
    def test_just_read(self):
        self.compare_sql(self.df_employee, "SELECT * FROM `employee` AS `employee`")

    def test_simple_select(self):
        df = self.df_employee.select("fname")
        self.compare_sql(df, "SELECT `employee`.`fname` AS `fname` FROM `employee` AS `employee`")

    def test_simple_select_with_alias(self):
        df = self.df_employee.select("fname").alias("a")
        self.compare_sql(df, "SELECT `employee`.`fname` AS `fname` FROM `employee` AS `employee`")

    def test_with_column_read_table(self):
        df = self.df_employee.withColumn("subset", F.substring(F.col("fname"), 1, 2))
        self.compare_sql(
            df,
            "WITH `t98571` AS (SELECT * FROM `employee` AS `employee`) SELECT *, SUBSTRING(`t98571`.`fname`, 1, 2) AS `subset` FROM `t98571`",
        )

    def test_with_column_then_select(self):
        df = self.df_employee.withColumn("subset", F.substring(F.col("fname"), 1, 2)).select(
            "subset", "lname"
        )
        self.compare_sql(
            df,
            "SELECT SUBSTRING(`employee`.`fname`, 1, 2) AS `subset`, `employee`.`lname` AS `lname` FROM `employee` AS `employee`",
        )

    def test_with_column_renamed(self):
        df = self.df_employee.withColumnRenamed("fname", "first_name")
        self.compare_sql(
            df,
            "WITH `t98571` AS (SELECT * FROM `employee` AS `employee`) SELECT * EXCEPT (`fname`), `t98571`.`fname` AS `first_name` FROM `t98571`",
        )
