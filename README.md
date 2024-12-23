# 1757. Recyclable and Low Fat Products

- ### Intuition and Approach for Filtering and Selecting Data

    The general approach to filtering and selecting data is similar across **PySpark**, **SQL**, and **Pandas**. The key steps are:

    1. **Filter Data**: Select rows based on conditions.
    2. **Select Columns**: Choose specific columns to display.

- ### Code Implementation
    1. **SQL:**
        ```sql []
        SELECT product_id FROM
        Products WHERE low_fats = 'Y' AND
        RECYCLABLE = 'Y'
        ```
    2. **Pyspark:**
        ```python3 []
        def find_products(products: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            product_ids = products\
                            .filter((products['low_fats'] == 'Y') & (products['recyclable'] == 'Y'))\
                            .select('product_id')
            return product_ids
        ```
    3. **Pandas:**
        ```python3 []
        def find_products_pandas(products: pd.DataFrame) -> pd.DataFrame:
            product_ids = products[(products['low_fats'] == 'Y') &
                                    (products['recyclable'] == 'Y')]\
                            .product_id
            product_ids = pd.DataFrame(product_ids)
            return product_ids
        ```