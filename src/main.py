import os
import time
from typing import Dict
from sources.jaffle import jaffle_shop_source
import dlt


def main(
    page_size: int = 100,
    parallelized: bool = False,
    action: str = "run",
    environment_variables: Dict[str, str] = {},
    limit_rows: int = 10000,
):
    for env_var, value in environment_variables.items():
        os.environ[env_var] = value

    pipeline = dlt.pipeline(
        pipeline_name="rest_client_jaffle_shop",
        destination="duckdb",
        dataset_name="rest_client_data",
    )

    source = jaffle_shop_source(
        page_size=page_size, parallelized=parallelized
    ).add_limit(limit_rows, count_rows=True)
    pipeline.run(source)

    return pipeline


if __name__ == "__main__":
    env_dict = {
        "DATA_WRITER__BUFFER_MAX_ITEMS": "1000",
        "EXTRACT__WORKERS": "15",
        "NORMALIZE__WORKERS": "3",
        "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS": "1000",
        "LOAD__WORKERS": "5",
        "LOAD__BUFFER_MAX_ITEMS": "10",
    }
    PAGE_SIZE = 2500
    PARALLELIZED = True
    LIMIT_ROWS = 10000
    pipeline = main(
        page_size=PAGE_SIZE,
        parallelized=PARALLELIZED,
        environment_variables=env_dict,
        limit_rows=LIMIT_ROWS,
    )

    print(pipeline.dataset().items.df().head())
