import dlt
from dlt.sources.rest_api import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


@dlt.source
def jaffle_shop_source(page_size: int = 100, parallelized: bool = False):
    client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1/",
        paginator=HeaderLinkPaginator(),
    )

    @dlt.resource(name="orders", parallelized=parallelized)
    def orders():
        for page in client.paginate(
            "/orders", params={"page": 1, "page_size": page_size}
        ):
            yield page

    @dlt.resource(name="items", parallelized=parallelized)
    def items():
        for page in client.paginate(
            "/items", params={"page": 1, "page_size": page_size}
        ):
            yield page

    return orders, items
