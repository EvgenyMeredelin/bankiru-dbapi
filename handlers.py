import collections
import itertools
import io
from abc import (
    ABC,
    abstractmethod
)
from functools import cached_property
from uuid import uuid4

import logfire
import pandas as pd
from aiobotocore.client import AioBaseClient
from environs import env
from pydantic_ai import (
    Agent,
    UsageLimits
)
from pydantic_ai.exceptions import (
    ModelHTTPError,
    UsageLimitExceeded
)
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from sqlalchemy.engine import ScalarResult
from styleframe import (
    StyleFrame,
    Styler,
    utils
)

from models import review_columns


class ScalarsHandler(ABC):
    """
    A base class for a handler that converts SQLAlchemy scalars to a specific
    output format, uploads result to S3 bucket, and generates a download link.
    Handler can also summarize scalars text content.
    """

    def __init__(
        self,
        scalars: list[ScalarResult],
        botoclient: AioBaseClient,
        is_backup: bool
    ) -> None:

        # preserve columns order as they declared in the Review table
        # and drop the "_sa_instance_state" column
        with logfire.span("Make a dataframe from the scalars"):
            records = list(map(vars, scalars))
            self.df = pd.DataFrame.from_records(records)[review_columns]

        with logfire.span("Set the rest of the attributes"):
            self.client = botoclient
            self.is_backup = is_backup
            self._body = io.BytesIO()

    @property
    @abstractmethod
    def extension(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def content_type(self) -> str:
        raise NotImplementedError

    @cached_property
    @abstractmethod
    def body(self) -> io.BytesIO:
        raise NotImplementedError

    @cached_property
    def key(self) -> str:
        name = "bankiru_reviews_db_backup" if self.is_backup else uuid4()
        return f"{name}.{self.__class__.extension}"

    async def upload_contents(self) -> None:
        with logfire.span("Make a format-specific object"):
            self.body.seek(0)

        with logfire.span("Put an object to a bucket"):
            await self.client.put_object(
                Bucket=env("OBS_BUCKET"),
                Key=self.key,
                Body=self.body,
                ContentType=self.__class__.content_type
            )

    async def generate_url(self) -> str:
        with logfire.span("Generate a pre-signed URL and return it"):
            url = await self.client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": env("OBS_BUCKET"),
                    "Key": self.key
                }
            )
            return url

    async def summarize_reviews(
        self, cloud_model: str | None, cloud_api_key: str
    ) -> str:
        with logfire.span("Summarize reviews"):
            model = OpenAIChatModel(
                model_name=cloud_model or env("DEFAULT_MODEL_NAME"),
                provider=OpenAIProvider(api_key=cloud_api_key)
            )
            limits = UsageLimits(
                output_tokens_limit=env.int("OUTPUT_TOKENS_LIMIT")
            )
            reviews = "\n\n".join(self.df.reviewBody.unique())
            agent = Agent(model, system_prompt=env("SYSTEM_PROMPT"))

            try:
                run = await agent.run(reviews, usage_limits=limits)
                return run.output
            except ModelHTTPError:
                return "Invalid Cloud API key"
            except UsageLimitExceeded as error:
                return str(error)


class CSVMaker(ScalarsHandler):
    extension = "csv"
    content_type = "text/csv"

    @cached_property
    def body(self) -> io.BytesIO:
        self.df.to_csv(
            self._body,
            index=False,
            encoding="utf-8"
        )
        return self._body


class JSONMaker(ScalarsHandler):
    extension = "json"
    content_type = "application/json"

    @cached_property
    def body(self) -> io.BytesIO:
        self.df.to_json(
            self._body,
            orient="records",
            date_format="iso",
            force_ascii=False,
            indent=4
        )
        return self._body


class ParquetMaker(ScalarsHandler):
    extension = "parquet"
    content_type = "application/vnd.apache.parquet"

    @cached_property
    def body(self) -> io.BytesIO:
        self.df.to_parquet(self._body, index=False)
        return self._body


class XlsxMaker(ScalarsHandler):
    extension = "xlsx"
    content_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    @cached_property
    def body(self) -> io.BytesIO:
        number = itertools.count(1)
        enumerator = collections.defaultdict(lambda: next(number))
        review_n = self.df.url.apply(lambda url: enumerator[url])
        odd_row_mask = (review_n % 2).astype(bool)

        base_style = Styler(
            font="Consolas",
            font_size=10,
            horizontal_alignment=utils.horizontal_alignments.left,
            wrap_text=False,
            shrink_to_fit=False,
            date_time_format="YYYY-MM-DD HH:MM:SS"
        )

        base_params = vars(base_style)
        sf = StyleFrame(self.df, base_style)

        headers_update = {
            "bg_color": "#57534D",
            "font_color": "#FFFFFF"
        }
        headers_params = base_params | headers_update
        sf.apply_headers_style(Styler(**headers_params))

        even_row_update = {"bg_color": "#FAD0E5"}
        even_row_params = base_params | even_row_update

        sf.apply_style_by_indexes(
            indexes_to_style=sf[odd_row_mask],
            styler_obj=Styler(bg_color="#D0FAE5"),
            complement_style=Styler(**even_row_params),
            overwrite_default_style=False
        )

        best_fit_columns = self.df.columns.to_list()
        best_fit_columns.remove("reviewBody")
        StyleFrame.A_FACTOR, StyleFrame.P_FACTOR = 3, 1.1

        with StyleFrame.ExcelWriter(self._body) as writer:
            sf.to_excel(
                excel_writer=writer,
                columns_and_rows_to_freeze="A2",
                best_fit=best_fit_columns,
                index=False
            )

        return self._body
