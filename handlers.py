import io
from abc import ABC, abstractmethod
from functools import cached_property
from uuid import uuid4

import logfire
import pandas as pd
from aiobotocore.client import AioBaseClient
from environs import env
from sqlalchemy.engine import ScalarResult

from models import review_columns


class ScalarsHandler(ABC):
    """
    A base class for a handler that converts SQLAlchemy scalars to a specific
    output format, uploads them to an S3 bucket, and generates a pre-signed
    download link.
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
            records = [scalar.__dict__ for scalar in scalars]
            self.df = pd.DataFrame.from_records(records)[review_columns]
        with logfire.span("Set the rest of the attributes"):
            self.client = botoclient
            self._is_backup = is_backup
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
        name = "bankiru_reviews_db_backup" if self._is_backup else uuid4()
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
        with logfire.span("Generate a presigned url"):
            url = await self.client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": env("OBS_BUCKET"),
                    "Key": self.key
                }
            )
        with logfire.span("Shorten url and return it"):
            from py_spoo_url import Shortener
            return Shortener().shorten(url)


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
        with pd.ExcelWriter(self._body) as writer:
            self.df.to_excel(writer, index=False)
        return self._body
