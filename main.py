import operator
from contextlib import asynccontextmanager
from typing import Annotated

import fastapi
import logfire
from aiobotocore.client import AioBaseClient
from environs import env
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import APIKeyHeader
from sqlalchemy import cast, Date, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

import schemas
from botocore_client import get_async_client
from database import create_all_tables, get_async_session
from models import Review
from schemas import available_output_formats, Request, Response


DBSession = Annotated[AsyncSession, Depends(get_async_session)]
BotoClient = Annotated[AioBaseClient, Depends(get_async_client)]
backup_request = Request(isBackup=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_all_tables()
    yield


async def api_token(
    token: Annotated[str, Depends(APIKeyHeader(name="API-Token"))]
) -> None:
    if token != env("API_TOKEN"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)


app = FastAPI(
    lifespan=lifespan,
    description="Banki.ru claims and negative reviews database API",
    version="0.1.0",
    contact={
        "name": "Evgeny Meredelin",
        "email": "eimeredelin@sberbank.ru"
    }
)
logfire.instrument_fastapi(app, capture_headers=True)


@app.post(
    "/reviews",
    dependencies=[Depends(api_token)],
    status_code=status.HTTP_201_CREATED
)
async def post_reviews(
    reviews: list[schemas.Review], session: DBSession, client: BotoClient
) -> None:
    with logfire.span("Create new entries"):
        reviews = [Review(**review.model_dump()) for review in reviews]
    with logfire.span("Add entries and commit"):
        session.add_all(reviews)
        await session.commit()
    with logfire.span("Make a database backup"):
        await get_reviews(backup_request, session, client)


@app.get("/reviews")
async def get_reviews(
    r: Annotated[Request, Query()], session: DBSession, client: BotoClient
) -> Response | None:
    with logfire.span("Select entries"):
        dates = cast(Review.datePublished, Date)
        clauses = [(r.startDate, operator.ge), (r.endDate, operator.le)]
        clauses = [func(dates, date) for date, func in clauses if date]
        columns = [Review.datePublished, Review.url, Review.product]
        statement = select(Review).where(*clauses).order_by(*columns)
        result = await session.execute(statement)

    with logfire.span("Pick a handler, handle entries, return a response"):
        if not (scalars := result.scalars().all()):
            return Response(**r.model_dump(), comment="No results")

        handler_class = available_output_formats[r.outputFormat]
        handler = handler_class(scalars, client, r.isBackup)
        await handler.upload_contents()

        if r.isBackup:
            return fastapi.Response(status_code=status.HTTP_204_NO_CONTENT)

        shortened_presigned_url = await handler.generate_url()
        return Response(**r.model_dump(), url=shortened_presigned_url)


@app.delete(
    "/reviews",
    dependencies=[Depends(api_token)],
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_reviews(
    delete_ids: list[int], session: DBSession, client: BotoClient
) -> None:
    with logfire.span("Delete entries and commit"):
        statement = delete(Review).where(Review.id.in_(delete_ids))
        await session.execute(statement)
        await session.commit()
    with logfire.span("Make a database backup"):
        await get_reviews(backup_request, session, client)
