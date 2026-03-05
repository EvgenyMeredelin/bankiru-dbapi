from contextlib import asynccontextmanager
from typing import Annotated

import fastapi
import logfire
from aiobotocore.client import AioBaseClient
from environs import env
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    status
)
from fastapi.security import APIKeyHeader
from sqlalchemy import (
    Date,
    cast,
    delete,
    or_,
    select
)
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import RedirectResponse

import schemas
from botocore_client import get_async_client
from database import (
    create_all_tables,
    get_async_session
)
from models import Review
from schemas import (
    available_output_formats,
    Request,
    Response
)


DBSession = Annotated[AsyncSession, Depends(get_async_session)]
BotoClient = Annotated[AioBaseClient, Depends(get_async_client)]
backup_request = Request(isBackup=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_all_tables()
    yield


async def api_token(
    token: Annotated[str, Depends(APIKeyHeader(name="API-Token"))]
):
    if token != env("API_TOKEN"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)


app = FastAPI(
    lifespan=lifespan,
    title="Banki.ru Claims and Negative Reviews Database API",
    version="0.1.0",
    contact={
        "name": "Evgeny Meredelin",
        "email": "eimeredelin@sberbank.ru"
    }
)
logfire.instrument_fastapi(app)


@app.get("/")
async def redirect_from_root_to_docs():
    return RedirectResponse(url="/docs")


@app.post(
    path="/reviews",
    dependencies=[Depends(api_token)],
    status_code=status.HTTP_201_CREATED
)
async def post_reviews(
    reviews: list[schemas.Review],
    session: DBSession,
    client: BotoClient
):
    with logfire.span("Create new entries"):
        reviews = [Review(**r.model_dump()) for r in reviews]

    with logfire.span("Add entries and commit"):
        session.add_all(reviews)
        await session.commit()

    with logfire.span("Make a database backup"):
        await get_reviews(backup_request, session, client)


@app.get("/reviews")
async def get_reviews(
    r: Annotated[Request, Query()],
    session: DBSession,
    client: BotoClient
):
    with logfire.span("Select entries"):
        clauses = []
        dates = cast(Review.datePublished, Date)

        if r.startDate:
            clauses.append(dates >= r.startDate)
        if r.endDate:
            clauses.append(dates <= r.endDate)
        if r.location:
            location_prefix_clauses = [
                Review.location.startswith(loc)
                for loc in r.location
            ]
            clauses.append(or_(*location_prefix_clauses))
        if r.bankName:
            clauses.append(Review.bankName.in_(r.bankName))
        if r.product:
            clauses.append(Review.product.in_(r.product))

        sort_order = [Review.datePublished, Review.url, Review.product]
        statement = select(Review).where(*clauses).order_by(*sort_order)
        result = await session.execute(statement)

    with logfire.span("Pick a handler, handle reviews, return a response"):
        if not (scalars := result.scalars().all()):
            return Response(
                **r.model_dump(),
                comment="Your search did not match any reviews"
            )

        handler_class = available_output_formats[r.outputFormat]
        handler = handler_class(scalars, client, r.isBackup)
        await handler.upload_contents()

        if r.isBackup:
            return fastapi.Response(status_code=status.HTTP_204_NO_CONTENT)

        url = await handler.generate_url()
        comment = await handler.summarize_reviews(r.cloudModel)

        return Response(
            **r.model_dump(),
            filename=handler.key,
            url=url,
            comment=comment
        )


@app.delete(
    path="/reviews",
    dependencies=[Depends(api_token)],
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_reviews(
    delete_ids: list[int],
    session: DBSession,
    client: BotoClient
):
    with logfire.span("Delete entries and commit"):
        statement = delete(Review).where(Review.id.in_(delete_ids))
        await session.execute(statement)
        await session.commit()

    with logfire.span("Make a database backup"):
        await get_reviews(backup_request, session, client)
