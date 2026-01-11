from datetime import datetime

from sqlalchemy import (
    DateTime,
    Integer,
    MetaData,
    Text
)
from sqlalchemy.orm import (
    declarative_base,
    Mapped,
    mapped_column
)


Base = declarative_base(
    metadata=MetaData(schema="bankiru")
)


class Review(Base):
    __tablename__ = "reviews"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True
    )
    datePublished: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False
    )
    reviewBody: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )
    bankName: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )
    url: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )
    location: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )
    product: Mapped[str] = mapped_column(
        Text,
        nullable=False
    )


review_columns = Review.__table__.columns.keys()
