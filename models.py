from datetime import datetime

from sqlalchemy import DateTime, Integer, MetaData, String, Text
from sqlalchemy.orm import declarative_base, Mapped, mapped_column


Base = declarative_base(metadata=MetaData(schema="bankiru-schema"))


class Review(Base):
    __tablename__ = "reviews"
    # __table_args__ = {"schema": "bankiru-schema"}

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    datePublished: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    reviewBody: Mapped[str] = mapped_column(Text, nullable=False)
    bankName: Mapped[str] = mapped_column(String(255), nullable=False)
    url: Mapped[str] = mapped_column(String(255), nullable=False)
    location: Mapped[str] = mapped_column(String(255), nullable=False)
    product: Mapped[str] = mapped_column(String(255), nullable=False)


review_columns = Review.__table__.columns.keys()
