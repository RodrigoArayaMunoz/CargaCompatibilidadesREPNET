from datetime import datetime
from sqlalchemy import BigInteger, Integer, SmallInteger, Text, CheckConstraint, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from models import Base


class MLToken(Base):
    __tablename__ = "ml_tokens"

    __table_args__ = (
        CheckConstraint("id = 1", name="ml_tokens_single_row"),
    )

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, default=1)
    access_token: Mapped[str] = mapped_column(Text, nullable=False)
    refresh_token: Mapped[str] = mapped_column(Text, nullable=False)
    token_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    scope: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    expires_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expires_at: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)