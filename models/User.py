from sqlmodel import Field, SQLModel, Column, DateTime, func
from datetime import datetime
from typing import Optional
import uuid

class User(SQLModel, table=True):
    __tablename__ = "users"

    id: str = Field(primary_key=True)
    documentType: Optional[str] = Field(default=None)
    documentNumber: str = Field(unique=True)
    name: str
    email: str = Field(unique=True)
    phone: Optional[str] = Field(default=None)
    country: Optional[str] = Field(default=None)
    department: Optional[str] = Field(default=None)
    city: Optional[str] = Field(default=None)
    address: Optional[str] = Field(default=None)
    createdAt: datetime = Field(
        sa_column=Column(DateTime(timezone=True), server_default=func.now())
    )
    updatedAt: Optional[datetime] = Field(
        sa_column=Column(DateTime(timezone=True), onupdate=func.now())
    )

    def __repr__(self):
        return f"User(id={self.id}, name={self.name}, email={self.email})"