from sqlmodel import Field, SQLModel, Column, DateTime, func, select, Session
from datetime import datetime
from typing import Optional
import uuid

class File(SQLModel, table=True):
    __tablename__ = "files"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    user_id: str = Field(foreign_key="users.id")
    file_path: Optional[str] = Field(default=None)
    file_name: str
    authenticated: bool = Field(default=False)
    type: Optional[str] = Field(default=None)
    created_at: datetime = Field(
        sa_column=Column(DateTime(timezone=True), server_default=func.now())
    )
    updated_at: Optional[datetime] = Field(
        sa_column=Column(DateTime(timezone=True), onupdate=func.now())
    )

    def __repr__(self):
        return f"File(id={self.id}, user_id={self.user_id}, file_name={self.file_name},  type={self.type}, created_at={self.created_at}, updated_at={self.updated_at})"
    
    @classmethod
    def create_new(cls, user_id, file_name, file_type=None):
        """Factory method to create a new file record with a UUID"""
        return cls(
            id=str(uuid.uuid4()),
            user_id=user_id,
            file_name=file_name,
            type=file_type
        )
    @classmethod
    def get_all_files_by_user_id(cls, session: Session, user_id: str):
        """Get all files belonging to a specific user"""
        statement = select(cls).where(cls.user_id == user_id)
        return session.exec(statement).all()
    
    @classmethod
    def delete_by_path(cls, session: Session, file_path: str) -> bool:
        statement = select(cls).where(cls.file_path == file_path)
        file = session.exec(statement).first()
        
        if file:
            session.delete(file)
            session.commit()
            return True
        return False
    
    @classmethod
    def update_authenticated(cls, session: Session, file_path: str) -> bool:
        """Marca el archivo como autenticado. Devuelve True si se actualizó, False si no se encontró."""
        statement = select(cls).where(cls.file_path == file_path)
        file = session.exec(statement).first()

        if file:
            file.authenticated = True
            session.add(file)
            session.commit()
            session.refresh(file)
            return True

        return False

