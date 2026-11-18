from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from src.config.settings import DATABASE_URL
class Base(DeclarativeBase):
    pass
# SQLite仅用于本地验证脚本（scripts/verify_backups.py）；生产使用PostgreSQL，行锁语义以PG为准。
if DATABASE_URL.startswith('sqlite'):
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={'check_same_thread': False, 'timeout': 15})
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine, expire_on_commit=False)
def get_db():
    with Session() as session:
        yield session
