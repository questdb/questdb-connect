class QuestDBImpl(alembic.ddl.impl.DefaultImpl):
    __dialect__ = "questdb"
    transactional_ddl = False
