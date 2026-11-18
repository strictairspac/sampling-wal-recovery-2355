import os
from sqlalchemy import select
from src.config.database import Base,engine,Session
from src.models.entities import User,LogSet,CrashCase,Backup
from src.schemas.backup import BackupInput
from src.services.backup_service import create_backup
from src.utils.security import hash_password
RECORDS=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},{'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':40},{'op':'begin','tx':'T3'},{'op':'write','tx':'T3','key':'sample_A','value':60},{'op':'commit','tx':'T3'},{'op':'write','tx':'T2','key':'sample_B','value':35},{'op':'commit','tx':'T2'},{'op':'checkpoint'}]
BACKUP_CHAIN=[('基线-初始全量',None,0),('晚班检查点4-增量','基线-初始全量',4),('晚班检查点12-增量','晚班检查点4-增量',12)]
def seed():
    Base.metadata.create_all(engine)
    if os.getenv('SEED_DEMO','true').lower()!='true':return
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@haili.local'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,role=role,password_hash=hash_password('Recovery2026!')))
        logs=db.scalar(select(LogSet).where(LogSet.name=='海砾采样账本-晚班'))
        if logs is None:
            logs=LogSet(name='海砾采样账本-晚班',description='A初始100份、B初始50份，T1提交后检查点，T2未提交，T3已提交',initial={'sample_A':100,'sample_B':50},records=RECORDS);db.add(logs);db.flush()
        for name,point in [('断电点10-撤销T2',10),('完整提交点11',11),('初始点0',0)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=logs.id,crash_after=point))
        # 种子备份链：自然键查重；旧数据缺少对应静止检查点时跳过后续链环，不覆盖用户修改。
        for label,parent_label,lsn in BACKUP_CHAIN:
            if db.scalar(select(Backup).where(Backup.label==label)):continue
            if lsn>0 and (lsn>len(logs.records) or logs.records[lsn-1].get('op')!='checkpoint'):break
            parent_id=db.scalar(select(Backup.id).where(Backup.label==parent_label)) if parent_label else None
            if parent_label and parent_id is None:break
            create_backup(db,BackupInput(log_set_id=logs.id,label=label,parent_id=parent_id,checkpoint_lsn=lsn))
        db.commit()
if __name__=='__main__':seed()
