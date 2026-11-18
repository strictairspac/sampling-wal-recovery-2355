from sqlalchemy import select
from fastapi import HTTPException
from src.models.entities import Recovery,CrashCase,LogSet,Backup,RecoveryBackup
from src.services.backup_service import chain_of,restore_backup,chain_evidence
from src.utils.serialize import record

def list_recoveries(db):return [record(x) for x in db.scalars(select(Recovery).order_by(Recovery.id.desc()).limit(100))]
def get_recovery(db,ident):
    obj=db.get(Recovery,ident)
    if obj is None:raise HTTPException(404,'恢复记录不存在')
    return obj
def create_recovery(db,data):
    case=db.get(CrashCase,data.crash_case_id)
    if case is None:raise HTTPException(404,'故障场景不存在')
    if len(db.scalars(select(Recovery.id).where(Recovery.status.in_(['queued','running']))).all())>=20:raise HTTPException(429,'待处理配额20已满')
    logs=db.get(LogSet,case.log_set_id)
    snapshot={'log_set':record(logs),'case':record(case)}
    chain_ids=[]
    if data.backup_id:
        backup=db.get(Backup,data.backup_id)
        if backup is None:raise HTTPException(404,'备份不存在或已被回收')
        if backup.log_set_id!=logs.id:raise HTTPException(422,'备份与故障场景分属不同日志集，拒绝跨日志集拼接')
        try:chain=chain_of(db,backup.id)
        except ValueError as exc:raise HTTPException(422,str(exc))
        # 按 id 升序锁整条链，与并发回收互斥：链成员被回收则排队失败，排队成功则回收必能看到依赖。
        ids=sorted(b.id for b in chain)
        locked=db.scalars(select(Backup.id).where(Backup.id.in_(ids)).order_by(Backup.id).with_for_update()).all()
        if len(locked)!=len(ids):raise HTTPException(409,'备份链正在回收，请稍后重试')
        try:restored=restore_backup(db,backup.id)
        except ValueError as exc:raise HTTPException(422,str(exc))
        checkpoint=restored['checkpoint_lsn']
        if case.crash_after<checkpoint:raise HTTPException(422,f'崩溃点{case.crash_after}早于备份检查点LSN{checkpoint}，无法从该备份演练')
        if not logs.records[checkpoint:]:raise HTTPException(422,'备份检查点位于日志末尾，之后没有可演练的日志')
        # 排队即冻结链证据与日志输入，之后制作新备份不改变本次演练输入。
        snapshot['backup_id']=backup.id
        snapshot['chain_ids']=[b.id for b in chain]
        snapshot['chain']=chain_evidence(chain)
        chain_ids=[b.id for b in chain]
    values=data.model_dump();values.pop('backup_id',None)
    obj=Recovery(**values,status='queued',snapshot=snapshot)
    db.add(obj);db.flush()
    for backup_id in chain_ids:db.add(RecoveryBackup(recovery_id=obj.id,backup_id=backup_id))
    db.flush();return obj
