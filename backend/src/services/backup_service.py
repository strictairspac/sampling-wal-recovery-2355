"""增量备份链：首份全量基线，后续只存相对父备份变化的键。

每份备份绑定同一日志集与静止检查点，chain_hash 把父哈希、日志集、
检查点与载荷串成链。恢复前核验整条链与后续日志：缺父、篡改或跨日志集
拼接一律拒绝，避免用错误账本覆盖数据。回收保护后代与排队演练所需祖先，
删除在单事务内完成，重试安全。
"""
import hashlib, json
from sqlalchemy import select, delete
from fastapi import HTTPException
from src.models.entities import Backup, RecoveryBackup, Recovery, LogSet
from src.services.wal_engine import validate, ledger_at
from src.utils.serialize import record

MAX_CHAIN_DEPTH = 32
BLOCKING_STATUSES = ('queued', 'running')

class BlockedError(Exception):
    """回收被保护规则阻止，blockers 为可展示的中文原因列表。"""
    def __init__(self, blockers):
        super().__init__('；'.join(blockers))
        self.blockers = blockers

def compute_chain_hash(parent_hash, log_set_id, checkpoint_lsn, kind, payload):
    source = json.dumps({'parent': parent_hash, 'log_set_id': log_set_id,
                         'checkpoint_lsn': checkpoint_lsn, 'kind': kind, 'payload': payload}, sort_keys=True)
    return hashlib.sha256(source.encode()).hexdigest()

def latest_checkpoint(records):
    return max((lsn for lsn, r in enumerate(records, 1) if r.get('op') == 'checkpoint'), default=0)

def chain_of(db, backup_id):
    """从指定备份走回链首；缺父、成环或超深时拒绝。"""
    first = db.get(Backup, backup_id)
    if first is None: raise ValueError(f'备份 #{backup_id} 不存在或已被回收')
    chain = [first]; seen = {first.id}
    while chain[-1].parent_id is not None:
        parent = db.get(Backup, chain[-1].parent_id)
        if parent is None: raise ValueError(f'缺父备份 #{chain[-1].parent_id}：备份 #{chain[-1].id} 的链断裂，拒绝恢复')
        if parent.id in seen or len(chain) >= MAX_CHAIN_DEPTH: raise ValueError('备份链存在环或超过深度上限，拒绝恢复')
        seen.add(parent.id); chain.append(parent)
    chain.reverse()
    return chain

def verify_chain(chain):
    """核验链结构、同一日志集绑定与逐环哈希；返回日志集 id。"""
    root = chain[0]
    if root.kind != 'full' or root.parent_id is not None: raise ValueError('链首必须是全量基线备份')
    log_set_id = root.log_set_id; parent_hash = ''; previous = -1
    for b in chain:
        if b.log_set_id != log_set_id: raise ValueError(f'备份 #{b.id} 属于其他日志集，拒绝跨日志集拼接')
        if b.id != root.id and b.kind != 'incr': raise ValueError(f'备份 #{b.id} 类型非法：链中只允许首份全量')
        if b.checkpoint_lsn <= previous: raise ValueError(f'备份 #{b.id} 检查点顺序异常，链必须单调推进')
        if compute_chain_hash(parent_hash, b.log_set_id, b.checkpoint_lsn, b.kind, b.payload) != b.chain_hash:
            raise ValueError(f'备份 #{b.id} 校验和不符，内容疑似被篡改，拒绝恢复')
        parent_hash = b.chain_hash; previous = b.checkpoint_lsn
    return log_set_id

def materialize(chain):
    ledger = dict(chain[0].payload)
    for b in chain[1:]: ledger.update(b.payload)
    return ledger

def chain_evidence(chain):
    return [{'id': b.id, 'label': b.label, 'kind': b.kind, 'checkpoint_lsn': b.checkpoint_lsn,
             'chain_hash': b.chain_hash} for b in chain]

def restore_backup(db, backup_id):
    """核验整条链与后续日志后物化账本；任何一环不可信都拒绝，避免数据丢失。"""
    chain = chain_of(db, backup_id)
    log_set_id = verify_chain(chain)
    logs = db.get(LogSet, log_set_id)
    if logs is None: raise ValueError('备份所属日志集不存在')
    for b in chain:
        if b.checkpoint_lsn > 0 and (b.checkpoint_lsn > len(logs.records) or logs.records[b.checkpoint_lsn - 1].get('op') != 'checkpoint'):
            raise ValueError(f'备份 #{b.id} 绑定的 LSN {b.checkpoint_lsn} 不是该日志集的静止检查点')
    ledger = materialize(chain)
    expected = ledger_at(logs.initial, logs.records, chain[-1].checkpoint_lsn)
    if ledger != expected: raise ValueError('备份链物化账本与日志集重放不一致，拒绝恢复')
    suffix = logs.records[chain[-1].checkpoint_lsn:]
    if suffix: validate(ledger, suffix)
    return {'ledger': ledger, 'chain': chain, 'checkpoint_lsn': chain[-1].checkpoint_lsn, 'log_set': logs}

def create_backup(db, data):
    logs = db.get(LogSet, data.log_set_id)
    if logs is None: raise HTTPException(404, '日志集不存在')
    checkpoint = data.checkpoint_lsn if data.checkpoint_lsn is not None else latest_checkpoint(logs.records)
    if checkpoint > len(logs.records): raise HTTPException(422, '检查点LSN超出日志范围')
    if checkpoint > 0 and logs.records[checkpoint - 1].get('op') != 'checkpoint':
        raise HTTPException(422, f'LSN {checkpoint} 不是静止检查点，备份只能绑定静止检查点')
    parent = None; parent_hash = ''; parent_state = {}
    if data.parent_id:
        # 行锁与回收互斥：父备份被回收则制作失败，父备份存活则回收能看到新后代。
        parent = db.scalar(select(Backup).where(Backup.id == data.parent_id).with_for_update())
        if parent is None: raise HTTPException(422, '父备份不存在或已被回收')
        if parent.log_set_id != logs.id: raise HTTPException(422, '父备份属于其他日志集，拒绝跨日志集拼接')
        if checkpoint <= parent.checkpoint_lsn: raise HTTPException(422, '子备份检查点必须晚于父备份检查点')
        try: restored = restore_backup(db, parent.id)
        except ValueError as exc: raise HTTPException(422, str(exc))
        if len(restored['chain']) >= MAX_CHAIN_DEPTH: raise HTTPException(422, f'备份链深度超过上限{MAX_CHAIN_DEPTH}')
        parent_hash = parent.chain_hash; parent_state = restored['ledger']
    state = ledger_at(logs.initial, logs.records, checkpoint)
    kind = 'incr' if parent else 'full'
    payload = {k: v for k, v in state.items() if parent_state.get(k) != v} if parent else state
    obj = Backup(log_set_id=logs.id, label=data.label, parent_id=parent.id if parent else None,
                 kind=kind, checkpoint_lsn=checkpoint, payload=payload,
                 chain_hash=compute_chain_hash(parent_hash, logs.id, checkpoint, kind, payload))
    db.add(obj); db.flush()
    return record(obj)

def _blocking_refs(db):
    rows = db.execute(select(RecoveryBackup.backup_id, Recovery.id)
                      .join(Recovery, Recovery.id == RecoveryBackup.recovery_id)
                      .where(Recovery.status.in_(BLOCKING_STATUSES))).all()
    refs = {}
    for backup_id, recovery_id in rows: refs.setdefault(backup_id, []).append(recovery_id)
    return refs

def list_backups(db):
    backups = db.scalars(select(Backup).order_by(Backup.id)).all()
    by_id = {b.id: b for b in backups}
    children = {}
    for b in backups:
        if b.parent_id is not None: children.setdefault(b.parent_id, []).append(b.id)
    refs = _blocking_refs(db)
    out = []
    for b in backups:
        depth = 1; parent = by_id.get(b.parent_id)
        while parent is not None and depth <= MAX_CHAIN_DEPTH:
            depth += 1; parent = by_id.get(parent.parent_id)
        blockers = []
        if children.get(b.id): blockers.append('存在后继增量备份（' + '、'.join(f'#{i}' for i in children[b.id]) + '），须先回收后代')
        if refs.get(b.id): blockers.append('排队/执行中的演练（' + '、'.join(f'#{i}' for i in refs[b.id]) + '）依赖此备份')
        out.append({**record(b), 'depth': depth, 'blockers': blockers})
    return out

def reclaim_backup(db, ident):
    """回收单份备份；被保护时抛出 BlockedError，已回收时返回 None（重试安全）。"""
    backup = db.scalar(select(Backup).where(Backup.id == ident).with_for_update())
    if backup is None: return None
    children = db.scalars(select(Backup.id).where(Backup.parent_id == ident)).all()
    blockers = []
    if children: blockers.append('存在后继增量备份（' + '、'.join(f'#{i}' for i in children) + '），须先回收后代')
    refs = db.scalars(select(Recovery.id).join(RecoveryBackup, RecoveryBackup.recovery_id == Recovery.id)
                      .where(RecoveryBackup.backup_id == ident, Recovery.status.in_(BLOCKING_STATUSES))).all()
    if refs: blockers.append('排队/执行中的演练（' + '、'.join(f'#{i}' for i in refs) + '）依赖此备份')
    if blockers: raise BlockedError(blockers)
    db.execute(delete(RecoveryBackup).where(RecoveryBackup.backup_id == ident))
    db.delete(backup); db.flush()
    return record(backup)
