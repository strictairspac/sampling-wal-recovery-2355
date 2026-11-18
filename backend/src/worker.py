import logging, time
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.entities import Recovery
from src.services.wal_engine import recover
from src.services.backup_service import restore_backup, chain_evidence

def execute(db, job):
    """执行一次演练。从备份出发的任务先核验整条链与后续日志，再从检查点重放。"""
    snap = job.snapshot
    logs = snap['log_set']; case = snap['case']
    if snap.get('backup_id'):
        restored = restore_backup(db, snap['backup_id'])
        checkpoint = restored['checkpoint_lsn']
        crash = case['crash_after'] - checkpoint
        if crash < 0: raise ValueError('崩溃点早于备份检查点')
        suffix = logs['records'][checkpoint:]
        if not suffix: raise ValueError('备份检查点之后没有可演练的日志')
        result = recover(restored['ledger'], suffix, crash)
        result['backup_restore'] = {'backup_id': snap['backup_id'], 'checkpoint_lsn': checkpoint,
                                    'chain_length': len(restored['chain']), 'chain_verified': True,
                                    'restored': restored['ledger'], 'chain': chain_evidence(restored['chain'])}
        return result
    return recover(logs['initial'], logs['records'], case['crash_after'])

def process_one(db):
    job = db.scalar(select(Recovery).where(Recovery.status == 'queued').order_by(Recovery.id).with_for_update(skip_locked=True))
    if not job: return None
    job.status = 'running'
    try:
        # 结果账本与成功状态在同一事务提交，完成时两者一起可见。
        job.result = execute(db, job)
        job.status = 'completed'
    except ValueError as exc:
        job.result = {'error': str(exc), 'simulation': True}
        job.status = 'failed'
    db.commit()
    logging.info('recovery %s %s', job.id, job.status)
    return job.id

def main():
    redis = Redis.from_url(REDIS_URL)
    # Redis leadership lock ensures only one worker processes the durable DB queue.
    while True:
        Path('/tmp/heartbeat').touch()
        lock = redis.lock('wal-worker', timeout=15, blocking_timeout=1)
        try:
            with lock:
                with Session() as db:
                    process_one(db)
        except Exception:
            logging.exception('worker retry')
        time.sleep(1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
