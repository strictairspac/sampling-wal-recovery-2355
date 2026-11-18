"""增量备份链端到端验证。

覆盖：多级链物化、断链（缺父）、篡改、跨日志集拼接、回收保护（后代/排队演练祖先）、
回收竞速、幂等回收、排队输入冻结、完成时账本与状态一起可见、备份后未提交写入。

默认使用临时SQLite（BEGIN IMMEDIATE串行化写事务，FOR UPDATE为无操作）；
设VERIFY_DATABASE_URL指向PostgreSQL可按生产行锁语义复验：
    VERIFY_DATABASE_URL=postgresql+psycopg://recovery:...@localhost/recovery python -m scripts.verify_backups
"""
import os, sys, threading, traceback
os.environ.setdefault('DATABASE_URL', os.environ.get('VERIFY_DATABASE_URL', 'sqlite:////tmp/verify_backups.db'))
os.environ.setdefault('JWT_SECRET', 'verify-backups-secret')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/15')

from sqlalchemy import event, select, text
from src.config.database import Base, engine, Session

if engine.dialect.name == 'sqlite':
    if os.path.exists('/tmp/verify_backups.db'): os.remove('/tmp/verify_backups.db')
    @event.listens_for(engine, 'begin')
    def _begin_immediate(conn): conn.exec_driver_sql('BEGIN IMMEDIATE')

from fastapi import HTTPException
from src.models.entities import LogSet, CrashCase, Recovery, Backup, RecoveryBackup, User
from src.schemas.backup import BackupInput
from src.schemas.recovery import RecoveryInput
from src.schemas.crash_case import CrashCaseInput
from src.services import backup_service, recovery_service, crash_case_service
from src import worker
from src.services.backup_service import BlockedError
from src.services.wal_engine import ledger_at
from src.utils.security import hash_password

INITIAL = {'A': 100, 'B': 50, 'C': 7}
RECORDS = [
    {'op': 'begin', 'tx': 'T1'},                              # 1
    {'op': 'write', 'tx': 'T1', 'key': 'A', 'value': 80},     # 2
    {'op': 'commit', 'tx': 'T1'},                             # 3
    {'op': 'checkpoint'},                                     # 4  cp1: A=80 B=50 C=7
    {'op': 'begin', 'tx': 'T2'},                              # 5
    {'op': 'write', 'tx': 'T2', 'key': 'B', 'value': 60},     # 6
    {'op': 'commit', 'tx': 'T2'},                             # 7
    {'op': 'checkpoint'},                                     # 8  cp2: A=80 B=60 C=7
    {'op': 'begin', 'tx': 'T3'},                              # 9
    {'op': 'write', 'tx': 'T3', 'key': 'C', 'value': 1},      # 10
    {'op': 'abort', 'tx': 'T3'},                              # 11 C回滚
    {'op': 'checkpoint'},                                     # 12 cp3: A=80 B=60 C=7
    {'op': 'begin', 'tx': 'T4'},                              # 13
    {'op': 'write', 'tx': 'T4', 'key': 'A', 'value': 1},      # 14 未提交（崩溃时撤销）
    {'op': 'begin', 'tx': 'T5'},                              # 15
    {'op': 'write', 'tx': 'T5', 'key': 'B', 'value': 66},     # 16
    {'op': 'commit', 'tx': 'T5'},                             # 17
]
RECORDS2 = [  # 第二日志集：以静止检查点结尾，用于跨日志集与末尾检查点用例
    {'op': 'begin', 'tx': 'U1'},
    {'op': 'write', 'tx': 'U1', 'key': 'A', 'value': 5},
    {'op': 'commit', 'tx': 'U1'},
    {'op': 'checkpoint'},
]

results = []
def check(name):
    def deco(fn):
        try:
            fn()
            results.append((name, None)); print(f'  PASS {name}')
        except Exception as exc:
            results.append((name, exc)); print(f'  FAIL {name}: {exc}')
            traceback.print_exc()
    return deco

def expect_http(fn, status, fragment):
    try:
        fn()
    except HTTPException as exc:
        assert exc.status_code == status, f'期望HTTP {status}，实际 {exc.status_code}: {exc.detail}'
        assert fragment in str(exc.detail), f'期望原因含「{fragment}」，实际「{exc.detail}」'
        return
    raise AssertionError(f'期望HTTP {status}（{fragment}），但调用成功')

def expect_value_error(fn, fragment):
    try:
        fn()
    except ValueError as exc:
        assert fragment in str(exc), f'期望原因含「{fragment}」，实际「{exc}」'
        return
    raise AssertionError(f'期望ValueError（{fragment}），但调用成功')

def make_log_set(db, name, initial, records):
    obj = LogSet(name=name, initial=initial, records=records, description='验证用')
    db.add(obj); db.flush(); return obj

def make_case(db, name, log_set_id, crash_after):
    return crash_case_service.create_crash_case(db, CrashCaseInput(name=name, log_set_id=log_set_id, crash_after=crash_after))

def make_backup(db, label, log_set_id, parent_id=None, checkpoint_lsn=None):
    return backup_service.create_backup(db, BackupInput(log_set_id=log_set_id, label=label, parent_id=parent_id, checkpoint_lsn=checkpoint_lsn))

def build_chain(db, prefix, log_set_id):
    """四级链：full@0 → incr@4 → incr@8 → incr@12"""
    b0 = make_backup(db, f'{prefix}-全量@0', log_set_id, None, 0)
    b1 = make_backup(db, f'{prefix}-增量@4', log_set_id, b0['id'], 4)
    b2 = make_backup(db, f'{prefix}-增量@8', log_set_id, b1['id'], 8)
    b3 = make_backup(db, f'{prefix}-增量@12', log_set_id, b2['id'], 12)
    return [b0, b1, b2, b3]

def main():
    Base.metadata.drop_all(engine); Base.metadata.create_all(engine)
    with Session() as db:
        logs = make_log_set(db, '验证账本-主', INITIAL, RECORDS)
        logs2 = make_log_set(db, '验证账本-末检查点', {'A': 100}, RECORDS2)
        case_end = make_case(db, '崩溃点17-末尾', logs.id, 17)
        case_early = make_case(db, '崩溃点10-检查点前', logs.id, 10)
        case_tail = make_case(db, '崩溃点4-末检查点', logs2.id, 4)
        db.commit()
        LOGS_ID, LOGS2_ID = logs.id, logs2.id
        CASE_END, CASE_EARLY, CASE_TAIL = case_end['id'], case_early['id'], case_tail['id']

    @check('多级链：四级链物化账本与逐环增量载荷正确')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T1', LOGS_ID)
            assert [b['kind'] for b in chain] == ['full', 'incr', 'incr', 'incr']
            assert chain[0]['payload'] == {'A': 100, 'B': 50, 'C': 7}, '全量基线须存完整账本'
            assert chain[1]['payload'] == {'A': 80}, f'增量只存变化的键: {chain[1]["payload"]}'
            assert chain[2]['payload'] == {'B': 60}
            assert chain[3]['payload'] == {}, 'T3中止后无变化键，增量为空'
            restored = backup_service.restore_backup(db, chain[3]['id'])
            assert restored['ledger'] == {'A': 80, 'B': 60, 'C': 7}, restored['ledger']
            assert restored['ledger'] == ledger_at(INITIAL, RECORDS, 12)
            assert len(restored['chain']) == 4 and restored['checkpoint_lsn'] == 12
            hashes = [b['chain_hash'] for b in chain]
            assert len(set(hashes)) == 4, '逐环哈希须不同'
            listing = {b['id']: b for b in backup_service.list_backups(db)}
            assert listing[chain[3]['id']]['depth'] == 4
            db.rollback()

    @check('断链：缺父备份拒绝恢复')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T2', LOGS_ID)
            middle = db.get(Backup, chain[1]['id'])
            db.delete(middle); db.commit()  # 模拟外力造成的断链（绕过回收保护）
            expect_value_error(lambda: backup_service.restore_backup(db, chain[3]['id']), '缺父备份')
            for b in (chain[3], chain[2], chain[0]):
                obj = db.get(Backup, b['id'])
                if obj: db.delete(obj)
            db.commit()

    @check('篡改：载荷被改动时哈希核验拒绝')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T3', LOGS_ID)
            obj = db.get(Backup, chain[2]['id'])
            obj.payload = {'B': 999}; db.commit()  # 直接改库模拟篡改
            expect_value_error(lambda: backup_service.restore_backup(db, chain[3]['id']), '篡改')
            obj = db.get(Backup, chain[2]['id'])
            obj.payload = {'B': 60}; obj.checkpoint_lsn = 8  # 还原载荷，改检查点同样破坏哈希
            obj.checkpoint_lsn = 7; db.commit()
            expect_value_error(lambda: backup_service.restore_backup(db, chain[3]['id']), '篡改')
            for b in chain:
                obj = db.get(Backup, b['id'])
                if obj: db.delete(obj)
            db.commit()

    @check('跨日志集拼接：制作与恢复两端均拒绝')
    def _():
        with Session() as db:
            mine = make_backup(db, 'T4-主链全量@0', LOGS_ID, None, 0)
            other = make_backup(db, 'T4-外集全量@0', LOGS2_ID, None, 0)
            expect_http(lambda: make_backup(db, 'T4-拼接子备份', LOGS2_ID, mine['id'], 4), 422, '跨日志集')
            obj = db.get(Backup, other['id'])
            obj.parent_id = mine['id']; obj.kind = 'incr'; db.commit()  # 直接改库模拟拼接
            expect_value_error(lambda: backup_service.restore_backup(db, other['id']), '跨日志集')
            for b in (mine, other):
                obj = db.get(Backup, b['id'])
                if obj: db.delete(obj)
            db.commit()

    @check('回收保护：后代与排队演练祖先均被阻止，原因可读')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T5', LOGS_ID)
            try:
                backup_service.reclaim_backup(db, chain[1]['id'])
                raise AssertionError('有后代的备份不应回收成功')
            except BlockedError as exc:
                assert any('后代' in b for b in exc.blockers), exc.blockers
            case = db.scalar(select(CrashCase).where(CrashCase.name == '崩溃点17-末尾'))
            job = recovery_service.create_recovery(db, RecoveryInput(crash_case_id=case.id, label='T5排队演练', backup_id=chain[3]['id']))
            db.commit()
            for link in chain:  # 排队演练所需整条链的祖先都受保护
                try:
                    backup_service.reclaim_backup(db, link['id'])
                    raise AssertionError(f'备份 #{link["id"]} 应被排队演练保护')
                except BlockedError as exc:
                    assert any('演练' in b for b in exc.blockers), exc.blockers
            listing = {b['id']: b for b in backup_service.list_backups(db)}
            assert listing[chain[0]['id']]['blockers'], '列表须展示阻止原因'
            db.get(Recovery, job.id).status = 'completed'; db.commit()  # 演练完成后解除排队保护
            removed = backup_service.reclaim_backup(db, chain[3]['id'])
            assert removed and removed['id'] == chain[3]['id']
            again = backup_service.reclaim_backup(db, chain[3]['id'])
            assert again is None, '重复回收须幂等返回，不报错不重复删除'
            db.commit()
            for b in chain[:3]:
                obj = db.get(Backup, b['id'])
                if obj: db.delete(obj)
            db.execute(text('DELETE FROM recovery_backups')); db.delete(db.get(Recovery, job.id)); db.commit()

    @check('回收竞速：并发排队与回收不留下悬空引用')
    def _():
        for round_no in range(20):
            with Session() as db:
                root = make_backup(db, f'T6-r{round_no}-根', LOGS_ID, None, 0)
                tip = make_backup(db, f'T6-r{round_no}-尖', LOGS_ID, root['id'], 4)
                case_id = db.scalar(select(CrashCase.id).where(CrashCase.name == '崩溃点17-末尾'))
                db.commit()
                tip_id = tip['id']
            barrier = threading.Barrier(3)
            outcome = {}
            def do_queue():
                with Session() as s:
                    try:
                        barrier.wait()
                        case = s.get(CrashCase, case_id)
                        recovery_service.create_recovery(s, RecoveryInput(crash_case_id=case.id, label=f'T6-r{round_no}演练', backup_id=tip_id))
                        s.commit(); outcome['queue'] = 'ok'
                    except HTTPException as exc:
                        s.rollback(); outcome['queue'] = f'http{exc.status_code}'
                    except Exception as exc:
                        s.rollback(); outcome['queue'] = type(exc).__name__
            def do_reclaim():
                with Session() as s:
                    try:
                        barrier.wait()
                        backup_service.reclaim_backup(s, tip_id)
                        s.commit(); outcome['reclaim'] = 'ok'
                    except BlockedError:
                        s.rollback(); outcome['reclaim'] = 'blocked'
                    except Exception as exc:
                        s.rollback(); outcome['reclaim'] = type(exc).__name__
            t1 = threading.Thread(target=do_queue); t2 = threading.Thread(target=do_reclaim)
            t1.start(); t2.start(); barrier.wait(); t1.join(); t2.join()
            assert outcome['queue'] in ('ok', 'http404', 'http409', 'http422'), outcome
            assert outcome['reclaim'] in ('ok', 'blocked'), outcome
            assert not (outcome['queue'] == 'ok' and outcome['reclaim'] == 'ok'), f'排队与回收不得同时成功: {outcome}'
            with Session() as s:  # 全局不变量：排队/执行中演练引用的链环必须存在
                dangling = s.execute(text(
                    'SELECT rb.backup_id FROM recovery_backups rb JOIN recoveries r ON r.id = rb.recovery_id '
                    "WHERE r.status IN ('queued','running') AND rb.backup_id NOT IN (SELECT id FROM backups)")).all()
                assert not dangling, f'存在悬空引用: {dangling}'
                gone = s.get(Backup, tip_id) is None
                if outcome['reclaim'] == 'ok': assert gone, '回收成功后备份须消失'
                if outcome['queue'] == 'ok': assert not gone, '排队成功后备份须受保护'
                # 清理本轮
                s.execute(text('DELETE FROM recovery_backups'))
                s.execute(text("DELETE FROM recoveries WHERE label LIKE 'T6-%'"))
                for bid in (tip_id, root['id']):
                    obj = s.get(Backup, bid)
                    if obj: s.delete(obj)
                s.commit()

    @check('排队后输入不随新备份改变，完成时账本与成功状态一起可见')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T7', LOGS_ID)
            case = db.scalar(select(CrashCase).where(CrashCase.name == '崩溃点17-末尾'))
            job = recovery_service.create_recovery(db, RecoveryInput(crash_case_id=case.id, label='T7冻结输入', backup_id=chain[3]['id']))
            db.commit()
            expected_snapshot = dict(job.snapshot)
            # 排队后制作新备份（同级分支与新链），不应改变已排队输入
            make_backup(db, 'T7-同级分支@12', LOGS_ID, chain[2]['id'], 12)
            make_backup(db, 'T7-新链全量@0', LOGS_ID, None, 0)
            db.commit()
            assert job.snapshot == expected_snapshot, '排队后快照不得改变'
            worker.process_one(db)
            db.refresh(job)
            assert job.status == 'completed', job.status
            assert job.result and job.result['recovered'] == {'A': 80, 'B': 66, 'C': 7}, '完成时账本须与成功状态一起可见'
            assert job.result['invariant_passed'] is True
            br = job.result['backup_restore']
            assert br['chain_verified'] is True and br['chain_length'] == 4 and br['checkpoint_lsn'] == 12
            assert br['restored'] == {'A': 80, 'B': 60, 'C': 7}
            assert job.snapshot == expected_snapshot, '执行后快照仍不得改变'
            db.execute(text('DELETE FROM recovery_backups'))
            for b in db.scalars(select(Backup).where(Backup.label.like('T7-%'))).all(): db.delete(b)
            db.delete(job); db.commit()

    @check('备份后未提交写入：物化账本不含未提交值，恢复正确撤销')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T8', LOGS_ID)
            case = db.scalar(select(CrashCase).where(CrashCase.name == '崩溃点17-末尾'))
            job = recovery_service.create_recovery(db, RecoveryInput(crash_case_id=case.id, label='T8未提交写入', backup_id=chain[3]['id']))
            db.commit(); worker.process_one(db); db.refresh(job)
            assert job.status == 'completed', job.result
            r = job.result
            assert r['backup_restore']['restored'] == {'A': 80, 'B': 60, 'C': 7}, '检查点物化账本不得含检查点后的写入'
            assert r['recovered'] == {'A': 80, 'B': 66, 'C': 7}, 'T4未提交须撤销、T5已提交须保留'
            assert r['losers'] == ['T4'] and r['committed'] == ['T5']
            assert r['undo_count'] == 1 and r['invariant_passed'] is True
            db.execute(text('DELETE FROM recovery_backups'))
            for b in db.scalars(select(Backup).where(Backup.label.like('T8-%'))).all(): db.delete(b)
            db.delete(job); db.commit()

    @check('非法输入：崩溃点早于检查点、非静止检查点、末尾检查点无后续日志均拒绝')
    def _():
        with Session() as db:
            chain = build_chain(db, 'T9', LOGS_ID)
            early = db.scalar(select(CrashCase).where(CrashCase.name == '崩溃点10-检查点前'))
            expect_http(lambda: recovery_service.create_recovery(db, RecoveryInput(crash_case_id=early.id, label='T9崩溃过早', backup_id=chain[3]['id'])), 422, '早于备份检查点')
            expect_http(lambda: make_backup(db, 'T9-非静止', LOGS_ID, None, 5), 422, '静止检查点')
            expect_http(lambda: make_backup(db, 'T9-越界', LOGS_ID, None, 99), 422, '超出日志范围')
            tail_case = db.scalar(select(CrashCase).where(CrashCase.name == '崩溃点4-末检查点'))
            tail_backup = make_backup(db, 'T9-末尾备份', LOGS2_ID, None, 4)
            expect_http(lambda: recovery_service.create_recovery(db, RecoveryInput(crash_case_id=tail_case.id, label='T9末尾无日志', backup_id=tail_backup['id'])), 422, '没有可演练的日志')
            db.rollback()

    @check('原演练回归：不带备份的排队、执行与导出不受影响')
    def _():
        with Session() as db:
            case = db.scalar(select(CrashCase).where(CrashCase.name == '崩溃点17-末尾'))
            job = recovery_service.create_recovery(db, RecoveryInput(crash_case_id=case.id, label='T11原流程演练'))
            db.commit()
            assert 'backup_id' not in job.snapshot and 'chain_ids' not in job.snapshot
            worker.process_one(db)
            db.refresh(job)
            assert job.status == 'completed' and job.result['invariant_passed'] is True
            assert 'backup_restore' not in job.result
            assert job.result['recovered'] == {'A': 80, 'B': 66, 'C': 7} and job.result['losers'] == ['T4']
            db.delete(job); db.commit()

    @check('HTTP冒烟：登录、制作/核验/回收备份、带备份排队演练、导出')
    def _():
        from fastapi.testclient import TestClient
        from src.main import app
        with Session() as db:
            if not db.scalar(select(User).where(User.email == 'operator@haili.local')):
                db.add(User(email='operator@haili.local', role='operator', password_hash=hash_password('Recovery2026!')))
                db.commit()
        client = TestClient(app)
        token = client.post('/auth/login', json={'email': 'operator@haili.local', 'password': 'Recovery2026!'}).json()['access_token']
        h = {'Authorization': f'Bearer {token}'}
        with Session() as db:
            case_id = db.scalar(select(CrashCase.id).where(CrashCase.name == '崩溃点17-末尾'))
        b0 = client.post('/backups', json={'log_set_id': LOGS_ID, 'label': 'T10-全量', 'checkpoint_lsn': 0}, headers=h)
        assert b0.status_code == 201, b0.text
        b1 = client.post('/backups', json={'log_set_id': LOGS_ID, 'label': 'T10-增量', 'parent_id': b0.json()['id'], 'checkpoint_lsn': 4}, headers=h)
        assert b1.status_code == 201 and b1.json()['payload'] == {'A': 80}, b1.text
        blocked = client.delete(f'/backups/{b0.json()["id"]}', headers=h)
        assert blocked.status_code == 409 and '后代' in blocked.json()['detail'], blocked.text
        ver = client.get(f'/backups/{b1.json()["id"]}/restore', headers=h)
        assert ver.status_code == 200 and ver.json()['verified'] and ver.json()['ledger'] == {'A': 80, 'B': 50, 'C': 7}, ver.text
        job = client.post('/recoveries', json={'crash_case_id': case_id, 'label': 'T10带备份演练', 'backup_id': b1.json()['id']}, headers=h)
        assert job.status_code == 202 and job.json()['snapshot']['chain_ids'] == [b0.json()['id'], b1.json()['id']], job.text
        blocked2 = client.delete(f'/backups/{b0.json()["id"]}', headers=h)
        assert blocked2.status_code == 409 and '演练' in blocked2.json()['detail'], blocked2.text
        with Session() as db:
            worker.process_one(db)
        done = client.get(f'/recoveries/{job.json()["id"]}', headers=h)
        assert done.status_code == 200 and done.json()['status'] == 'completed' and done.json()['result']['recovered'], '完成时账本与成功状态一起可见'
        export = client.get(f'/recoveries/{job.json()["id"]}/export', headers=h)
        assert export.status_code == 200 and 'backup_restore' in export.text
        lst = client.get('/backups', headers=h)
        assert any(b['label'] == 'T10-增量' and b['depth'] == 2 for b in lst.json()), lst.text
        with Session() as db:  # 清理
            db.execute(text('DELETE FROM recovery_backups'))
            db.execute(text("DELETE FROM recoveries WHERE label LIKE 'T10-%'"))
            db.execute(text("DELETE FROM backups WHERE label LIKE 'T10-%'"))
            db.execute(text("DELETE FROM audits"))
            db.commit()

    print()
    failed = [(n, e) for n, e in results if e is not None]
    print(f'验证完成：{len(results) - len(failed)}/{len(results)} 通过')
    if failed:
        for n, e in failed: print(f'  失败: {n}: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main()
