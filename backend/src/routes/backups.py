from fastapi import APIRouter,Depends,HTTPException
from src.config.database import get_db
from src.middlewares.auth import current_user,writable
from src.schemas.backup import BackupInput
from src.services import backup_service
from src.services.backup_service import BlockedError
from src.controllers.actions import audit
router=APIRouter()
@router.get('/backups')
def listing(db=Depends(get_db),user=Depends(current_user)):return backup_service.list_backups(db)
@router.post('/backups',status_code=201)
def create(data:BackupInput,db=Depends(get_db),user=Depends(writable)):
    out=backup_service.create_backup(db,data);audit(db,user,f'backup:create:{out["id"]}');db.commit();return out
@router.get('/backups/{ident}/restore')
def restore(ident:int,db=Depends(get_db),user=Depends(current_user)):
    try:restored=backup_service.restore_backup(db,ident)
    except ValueError as exc:raise HTTPException(422,str(exc))
    return {'verified':True,'backup_id':ident,'log_set_id':restored['log_set'].id,
            'checkpoint_lsn':restored['checkpoint_lsn'],'ledger':restored['ledger'],
            'chain':backup_service.chain_evidence(restored['chain'])}
@router.delete('/backups/{ident}')
def reclaim(ident:int,db=Depends(get_db),user=Depends(writable)):
    try:removed=backup_service.reclaim_backup(db,ident)
    except BlockedError as exc:raise HTTPException(409,'；'.join(exc.blockers))
    audit(db,user,f'backup:reclaim:{ident}');db.commit()
    return {'id':ident,'reclaimed':True,'already':removed is None}
