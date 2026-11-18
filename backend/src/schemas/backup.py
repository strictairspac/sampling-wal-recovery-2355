from pydantic import BaseModel, Field
class BackupInput(BaseModel):
    log_set_id: int=Field(gt=0)
    label: str=Field(min_length=2,max_length=120)
    parent_id: int | None=Field(default=None,gt=0)
    checkpoint_lsn: int | None=Field(default=None,ge=0,le=200)
