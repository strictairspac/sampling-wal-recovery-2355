# 初始化结构
SQLAlchemy metadata.create_all首次建users、log_sets、crash_cases、recoveries、audits、backups、recovery_backups。log_sets.name、crash_cases.name、backups.label唯一，关联使用外键，入队冻结日志和崩溃点快照。backups.parent_id自引用成增量链，chain_hash绑定父哈希、日志集、静止检查点与载荷；recovery_backups记录排队/执行中演练依赖的链环（纯整数列，回收备份时同步清理，不留悬空引用）。暂无在线schema升级器。
