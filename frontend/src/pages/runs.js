import {store} from '../stores/entities.js';
import {session} from '../stores/session.js';
import {recoveriesAPI} from '../api/entities.js';
import {table} from '../components/common/table.js';
import {badge} from '../components/common/status.js';
import {esc,formObject} from '../utils/html.js';
import {action} from '../hooks/action.js';
export function runsPage(){return `<section class="split compact"><div><h2>发起一次恢复演练</h2><p>提交时冻结日志与崩溃点，后台独立重做、撤销，再与提交事务结果对照。可选择一份备份作为起点：先核验整条备份链，再从其静止检查点重放后续日志，崩溃点不得早于备份检查点。</p></div>${session.role!=='viewer'?`<form id="run-form"><label>故障场景<select name="crash_case_id">${store.cases.map(c=>`<option value="${c.id}">${esc(c.name)}</option>`).join('')}</select></label><label>起始备份<select name="backup_id"><option value="">不使用 · 从初始账本重放</option>${store.backups.map(b=>`<option value="${b.id}" data-logset="${b.log_set_id}">#${b.id} ${esc(b.label)} · 检查点${b.checkpoint_lsn}</option>`).join('')}</select></label><label>记录标签<input name="label" value="晚班恢复核对" required></label><button type="submit">冻结日志并执行 →</button></form>`:'<p class="readonly">观察员可查看下方恢复结果。</p>'}</section><div class="section-title"><h2>恢复队列</h2><button class="secondary" id="refresh">刷新状态</button></div>${table(['编号','恢复记录','起点','状态','恢复核对','证据'],store.runs.map(r=>[r.id,esc(r.label),r.snapshot?.backup_id?('备份 #'+r.snapshot.backup_id):'初始账本',badge(r.status),r.result?.invariant_passed?'与提交事务一致':r.result?'需检查':'待计算',`<a href="#detail/${r.id}">查看恢复轨迹 →</a>`]))}`;}
export function bindRuns(render){
 document.querySelector('#refresh').onclick=render;
 const form=document.querySelector('#run-form');
 const caseSel=form?.querySelector('[name=crash_case_id]');
 const backupSel=form?.querySelector('[name=backup_id]');
 const sync=()=>{const c=store.cases.find(x=>x.id===Number(caseSel.value));[...backupSel.options].forEach(o=>{if(!o.value)return;const b=store.backups.find(x=>x.id===Number(o.value));o.hidden=!c||!b||b.log_set_id!==c.log_set_id;});if(backupSel.selectedOptions[0]?.hidden)backupSel.value='';};
 caseSel?.addEventListener('change',sync);if(form)sync();
 action(form,async f=>{const d=formObject(f);d.crash_case_id=Number(d.crash_case_id);if(d.backup_id)d.backup_id=Number(d.backup_id);else delete d.backup_id;const job=await recoveriesAPI.create(d);location.hash='#detail/'+job.id;});
}
