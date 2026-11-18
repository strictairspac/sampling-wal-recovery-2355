import {store} from '../stores/entities.js';
import {session} from '../stores/session.js';
import {backupsAPI} from '../api/entities.js';
import {table} from '../components/common/table.js';
import {esc,formObject} from '../utils/html.js';
import {action,message} from '../hooks/action.js';
import {BACKUP_KINDS} from '../constants/enums.js';
export function backupsPage(){return `<div class="section-title"><h2>增量备份链</h2><span>首份全量 · 后续只存相对父备份变化的键</span></div>${table(['编号','备份','类型','日志集','父备份','检查点LSN','链深','链哈希','回收保护','操作'],store.backups.map(b=>[b.id,esc(b.label),esc(BACKUP_KINDS[b.kind]||b.kind),esc(store.logs.find(l=>l.id===b.log_set_id)?.name||'#'+b.log_set_id),b.parent_id?'#'+b.parent_id:'—',b.checkpoint_lsn,b.depth,`<code>${esc(b.chain_hash.slice(0,12))}</code>`,b.blockers.length?esc(b.blockers.join('；')):'可回收',`<button class="secondary" data-verify="${b.id}">核验</button>${session.role!=='viewer'?` <button class="secondary" data-reclaim="${b.id}">回收</button>`:''}`]))}<section class="split"><div><h2>制作一份备份</h2><p>备份绑定同一日志集与静止检查点：首份保存检查点时的全量账本，后续增量只记录相对父备份变化的键。恢复前核验整条链与后续日志，缺父、篡改或跨日志集拼接一律拒绝。回收保护后代与排队演练所需祖先。</p><p class="big-note">链可核验，回收有保护。</p><details id="verify-panel"><summary>链核验结果</summary><pre id="verify-result">选择一份备份，核验整条链并查看物化账本。</pre></details></div>${session.role!=='viewer'?`<form id="backup-form"><label>日志集<select name="log_set_id">${store.logs.map(l=>`<option value="${l.id}">${esc(l.name)}</option>`).join('')}</select></label><label>父备份<select name="parent_id"></select></label><label>检查点LSN<input name="checkpoint_lsn" type="number" min="0" max="200" placeholder="留空取最新静止检查点"></label><label>备份标签<input name="label" value="晚班增量备份" required></label><button type="submit">制作备份</button></form>`:'<p class="readonly">观察员可核验备份链，制作与回收需要操作员或管理员。</p>'}</section>`;}
export function bindBackups(render){
 const form=document.querySelector('#backup-form');
 const logSel=form?.querySelector('[name=log_set_id]');
 const parentSel=form?.querySelector('[name=parent_id]');
 const syncParents=()=>{const lid=Number(logSel.value);parentSel.innerHTML='<option value="">无 · 制作全量基线</option>'+store.backups.filter(b=>b.log_set_id===lid).map(b=>`<option value="${b.id}">#${b.id} ${esc(b.label)} · 检查点${b.checkpoint_lsn}</option>`).join('');};
 logSel?.addEventListener('change',syncParents);if(form)syncParents();
 action(form,async f=>{const d=formObject(f);d.log_set_id=Number(d.log_set_id);if(d.parent_id)d.parent_id=Number(d.parent_id);else delete d.parent_id;if(d.checkpoint_lsn===''||d.checkpoint_lsn==null)delete d.checkpoint_lsn;else d.checkpoint_lsn=Number(d.checkpoint_lsn);await backupsAPI.create(d);await render();});
 document.querySelectorAll('[data-verify]').forEach(btn=>btn.addEventListener('click',async()=>{const panel=document.querySelector('#verify-result');try{const r=await backupsAPI.restore(btn.dataset.verify);panel.textContent=JSON.stringify(r,null,2);message(`备份 #${btn.dataset.verify} 整条链核验通过`);}catch(e){panel.textContent=e.message;message(e.message,true);}document.querySelector('#verify-panel').open=true;}));
 document.querySelectorAll('[data-reclaim]').forEach(btn=>btn.addEventListener('click',async()=>{try{const r=await backupsAPI.reclaim(btn.dataset.reclaim);await render();message(r.already?`备份 #${btn.dataset.reclaim} 此前已回收`:`已回收备份 #${btn.dataset.reclaim}`);}catch(e){message(e.message,true);}}));
}
