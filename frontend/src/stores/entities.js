import {logSetsAPI,crashCasesAPI,recoveriesAPI,backupsAPI} from '../api/entities.js';
export const store={logs:[],cases:[],runs:[],backups:[],async refresh(){[this.logs,this.cases,this.runs,this.backups]=await Promise.all([logSetsAPI.list(),crashCasesAPI.list(),recoveriesAPI.list(),backupsAPI.list()]);}};
