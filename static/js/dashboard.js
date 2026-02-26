const REFRESH_COOLDOWN_SEC=8;
let LAST_ALERT='';
let cooldown=0; let timerId=null;
// Read a <script type="application/json"> element by id (server-injected data)
function _readJsonEl(id){try{const el=document.getElementById(id);return el?JSON.parse(el.textContent):null;}catch(_e){return null;}}
// ── Funding countdown: one global tick per second, independent of API polling ──
// Timestamps are stored as data-next-funding-buy / data-next-funding-sell on each <tr>.
const EMPTY_TIMER='--:--:--';
const rowMap=new Map();
function _fmtTime(sec){sec=Math.max(0,Math.floor(sec));const h=Math.floor(sec/3600),m=Math.floor((sec%3600)/60),s=sec%60;return String(h).padStart(2,'0')+':'+String(m).padStart(2,'0')+':'+String(s).padStart(2,'0');}
setInterval(function(){
  const now=Date.now();
  rowMap.forEach(function(tr){
    const buyTs=Number(tr.dataset.nextFundingBuy)||0;
    const sellTs=Number(tr.dataset.nextFundingSell)||0;
    const spans=tr.querySelectorAll('[data-col="feta"] span');
    if(spans[0])spans[0].textContent=buyTs>now?_fmtTime(Math.floor((buyTs-now)/1000)):EMPTY_TIMER;
    if(spans[1])spans[1].textContent=sellTs>now?_fmtTime(Math.floor((sellTs-now)/1000)):EMPTY_TIMER;
  });
},1000);

// ────────────────────────────────────────────────────────────────────────────
let STATE={config:null,data:null,pinned:new Set(JSON.parse(localStorage.getItem('pinnedPairs')||'[]')),theme:localStorage.getItem('theme')||'theme-classic',sound:(localStorage.getItem('soundOn')||'0')==='1',lang:localStorage.getItem('lang')||'ru',soundFile:localStorage.getItem('soundFile')||'sms.wav',assets:{logos:{},sounds:[]},sortKey:'spread',sortDir:'desc',token:localStorage.getItem('authToken')||'',user:null,publicKey:'',authMode:'login'};
const I18N={
  ru:{filterTitle:'Фильтр',search:'Поиск монеты',vol:'Оборот 24h (USD)',spread:'OpenSpread, %',lang:'Язык',theme:'Тема',alert:'Оповещение',ex:'Биржи',clearFilters:'Очистить фильтр',clear:'Очистить',token:'Токен',pair:'Покупка / Продажа',price:'Цена вход/выход',register:'Регистрация',login:'Вход',logout:'Выход',guestAccess:'Гость: доступ',guestLimit:'до 2% спреда',userPrefix:'Пользователь:',adminRole:'admin (без лимита)',userRole:'пользователь (без лимита)',cancelBtn:'Скрыть',continueBtn:'Продолжить',registerBtn:'Зарегистрироваться',loginBtn:'Войти',loadUsers:'Загрузить пользователей',noAccess:'Нет доступа',enterCreds:'Введите логин и пароль',regOk:'Регистрация успешна',regErr:'Ошибка регистрации: ',loginErr:'Ошибка входа: ',showFilter:'Показать фильтр',hideFilter:'Скрыть фильтр',soundCheck:'звук',notFound:'Ничего не найдено.',loading:'Загрузка...',disableSub:'Отключить подписку',approveSub:'Подтвердить подписку',deleteUser:'🗑 Удалить',confirmDelete:'Удалить пользователя',tgPlaceholder:'telegram_username',tgLabel:'Telegram (необязательно)',linkTg:'🔗 Привязать Telegram',linkTgPending:'⏳ Telegram не привязан',linkTgLinked:'✅ Telegram привязан',subIndefinite:'Бессрочно',days30:'30 дней',days60:'60 дней',days90:'90 дней',days180:'180 дней',days365:'365 дней',expiresOn:'до',collapseUsers:'Свернуть ▲',expandUsers:'Развернуть ▼',subDaysLeft:'дн.',subUnlimited:'∞'},
  uk:{filterTitle:'Фільтр',search:'Пошук монети',vol:'Обсяг 24h (USD)',spread:'OpenSpread, %',lang:'Мова',theme:'Тема',alert:'Сповіщення',ex:'Біржі',clearFilters:'Очистити фільтр',clear:'Очистити',token:'Токен',pair:'Купівля / Продаж',price:'Ціна вхід/вихід',register:'Реєстрація',login:'Вхід',logout:'Вихід',guestAccess:'Гість: доступ',guestLimit:'до 2% спреду',userPrefix:'Користувач:',adminRole:'admin (без ліміту)',userRole:'користувач (без ліміту)',cancelBtn:'Сховати',continueBtn:'Продовжити',registerBtn:'Зареєструватися',loginBtn:'Увійти',loadUsers:'Завантажити користувачів',noAccess:'Немає доступу',enterCreds:'Введіть логін і пароль',regOk:'Реєстрація успішна',regErr:'Помилка реєстрації: ',loginErr:'Помилка входу: ',showFilter:'Показати фільтр',hideFilter:'Сховати фільтр',soundCheck:'звук',notFound:'Нічого не знайдено.',loading:'Завантаження...',disableSub:'Вимкнути підписку',approveSub:'Підтвердити підписку',deleteUser:'🗑 Видалити',confirmDelete:'Видалити користувача',tgPlaceholder:'telegram_username',tgLabel:'Telegram (необов\'язково)',linkTg:'🔗 Прив\'язати Telegram',linkTgPending:'⏳ Telegram не прив\'язано',linkTgLinked:'✅ Telegram прив\'язано',subIndefinite:'Безстроково',days30:'30 днів',days60:'60 днів',days90:'90 днів',days180:'180 днів',days365:'365 днів',expiresOn:'до',collapseUsers:'Згорнути ▲',expandUsers:'Розгорнути ▼',subDaysLeft:'дн.',subUnlimited:'∞'},
  en:{filterTitle:'Filter',search:'Search coin',vol:'24h Volume (USD)',spread:'OpenSpread, %',lang:'Language',theme:'Theme',alert:'Alert',ex:'Exchanges',clearFilters:'Clear filter',clear:'Clear',token:'Token',pair:'Buy / Sell',price:'Entry/Exit price',register:'Register',login:'Login',logout:'Logout',guestAccess:'Guest: access',guestLimit:'up to 2% spread',userPrefix:'User:',adminRole:'admin (no limit)',userRole:'user (no limit)',cancelBtn:'Hide',continueBtn:'Continue',registerBtn:'Register',loginBtn:'Sign in',loadUsers:'Load users',noAccess:'No access',enterCreds:'Enter login and password',regOk:'Registration successful',regErr:'Registration error: ',loginErr:'Login error: ',showFilter:'Show filter',hideFilter:'Hide filter',soundCheck:'sound',notFound:'Nothing found.',loading:'Loading...',disableSub:'Disable subscription',approveSub:'Approve subscription',deleteUser:'🗑 Delete',confirmDelete:'Delete user',tgPlaceholder:'telegram_username',tgLabel:'Telegram (optional)',linkTg:'🔗 Link Telegram',linkTgPending:'⏳ Telegram not linked',linkTgLinked:'✅ Telegram linked',subIndefinite:'Indefinite',days30:'30 days',days60:'60 days',days90:'90 days',days180:'180 days',days365:'365 days',expiresOn:'until',collapseUsers:'Collapse ▲',expandUsers:'Expand ▼',subDaysLeft:'d.',subUnlimited:'∞'}
};
const FALLBACK_LOGO={MEXC:'',Bybit:'',BingX:''};

const fmtPct=(x,d=2)=>Number.isFinite(x)?(x*100).toFixed(d)+'%':'N/A';
const fmtUsd=x=>!Number.isFinite(x)?'N/A':(x>=1e9?(x/1e9).toFixed(2)+'b$':x>=1e6?(x/1e6).toFixed(2)+'m$':x>=1e3?(x/1e3).toFixed(1)+'k$':Math.round(x)+'$');
const fmtPrice=x=>Number.isFinite(x)?x.toFixed(Math.abs(x)>=1?6:10).replace(/0+$/,'').replace(/\.$/,''):'N/A';
function authHeaders(base={}){if(STATE.token)base['Authorization']=`Bearer ${STATE.token}`; return base;}
const apiGet=async p=>(await fetch(p,{cache:'no-store',headers:authHeaders({})})).json();
const apiPost=async(p,b)=>(await fetch(p,{method:'POST',headers:authHeaders({'Content-Type':'application/json'}),body:JSON.stringify(b)})).json();

function b64(arr){let s=''; const bytes=new Uint8Array(arr); for(const b of bytes)s+=String.fromCharCode(b); return btoa(s);}
async function ensurePubKey(){if(STATE.publicKey)return STATE.publicKey; const j=await (await fetch('/api/auth/pubkey',{cache:'no-store'})).json(); STATE.publicKey=j.public_key||''; return STATE.publicKey;}
// Singleton promise so concurrent callers share one importKey operation.
// On failure the promise is reset to null so the next call can retry.
let _keyImportPromise=null;
async function _importPubKey(){
  if(!_keyImportPromise){
    _keyImportPromise=ensurePubKey().then(pem=>{
      const clean=pem.replace(/-----BEGIN PUBLIC KEY-----|-----END PUBLIC KEY-----|\s/g,'');
      const der=Uint8Array.from(atob(clean),c=>c.charCodeAt(0));
      return crypto.subtle.importKey('spki',der.buffer,{name:'RSA-OAEP',hash:'SHA-256'},false,['encrypt']);
    }).catch(err=>{_keyImportPromise=null; throw err;});
  }
  return _keyImportPromise;
}
async function encryptWithPub(plain){
  const key=await _importPubKey();
  const enc=await crypto.subtle.encrypt({name:'RSA-OAEP'},key,new TextEncoder().encode(plain));
  return b64(enc);
}
function setAuthStateText(msg){document.getElementById('authState').textContent=msg;}
function openAuthForm(mode){STATE.authMode=mode; const f=document.getElementById('authForm'); f.style.display='flex'; document.getElementById('authContainer').style.display='block'; const t=I18N[STATE.lang]||I18N.ru; document.getElementById('btnAuthSubmit').textContent=mode==='register'?t.registerBtn:t.loginBtn; const tgRow=document.getElementById('authTgRow'); if(tgRow){tgRow.style.display=mode==='register'?'flex':'none'; const tgInput=document.getElementById('authTg'); if(tgInput){tgInput.placeholder=t.tgPlaceholder||'telegram_username'; if(mode!=='register')tgInput.value='';}}} 
function closeAuthForm(){document.getElementById('authForm').style.display='none'; if(document.getElementById('adminBox').style.display!=='block') document.getElementById('authContainer').style.display='none';}
async function registerUser(){
  const btn=document.getElementById('btnAuthSubmit');
  if(btn.disabled)return;
  const u=document.getElementById('authUser').value.trim();
  const p=document.getElementById('authPass').value;
  const tgRaw=(document.getElementById('authTg')||{}).value||'';
  const tg=tgRaw.trim().replace(/^@/,'');
  const t=I18N[STATE.lang]||I18N.ru;
  if(!u||!p){setAuthStateText(t.enterCreds);return;}
  btn.disabled=true; const origTxt=btn.textContent; btn.textContent='...';
  try{
    let payload={username:u,password:p,tg_username:tg};
    try{const[ue,pe]=await Promise.all([encryptWithPub(u),encryptWithPub(p)]);payload={username:u,password:p,username_enc:ue,password_enc:pe,tg_username:tg};}catch(_e){}
    const r=await apiPost('/api/auth/register',payload);
    setAuthStateText(r.ok?t.regOk:t.regErr+(r.error||'unknown'));
    if(r.ok)closeAuthForm();
  }catch(err){setAuthStateText(t.regErr+(err.message||'network_error'));}
  finally{btn.disabled=false;btn.textContent=origTxt;}
}
async function loginUser(){
  const btn=document.getElementById('btnAuthSubmit');
  if(btn.disabled)return;
  const u=document.getElementById('authUser').value.trim();
  const p=document.getElementById('authPass').value;
  const t=I18N[STATE.lang]||I18N.ru;
  if(!u||!p){setAuthStateText(t.enterCreds);return;}
  btn.disabled=true; const origTxt=btn.textContent; btn.textContent='...';
  try{
    let payload={username:u,password:p};
    try{const[ue,pe]=await Promise.all([encryptWithPub(u),encryptWithPub(p)]);payload={username:u,password:p,username_enc:ue,password_enc:pe};}catch(_e){}
    const r=await apiPost('/api/auth/login',payload);
    if(!r.ok){setAuthStateText(t.loginErr+(r.error||'bad_login'));return;}
    STATE.token=r.token||''; localStorage.setItem('authToken',STATE.token); STATE.user=r.user||null;
    closeAuthForm(); renderAuth();
    try{await refreshData();}catch(_e){}
  }catch(err){setAuthStateText(t.loginErr+(err.message||'network_error'));}
  finally{btn.disabled=false;btn.textContent=origTxt;}
}
async function logoutUser(){
  try{await apiPost('/api/auth/logout',{});}catch(_e){}
  STATE.token=''; STATE.user=null; localStorage.removeItem('authToken');
  closeAuthForm(); renderAuth();
  try{await refreshData();}catch(_e){}
}
async function loadMe(){if(!STATE.token){STATE.user=null; return;} const r=await apiGet('/api/auth/me'); if(!r.ok){STATE.token=''; STATE.user=null; localStorage.removeItem('authToken'); return;} STATE.user=r.user;}
function renderAuth(){
  const u=STATE.user;
  const t=I18N[STATE.lang]||I18N.ru;
  const adminBox=document.getElementById('adminBox');
  const authContainer=document.getElementById('authContainer');
  const bLogin=document.getElementById('btnLogin');
  const bReg=document.getElementById('btnRegister');
  const bOut=document.getElementById('btnLogout');
  // Remove any previously injected tg-link button to avoid duplicates
  const prevTgBtn=document.getElementById('btnLinkTg');
  if(prevTgBtn) prevTgBtn.remove();
  const lim=STATE.data&&STATE.data.access&&Number.isFinite(STATE.data.access.spread_limit)?`до ${(STATE.data.access.spread_limit*100).toFixed(0)}%`:'';
  if(!u){
    setAuthStateText(`${t.guestAccess} ${lim||t.guestLimit}`);
    adminBox.style.display='none';
    // Hide authContainer only if form is also closed
    if(document.getElementById('authForm').style.display!=='flex') authContainer.style.display='none';
    bLogin.style.display='inline-block';
    bReg.style.display='inline-block';
    bOut.style.display='none';
    return;
  }
  const status=u.is_admin?t.adminRole:t.userRole;
  // Build subscription badge for non-admin subscribers
  let subBadge='';
  if(!u.is_admin && u.subscription_approved){
    if(u.subscription_expires){
      const daysLeft=Math.max(0,Math.ceil((u.subscription_expires*1000-Date.now())/(86400*1000)));
      subBadge=` • ⏳ ${daysLeft} ${t.subDaysLeft||'д.'}`;
    } else {
      subBadge=` • ${t.subUnlimited||'∞'}`;
    }
  }
  setAuthStateText(`${t.userPrefix} ${u.username} • ${status}${subBadge}`);
  adminBox.style.display=u.is_admin?'block':'none';
  if(u.is_admin) authContainer.style.display='block';
  else if(document.getElementById('authForm').style.display!=='flex') authContainer.style.display='none';
  bLogin.style.display='none';
  bReg.style.display='none';
  bOut.style.display='inline-block';
  // Show Telegram link button when tg_username set but tg_chat_id not yet resolved
  if(u.tg_username && !u.tg_chat_id){
    const btn=document.createElement('button');
    btn.id='btnLinkTg'; btn.className='btn'; btn.style.cssText='margin-left:6px;font-size:11px;padding:3px 8px;';
    btn.title=t.linkTgPending; btn.textContent=t.linkTg;
    btn.onclick=async()=>{
      btn.disabled=true; btn.textContent='…';
      try{
        const r=await apiGet('/api/user/link-code');
        if(r&&r.ok&&r.link){ window.open(r.link,'_blank','noopener'); btn.textContent=t.linkTgPending; }
        else { btn.textContent=t.linkTg; }
      }catch(_e){ btn.textContent=t.linkTg; }
      finally{ btn.disabled=false; }
    };
    bOut.after(btn);
  } else if(u.tg_chat_id){
    // Already linked — show a small green indicator
    const span=document.createElement('span');
    span.id='btnLinkTg'; span.style.cssText='margin-left:6px;font-size:11px;color:#4caf50;';
    span.textContent=t.linkTgLinked;
    bOut.after(span);
  }
}
async function loadUsersAdmin(){
  const t=I18N[STATE.lang]||I18N.ru;
  const r=await apiGet('/api/admin/users');
  if(!r.ok){document.getElementById('adminUsers').textContent=t.noAccess; return;}
  const box=document.getElementById('adminUsers');
  box.innerHTML='';
  // Show/reset collapse button and ensure list is visible after (re)load
  const colBtn=document.getElementById('btnCollapseUsers');
  const t2=I18N[STATE.lang]||I18N.ru;
  box.style.display=''; colBtn.style.display=''; colBtn.textContent=t2.collapseUsers;
  r.users.forEach(x=>{
    const row=document.createElement('div'); row.style.cssText='margin:4px 0;display:flex;align-items:center;flex-wrap:wrap;gap:6px;';
    const label=document.createElement('span');
    const tgInfo=x.tg_username?` @${x.tg_username}${x.tg_chat_id?' 🔗':' ⏳'}`:'';
    const expiresStr=x.subscription_expires?` ${t.expiresOn} ${new Date(x.subscription_expires*1000).toLocaleDateString()}`:'';
    label.textContent=`${x.username}${x.is_admin?' (admin)':''}${tgInfo} ${x.subscription_approved?'✅'+expiresStr:'⏳'}`;
    label.style.cssText='flex:1;min-width:140px;';
    row.appendChild(label);
    if(!x.is_admin){
      // Period selector (shown only when subscribing)
      const selDays=document.createElement('select'); selDays.className='btn'; selDays.style.cssText='padding:4px 6px;font-size:12px;';
      [[0,t.subIndefinite],[30,t.days30],[60,t.days60],[90,t.days90],[180,t.days180],[365,t.days365]].forEach(([v,lbl])=>{
        const o=document.createElement('option'); o.value=v; o.textContent=lbl; selDays.appendChild(o);
      });
      row.appendChild(selDays);
      const btnSub=document.createElement('button'); btnSub.className='btn'; btnSub.style.cssText='padding:4px 10px;font-size:12px;';
      btnSub.textContent=x.subscription_approved?t.disableSub:t.approveSub;
      btnSub.onclick=async()=>{
        btnSub.disabled=true; selDays.disabled=true;
        const days=x.subscription_approved?0:parseInt(selDays.value)||0;
        await apiPost('/api/admin/subscription',{username:x.username,approved:!x.subscription_approved,days});
        await loadUsersAdmin();
      };
      row.appendChild(btnSub);
      const btnDel=document.createElement('button'); btnDel.className='btn btn-danger'; btnDel.style.cssText='padding:4px 10px;font-size:12px;';
      btnDel.textContent=t.deleteUser;
      btnDel.onclick=async()=>{
        if(!confirm(`${t.confirmDelete}: ${x.username}?`)) return;
        btnDel.disabled=true;
        const resp=await apiPost('/api/admin/delete-user',{username:x.username});
        if(resp&&resp.ok) await loadUsersAdmin();
        else{btnDel.disabled=false; alert(resp?.error||'error');}
      };
      row.appendChild(btnDel);
    }
    box.appendChild(row);
  });
}

function parseVolumeInput(raw){const s=(raw||'').toString().trim().toLowerCase().replace(',', '.').replace('м','m'); if(!s) return 0; const m=s.match(/^([0-9]+(?:\.[0-9]+)?)([kmb])?$/i); if(!m) return parseFloat(s)||0; const v=parseFloat(m[1]); const suf=(m[2]||'').toLowerCase(); if(suf==='k') return v*1e3; if(suf==='m') return v*1e6; if(suf==='b') return v*1e9; return v;}

function logoFor(ex){return STATE.assets.logos?.[ex]||FALLBACK_LOGO[ex]||'';}
function applyTheme(){document.body.className=STATE.theme; document.getElementById('themeSel').value=STATE.theme; localStorage.setItem('theme',STATE.theme);}
function applyLang(){const t=I18N[STATE.lang]||I18N.ru; document.getElementById('filterTitle').textContent=t.filterTitle; document.getElementById('lblSearch').textContent=t.search; document.getElementById('lblMinVol').textContent=t.vol; document.getElementById('lblMinSpread').textContent=t.spread; document.getElementById('lblTheme').textContent=t.theme; document.getElementById('lblSound').textContent=t.alert; document.getElementById('lblExchanges').textContent=t.ex; document.getElementById('clearFiltersBtn').textContent=t.clearFilters; document.getElementById('thToken').textContent=t.token; document.getElementById('thPair').textContent=t.pair; document.getElementById('thPrice').childNodes[0].textContent=t.price; document.getElementById('btnRegister').textContent=t.register; document.getElementById('btnLogin').textContent=t.login; document.getElementById('btnLogout').textContent=t.logout; document.getElementById('btnAuthCancel').textContent=t.cancelBtn; document.getElementById('btnLoadUsers').textContent=t.loadUsers; const colBtn=document.getElementById('btnCollapseUsers'); if(colBtn.style.display!=='none'){const box=document.getElementById('adminUsers'); colBtn.textContent=box.style.display==='none'?t.expandUsers:t.collapseUsers;} const fp=document.getElementById('filterPanel'); const ftBtn=document.getElementById('filterToggleBtn'); ftBtn.textContent=fp.classList.contains('open')?t.hideFilter:t.showFilter; document.getElementById('langSel').value=STATE.lang; localStorage.setItem('lang',STATE.lang); renderAuth();}
function setCooldown(sec){cooldown=sec; const btn=document.getElementById('refreshBtn'); if(timerId)clearInterval(timerId); timerId=setInterval(()=>{cooldown=Math.max(0,cooldown-1); btn.disabled=cooldown>0; btn.textContent=cooldown>0?`↻ Refresh (${cooldown})`:'↻ Refresh'; document.getElementById('cooldownBadge').textContent=`Manual refresh cooldown: ${cooldown}s`; if(cooldown===0){clearInterval(timerId);timerId=null;}},1000); btn.disabled=true; btn.textContent=`↻ Refresh (${cooldown})`;}
function pairKey(r){return `${r.symbol}|${r.buy_ex}|${r.sell_ex}`;}
function isPinnedPair(r){return STATE.pinned.has(pairKey(r));}
function togglePinnedPair(r){const k=pairKey(r); if(STATE.pinned.has(k))STATE.pinned.delete(k); else STATE.pinned.add(k); localStorage.setItem('pinnedPairs',JSON.stringify([...STATE.pinned])); render();}
function refreshSortIndicators(){document.querySelectorAll('th.sortable').forEach(th=>{const key=th.getAttribute('data-sort'); th.querySelector('.arr').textContent=(key===STATE.sortKey)?(STATE.sortDir==='asc'?'▲':'▼'):'↕';});}

function renderExchangeFilters(){const box=document.getElementById('exchangeBox'); box.innerHTML=''; ['MEXC','Bybit','BingX'].forEach(ex=>{const chip=document.createElement('label'); const on=!!STATE.config.enabled?.[ex]; chip.className='chip'+(on?'':' off'); const logo=logoFor(ex); chip.innerHTML=`<input type="checkbox" ${on?'checked':''}/> ${logo?`<img src="${logo}" alt="${ex}"/>`:''} ${ex}`; chip.onclick=async (e)=>{e.preventDefault(); const en={...(STATE.config.enabled||{})}; en[ex]=!en[ex]; STATE.config=await apiPost('/api/config',{enabled:en}); renderExchangeFilters(); await refreshData();}; box.appendChild(chip);});}
function clearAllFilters(){document.getElementById('q').value=''; document.getElementById('minVol').value='0'; localStorage.setItem('minVolInput','0'); document.getElementById('minSpread').value='0%'; STATE.config.min_vol=0; STATE.config.min_spread=0; STATE.config.enabled={MEXC:true,Bybit:true,BingX:true}; apiPost('/api/config',{min_vol:0,min_spread:0,enabled:STATE.config.enabled}).then(async c=>{STATE.config=c; renderExchangeFilters(); await refreshData();});}

function applyFilters(rows){const q=(document.getElementById('q').value||'').trim().toUpperCase(); const minVol=parseVolumeInput(document.getElementById('minVol').value||'0'); const minSp=parsePctInput(document.getElementById('minSpread').value||'0'); return rows.filter(r=>{const sym=(r.symbol||'').toUpperCase(); if(q && !sym.startsWith(q)) return false; if(minVol>0){const buyOk=Number.isFinite(r.buy_vol)?r.buy_vol>=minVol:true; const sellOk=Number.isFinite(r.sell_vol)?r.sell_vol>=minVol:true; if(!(buyOk&&sellOk)) return false;} if(Number.isFinite(minSp)&&minSp>0&&!(r.spread>=minSp)) return false; return true;});}
function sortRows(rows){const key=STATE.sortKey; const dir=STATE.sortDir==='asc'?1:-1; rows.sort((a,b)=>{const pa=isPinnedPair(a)?1:0; const pb=isPinnedPair(b)?1:0; if(pa!==pb) return pb-pa; const va=Number.isFinite(a[key])?a[key]:-Infinity; const vb=Number.isFinite(b[key])?b[key]:-Infinity; if(va<vb) return -1*dir; if(va>vb) return 1*dir; return 0;});}
function fundingClass(v){if(!Number.isFinite(v)) return ''; return v<0?'fneg':'fpos';}
function spreadClass(v){if(!Number.isFinite(v)) return 'neg'; return v<0?'neg':'pos';}

async function playAlert(){ if(!STATE.sound) return; try{ if(STATE.soundFile){const a=new Audio(`/assets/sounds/${encodeURIComponent(STATE.soundFile)}`); a.volume=0.8; await a.play(); return;} }catch(_e){} try{const ac=new (window.AudioContext||window.webkitAudioContext)(); const o=ac.createOscillator(); const g=ac.createGain(); o.type='triangle'; o.frequency.value=920; g.gain.setValueAtTime(0.0001,ac.currentTime); g.gain.exponentialRampToValueAtTime(0.18,ac.currentTime+0.01); g.gain.exponentialRampToValueAtTime(0.0001,ac.currentTime+0.14); o.connect(g); g.connect(ac.destination); o.start(); o.stop(ac.currentTime+0.15);}catch(_e2){} }

function render(){
if(!STATE.data)return;
const srvLimit=(STATE.data.access&&Number.isFinite(STATE.data.access.spread_limit))?STATE.data.access.spread_limit:null;
document.getElementById('updated').textContent=`Updated: ${STATE.data.updated_at||'—'}`;
const dbgEl=document.getElementById('dbg'); const dbg=(STATE.data&&STATE.data.dbg)||{}; if(STATE.user&&STATE.user.is_admin){dbgEl.style.display='inline-block'; if(dbg.loading){dbgEl.textContent='⏳ DBG: loading...';}else{dbgEl.textContent=`DBG${dbg.ws_mode?' WS':''} mexc=${dbg.mexc??'?'} bybit=${dbg.bybit??'?'} bingx=${dbg.bingx??'?'} kept=${dbg.kept??'?'} took=${dbg.took_ms??'?'}ms`;}} else {dbgEl.style.display='none';}
let rows=[...(STATE.data.rows||[])];
if(srvLimit!==null){rows=rows.filter(r=>Number.isFinite(r.spread)?r.spread<=srvLimit:false);}
rows=applyFilters(rows);
sortRows(rows);
refreshSortIndicators();
const tb=document.getElementById('tbody');
[...tb.querySelectorAll('tr:not([data-key])')].forEach(tr=>tr.remove());
if(!rows.length){rowMap.forEach(tr=>{if(tr.parentNode===tb)tb.removeChild(tr);}); const e=document.createElement('tr'); e.innerHTML=`<td colspan="10" class="empty-row">${(I18N[STATE.lang]||I18N.ru).notFound}</td>`; tb.appendChild(e); return;}
const top=rows[0];
const alertKey=`${top.symbol}|${top.buy_ex}|${top.sell_ex}|${(top.spread||0).toFixed(4)}`;
if(alertKey!==LAST_ALERT){LAST_ALERT=alertKey; playAlert();}

const split=(a,b,col='',lbl='')=>`<td class='split-cell mono' data-col='${col}' data-label='${lbl}'><div class='line'>${a}</div><div class='line'>${b}</div></td>`;
const currentKeys=new Set();
rows.forEach(r=>{
  const rKey=pairKey(r);
  currentKeys.add(rKey);
  const pin=isPinnedPair(r);
  let tr=rowMap.get(rKey);
  if(tr){
    // Existing row: update only dynamic cells; keep timer spans intact to avoid flicker
    tr.className=pin?'pinned':'';
    if(!Number(tr.dataset.nextFundingBuy)||Number(tr.dataset.nextFundingBuy)<Date.now())tr.dataset.nextFundingBuy=r.buy_next_ts_ms||0;
    if(!Number(tr.dataset.nextFundingSell)||Number(tr.dataset.nextFundingSell)<Date.now())tr.dataset.nextFundingSell=r.sell_next_ts_ms||0;
    const fav=tr.querySelector('.fav'); if(fav){fav.textContent=pin?'★':'☆'; fav.onclick=()=>togglePinnedPair(r);}
    const priceCell=tr.querySelector('[data-col="price"]'); if(priceCell){const ls=priceCell.querySelectorAll('.line'); if(ls[0])ls[0].textContent=fmtPrice(r.buy_ask); if(ls[1])ls[1].textContent=fmtPrice(r.sell_bid);}
    const fundCell=tr.querySelector('[data-col="funding"]'); if(fundCell){const ls=fundCell.querySelectorAll('.line'); if(ls[0])ls[0].innerHTML=`${fmtPct(r.buy_funding,3)} / <span id="ivl-${rKey}-buy">${r.buy_funding_interval||'8h'}</span>`; if(ls[1])ls[1].innerHTML=`${fmtPct(r.sell_funding,3)} / <span id="ivl-${rKey}-sell">${r.sell_funding_interval||'8h'}</span>`;}
    const fspreadCell=tr.querySelector('[data-col="fspread"]'); if(fspreadCell){fspreadCell.className=`mono ${fundingClass(r.funding_spread)}`; fspreadCell.textContent=fmtPct(r.funding_spread,3);}
    const spreadCell=tr.querySelector('[data-col="spread"]'); if(spreadCell){const sp=spreadCell.querySelector('.spread-pill'); if(sp){sp.className=`spread-pill ${spreadClass(r.spread)}`; sp.textContent=fmtPct(r.spread,2);}}
    const volCell=tr.querySelector('[data-col="vol"]'); if(volCell){const ls=volCell.querySelectorAll('.line'); if(ls[0])ls[0].textContent=fmtUsd(r.buy_vol); if(ls[1])ls[1].textContent=fmtUsd(r.sell_vol);}
    tb.appendChild(tr);
    return;
  }
  // New row: build full DOM once, register in rowMap so it is never recreated
  tr=document.createElement('tr'); tr.dataset.key=rKey;
  tr.className=pin?'pinned':'';
  const lbuy=logoFor(r.buy_ex);
  const lsell=logoFor(r.sell_ex);
  tr.innerHTML=`
    <td data-col='fav'><span class='fav'>${pin?'★':'☆'}</span></td>
    <td class='token' data-col='token' data-label=''>${r.symbol.replace('USDT','')}</td>
    <td class='split-cell' data-col='pair' data-label=''>
      <div class='line pair-line long'>⬆ LONG ${lbuy?`<img class='xlogo' src='${lbuy}'/>`:''} <a href='${r.buy_url}' target='_blank'>${r.buy_ex}</a></div>
      <div class='line pair-line short'>⬇ SHORT ${lsell?`<img class='xlogo' src='${lsell}'/>`:''} <a href='${r.sell_url}' target='_blank'>${r.sell_ex}</a></div>
    </td>
    ${split(fmtPrice(r.buy_ask),fmtPrice(r.sell_bid),'price','Цена')}
    ${split(`${fmtPct(r.buy_funding,3)} / <span id="ivl-${rKey}-buy">${r.buy_funding_interval||'8h'}</span>`,`${fmtPct(r.sell_funding,3)} / <span id="ivl-${rKey}-sell">${r.sell_funding_interval||'8h'}</span>`,'funding','Funding')}
    <td class='split-cell mono' data-col='feta' data-label='ETA'><div class='line'><span id='timer-${rKey}-buy'>--:--:--</span></div><div class='line'><span id='timer-${rKey}-sell'>--:--:--</span></div></td>
    <td class='mono ${fundingClass(r.funding_spread)}' data-col='fspread' data-label='F.Спред'>${fmtPct(r.funding_spread,3)}</td>
    <td data-col='spread' data-label=''><span class='spread-pill ${spreadClass(r.spread)}'>${fmtPct(r.spread,2)}</span></td>
    ${split(fmtUsd(r.buy_vol),fmtUsd(r.sell_vol),'vol','Объём')}
    <td data-col='graf' data-label=''><a class='btn' style='padding:4px 8px;font-size:12px' href='/graph?pair_key=${encodeURIComponent(pairKey(r))}' target='_blank' rel='noopener'>Grafic</a></td>
  `;
  tr.querySelector('.fav').onclick=()=>togglePinnedPair(r);
  // Store funding timestamps once; the global timer reads them every second
  tr.dataset.nextFundingBuy=r.buy_next_ts_ms||0;
  tr.dataset.nextFundingSell=r.sell_next_ts_ms||0;
  tb.appendChild(tr);
  rowMap.set(rKey,tr);
});
rowMap.forEach((tr,key)=>{if(!currentKeys.has(key)&&tr.parentNode===tb)tb.removeChild(tr);});
}

let _lastDataEtag='';
async function refreshData(){
  try{
    // Send If-None-Match manually so 304 works regardless of browser cache policy.
    // cache:'no-cache' still bypasses stale browser cache entries.
    const hdrs=authHeaders({});
    if(_lastDataEtag)hdrs['If-None-Match']=_lastDataEtag;
    const resp=await fetch('/api/data',{cache:'no-cache',headers:hdrs});
    if(resp.status===304)return;  // data unchanged → skip re-render
    if(resp.status===202)return;  // server loading, keep existing STATE.data
    const etag=resp.headers.get('etag');
    if(etag)_lastDataEtag=etag;
    const _d=await resp.json();
    // Never store a loading placeholder — server returns 202 for that case (handled above),
    // but skip as a safety net if somehow a loading payload arrives on a 200 response.
    if(_d&&_d.dbg&&_d.dbg.loading)return;
    STATE.data=_d;
  }catch(e){console.error('refreshData failed',e);return;}
  render();
}

let EVENTS_BOUND=false;
function bindUiEvents(){
  if(EVENTS_BOUND) return;
  EVENTS_BOUND=true;
  document.getElementById('q').addEventListener('input',render);
  document.getElementById('minVol').addEventListener('change',async e=>{const raw=(e.target.value||'0').trim(); const v=Math.max(0,parseVolumeInput(raw)); localStorage.setItem('minVolInput',raw||'0'); try{STATE.config=await apiPost('/api/config',{min_vol:v});}catch(err){console.error(err);} await refreshData();});
  document.getElementById('minSpread').addEventListener('change',async e=>{const v=parsePctInput(e.target.value||'0'); e.target.value=(v*100).toFixed(2).replace(/\.00$/,'')+'%'; try{STATE.config=await apiPost('/api/config',{min_spread:v});}catch(err){console.error(err);} await refreshData();});
  document.getElementById('themeSel').addEventListener('change',e=>{STATE.theme=e.target.value; applyTheme();});
  document.getElementById('langSel').addEventListener('change',e=>{STATE.lang=e.target.value; applyLang(); render();});
  document.getElementById('soundToggle').addEventListener('change',e=>{STATE.sound=!!e.target.checked; localStorage.setItem('soundOn',STATE.sound?'1':'0'); if(STATE.sound) playAlert();});
  document.getElementById('soundSel').addEventListener('change',e=>{STATE.soundFile=e.target.value; localStorage.setItem('soundFile',STATE.soundFile);});
  document.getElementById('refreshBtn').addEventListener('click',async()=>{if(cooldown>0)return; setCooldown(REFRESH_COOLDOWN_SEC); try{await apiPost('/api/refresh',{});}catch(err){console.error(err);} await refreshData();});
  document.getElementById('clearFiltersBtn').addEventListener('click',clearAllFilters);
  document.getElementById('filterToggleBtn').addEventListener('click',()=>{const p=document.getElementById('filterPanel'); const open=p.classList.toggle('open'); const t=I18N[STATE.lang]||I18N.ru; document.getElementById('filterToggleBtn').textContent=open?t.hideFilter:t.showFilter;});
  document.getElementById('btnRegister').addEventListener('click',()=>openAuthForm('register')); document.getElementById('btnLogin').addEventListener('click',()=>openAuthForm('login')); document.getElementById('btnLogout').addEventListener('click',logoutUser); document.getElementById('btnAuthCancel').addEventListener('click',closeAuthForm); document.getElementById('btnAuthSubmit').addEventListener('click',async()=>{if(STATE.authMode==='register') await registerUser(); else await loginUser();}); document.getElementById('btnLoadUsers').addEventListener('click',loadUsersAdmin); document.getElementById('btnCollapseUsers').addEventListener('click',()=>{const t=I18N[STATE.lang]||I18N.ru; const box=document.getElementById('adminUsers'); const btn=document.getElementById('btnCollapseUsers'); const hidden=box.style.display==='none'; box.style.display=hidden?'':'none'; btn.textContent=hidden?t.collapseUsers:t.expandUsers;});
  document.querySelectorAll('th.sortable').forEach(th=>{th.addEventListener('click',()=>{const k=th.getAttribute('data-sort'); if(STATE.sortKey===k){STATE.sortDir=STATE.sortDir==='asc'?'desc':'asc';}else{STATE.sortKey=k;STATE.sortDir='desc';} render();});});
}

function parsePctInput(v){const t=String(v||'').replace(/%/g,'').replace(',','.').trim(); if(!t)return 0; const n=parseFloat(t); return Number.isFinite(n)?(n/100):0;}

async function boot(){
  bindUiEvents();
  // Pre-warm RSA key import in background so login/register won't pause on first click
  _importPubKey().catch(err=>console.debug('[auth] RSA key pre-warm failed (will retry on login):',err));

  // ── Phase 1: instant first render using server-injected snapshot ──────────
  // The server embeds current LIVE_ROWS + CFG into the HTML as JSON elements.
  // This means the table renders on first paint — zero API round-trips needed.
  const serverConfig=_readJsonEl('__initial-config__');
  const serverData=_readJsonEl('__initial-data__');
  STATE.config=serverConfig||{refresh_sec:1,min_vol:0,min_spread:0,enabled:{MEXC:true,Bybit:true,BingX:true}};
  STATE.data=serverData||null;

  document.getElementById('minVol').value=localStorage.getItem('minVolInput')||String(STATE.config.min_vol||0);
  document.getElementById('minSpread').value=String((STATE.config.min_spread||0)*100)+'%';
  document.getElementById('soundToggle').checked=STATE.sound;
  applyTheme(); applyLang(); renderExchangeFilters();
  if(STATE.data)render(); // First paint — table visible immediately

  // ── Phase 2: parallel fetch of auth info + fresh data + assets ───────────
  // All three calls fire simultaneously; none blocks the others.
  const [_me,_data,_assets]=await Promise.allSettled([
    loadMe(),                  // sets STATE.user; clears token if expired
    apiGet('/api/data'),       // get fresh data with auth token (full rows for logged-in users)
    apiGet('/api/assets'),     // logos + sounds
  ]);
  // Accept real data (even empty rows); skip 202 loading placeholders — SSE/polling will
  // update STATE.data once the server has real data ready (even if rows are empty).
  if(_data.status==='fulfilled'&&_data.value&&!(_data.value.dbg&&_data.value.dbg.loading))STATE.data=_data.value;
  if(_assets.status==='fulfilled'&&_assets.value)STATE.assets=_assets.value||{logos:{},sounds:[]};

  const ss=document.getElementById('soundSel'); ss.innerHTML='';
  (STATE.assets.sounds||[]).forEach(n=>{const o=document.createElement('option'); o.value=n; o.textContent=n; ss.appendChild(o);});
  if((STATE.assets.sounds||[]).includes(STATE.soundFile)){ss.value=STATE.soundFile;}
  else if((STATE.assets.sounds||[]).length){STATE.soundFile=STATE.assets.sounds[0]; ss.value=STATE.soundFile; localStorage.setItem('soundFile',STATE.soundFile);}

  renderAuth(); renderExchangeFilters(); render();

  let _sseActive=false;
  let _refreshInFlight=false;
  const _sseEl=document.getElementById('sseIndicator');
  function _sseSetStatus(s){if(_sseEl)_sseEl.textContent=s;}
  async function safeRefresh(){
    if(_refreshInFlight)return;
    _refreshInFlight=true;
    try{await refreshData();}catch(_e){}finally{_refreshInFlight=false;}
  }
  function connectSSE(){
    if(typeof EventSource==='undefined')return;
    _sseSetStatus('🔄 Connecting...');
    const src=new EventSource('/events');
    src.onopen=()=>{_sseActive=true;_sseSetStatus('🟢 Live');safeRefresh();};
    src.onmessage=e=>{try{const m=JSON.parse(e.data);if(m.t==='upd')safeRefresh();}catch(_e){}};
    src.onerror=()=>{_sseActive=false;_sseSetStatus('🔴 Reconnecting...');src.close();setTimeout(connectSSE,2000);};
  }
  connectSSE();
  // Poll /api/data every 5s as a safety net regardless of SSE state.
  // When SSE is active and working, ETags (304) prevent unnecessary re-renders.
  // When SSE is connected but the server isn't pushing "upd" events (e.g. COLLECTOR_ONLY
  // mode, collector process restarting), this ensures the UI recovers automatically.
  setInterval(()=>safeRefresh(),5000);
}
boot();