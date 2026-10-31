/* 心灵感应 · 两人默契大考验
   实时通道：公共 MQTT over WebSocket（免注册、免后端） */
(function () {
  'use strict';

  /* ============ 配置 ============ */
  var BROKERS = [
    { id: 'emqx', name: 'EMQX · 通道 A（推荐）', url: 'wss://broker.emqx.io:8084/mqtt' },
    { id: 'hivemq', name: 'HiveMQ · 通道 B', url: 'wss://broker.hivemq.com:8884/mqtt' },
    { id: 'emqx-cn', name: 'EMQX · 通道 C', url: 'wss://broker-cn.emqx.io:8084/mqtt' },
    { id: 'custom', name: '自定义地址', url: '' }
  ];

  var NS = 'mochi/v1';
  var MATCH_TIME = 60;    // 房间会合时限
  var ANSWER_TIME = 60;   // 答题时限
  var GRACE_TIME = 3;     // 等待对方答案的宽限
  var JUDGE_TIME = 20;    // 判定时限
  var ADV_TTL = 150000;   // 招领信息有效期
  var STORE_KEY = 'mochi_archive_v1';
  var PREF_KEY = 'mochi_pref_v1';

  /* ============ 状态 ============ */
  var S = {
    view: 'home', phase: 'idle',
    nick: '', code: '', targetRounds: 0,
    client: null, brokerIdx: 0, customUrl: '', connected: false, trying: false,
    myId: rid(),
    role: null, token: null, peer: null,
    advTs: 0, advId: '',
    round: 0, myWord: null, peerWord: null, myChoice: null, peerChoice: null,
    roundResult: null, ready: null, grace: false,
    log: [], matchId: null, hitCache: 0,
    deadline: 0, timerTotal: 0, tickTimer: null, pingTimer: null, lastSeen: 0,
    ended: false, lastMatch: null
  };

  /* ============ 工具 ============ */
  function $(s) { return document.querySelector(s); }
  function rid() { return Math.random().toString(36).slice(2, 10); }
  function now() { return Date.now(); }
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) { return ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]; }); }
  function norm(s) {
    return String(s || '').trim().replace(/\s+/g, '')
      .replace(/[，。！？、；：,.!?;:~～'"“”‘’·]/g, '').toLowerCase();
  }
  var toastTimer = null;
  function toast(msg, ms) {
    var t = $('#toast'); t.textContent = msg; t.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { t.classList.remove('show'); }, ms || 2200);
  }
  function fmtTime(ts) {
    var d = new Date(ts), p = function (n) { return n < 10 ? '0' + n : '' + n; };
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + ' ' + p(d.getHours()) + ':' + p(d.getMinutes());
  }

  /* ============ 视图 ============ */
  function showView(v) {
    S.view = v;
    ['home', 'match', 'game', 'summary', 'history'].forEach(function (n) {
      $('#view-' + n).classList.toggle('active', n === v);
    });
    window.scrollTo(0, 0);
  }
  function showStage(id) {
    ['answer', 'wait', 'reveal', 'judge', 'result'].forEach(function (n) {
      $('#stage-' + n).classList.toggle('active', n === id);
    });
  }

  /* ============ 存档 ============ */
  function loadStore() {
    try { return JSON.parse(localStorage.getItem(STORE_KEY)) || { matches: [] }; }
    catch (e) { return { matches: [] }; }
  }
  function saveStore(st) {
    try { localStorage.setItem(STORE_KEY, JSON.stringify(st)); } catch (e) { }
  }
  function upsertMatch(finished) {
    var st = loadStore(), m = null;
    for (var i = 0; i < st.matches.length; i++) { if (st.matches[i].id === S.matchId) { m = st.matches[i]; break; } }
    if (!m) {
      m = { id: S.matchId, ts: now(), code: S.code, players: [S.nick, S.peer ? S.peer.nick : '对方'], rounds: [] };
      st.matches.unshift(m);
    }
    m.players = [S.nick, S.peer ? S.peer.nick : '对方'];
    m.rounds = S.log.slice();
    m.finished = !!finished;
    saveStore(st);
    S.lastMatch = m;
    return m;
  }
  function loadPref() {
    try { return JSON.parse(localStorage.getItem(PREF_KEY)) || {}; } catch (e) { return {}; }
  }
  function savePref(p) {
    try { localStorage.setItem(PREF_KEY, JSON.stringify(p)); } catch (e) { }
  }

  /* ============ 实时通道 ============ */
  function brokerUrl(idx) {
    if (BROKERS[idx] && BROKERS[idx].id === 'custom') return S.customUrl;
    return BROKERS[idx] ? BROKERS[idx].url : BROKERS[0].url;
  }
  function setConn(state, text) {
    var d = $('#dot-home'), t = $('#conn-home-text');
    d.className = 'dot' + (state === 'on' ? ' on' : state === 'bad' ? ' bad' : ' wait');
    t.textContent = text;
    var g = $('#dot-game'); if (g) g.className = 'dot' + (state === 'on' ? ' online' : state === 'bad' ? ' bad' : ' wait');
  }

  function ensureMqtt(cb) {
    if (window.mqtt) return cb();
    var s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/mqtt@5.10.1/dist/mqtt.min.js';
    s.onload = cb;
    s.onerror = function () { setConn('bad', '通信组件加载失败'); toast('实时通信组件加载失败，请检查网络'); };
    document.head.appendChild(s);
  }

  function connect(idx) {
    ensureMqtt(function () {
      if (!window.mqtt) return;
      S.brokerIdx = idx;
      var url = brokerUrl(idx);
      if (!url) { setConn('bad', '未填自定义地址'); return; }
      if (S.client) { try { S.client.end(true); } catch (e) { } S.client = null; }
      S.connected = false;
      setConn('wait', '连接中…');
      var c;
      try {
        c = mqtt.connect(url, {
          clientId: 'mochi_' + S.myId + '_' + rid(),
          clean: true, keepalive: 30, connectTimeout: 10000, reconnectPeriod: 3000
        });
      } catch (e) { onConnFail(); return; }
      S.client = c;

      var settled = false;
      c.on('connect', function () {
        settled = true; S.connected = true;
        setConn('on', '已连接 · ' + (BROKERS[idx] ? BROKERS[idx].name.replace(/ ·.*/, '') : url));
        if (S.phase === 'match') subscribeLobby();
        if (S.token) sub(S.topicRoom());
      });
      c.on('error', function (e) { if (!settled) onConnFail(); else setConn('bad', '连接异常'); });
      c.on('close', function () { S.connected = false; if (S.phase !== 'idle') setConn('wait', '重连中…'); });
      c.on('message', onMessage);

      setTimeout(function () { if (!settled) onConnFail(); }, 11000);

      function onConnFail() {
        if (S.connected) return;
        var next = idx + 1;
        if (next < BROKERS.length && BROKERS[next].id !== 'custom') {
          setConn('wait', '换用通道 ' + BROKERS[next].name.replace(/ ·.*/, '') + '…');
          toast('通道 A 不通，自动切换', 1500);
          connect(next);
        } else {
          setConn('bad', '连接失败，请检查网络或换通道');
        }
      }
    });
  }

  function sub(topic, qos) { if (S.client && S.connected) S.client.subscribe(topic, { qos: qos || 1 }); }
  function unsub(topic) { if (S.client) try { S.client.unsubscribe(topic); } catch (e) { } }
  function pub(topic, obj, retain) {
    if (!S.client || !S.connected) return;
    S.client.publish(topic, typeof obj === 'string' ? obj : JSON.stringify(obj), { qos: 1, retain: !!retain });
  }

  /* topic helpers */
  S.topicHost = function () { return NS + '/l/' + S.code + '/host'; };
  S.topicJoin = function () { return NS + '/l/' + S.code + '/join'; };
  S.topicRoom = function () { return NS + '/r/' + S.token; };
  function roomPub(obj) { obj.f = S.myId; pub(S.topicRoom(), obj); }

  function onMessage(topic, payload) {
    var txt;
    try { txt = typeof payload === 'string' ? payload : new TextDecoder().decode(payload); }
    catch (e) { return; }
    if (!txt) return;
    var m; try { m = JSON.parse(txt); } catch (e) { return; }
    if (!m || m.f === S.myId) return;

    if (topic.indexOf('/l/') > -1) { onLobby(topic, m); return; }
    if (S.token && topic === S.topicRoom()) { S.lastSeen = now(); onRoom(m); }
  }

  /* ============ 房间会合 ============ */
  function subscribeLobby() {
    sub(S.topicHost(), 1);
    setTimeout(function () { if (S.phase === 'match' && !S.role) becomeHost(); }, 1000);
  }

  function becomeHost() {
    S.role = 'host';
    S.token = rid() + rid();
    S.advTs = now(); S.advId = S.myId;
    pub(S.topicHost(), { f: S.myId, t: 'adv', token: S.token, hostId: S.myId, nick: S.nick, ts: S.advTs }, true);
    sub(S.topicJoin(), 1);
    setMatchStatus('已开好房间，等待对方输入同样的房间码…');
  }

  function becomeGuest(adv) {
    S.role = 'guest';
    S.token = adv.token;
    sub(S.topicRoom(), 1);
    setMatchStatus('找到房间，正在加入…');
    var n = 0;
    var iv = setInterval(function () {
      if (S.phase !== 'match' || S.role !== 'guest') { clearInterval(iv); return; }
      pub(S.topicJoin(), { f: S.myId, t: 'join', id: S.myId, nick: S.nick });
      if (++n >= 5) clearInterval(iv);
    }, 700);
    pub(S.topicJoin(), { f: S.myId, t: 'join', id: S.myId, nick: S.nick });
  }

  function onLobby(topic, m) {
    if (S.phase !== 'match') return;
    if (topic === S.topicHost()) {
      if (m.t !== 'adv' || !m.token) return;               // 被清除的 retained
      if (m.hostId === S.myId) return;
      if (now() - m.ts > ADV_TTL) return;                  // 陈旧招领
      if (S.role === 'host') {
        // 双方同时开房：时间戳早的一方胜出，另一方降级为访客
        if (m.ts < S.advTs || (m.ts === S.advTs && String(m.hostId) < String(S.myId))) {
          unsub(S.topicJoin());
          becomeGuest(m);
        }
        return;
      }
      becomeGuest(m);
    } else if (topic === S.topicJoin()) {
      if (S.role !== 'host' || m.t !== 'join' || S.peer) return;
      S.peer = { id: m.id, nick: m.nick };
      sub(S.topicRoom(), 1);
      pub(S.topicHost(), '', true);                        // 撤下招领
      unsub(S.topicJoin());
      roomPub({ t: 'start', token: S.token, rounds: S.targetRounds, players: [{ id: S.myId, nick: S.nick }, { id: m.id, nick: m.nick }] });
      enterGame(m.id, m.nick, S.targetRounds);
    }
  }

  function onStart(m) {
    var me = null, peer = null;
    (m.players || []).forEach(function (p) { if (p.id === S.myId) me = p; else peer = p; });
    if (!peer) return;
    enterGame(peer.id, peer.nick, m.rounds || 0);
  }

  /* ============ 开始对局 ============ */
  function enterGame(peerId, peerNick, target) {
    if (S.phase === 'game') return;
    S.peer = { id: peerId, nick: peerNick };
    S.targetRounds = target || 0;
    S.matchId = 'm_' + now() + '_' + rid();
    S.log = []; S.ended = false; S.lastSeen = now();
    S.phase = 'game';
    stopTimer();
    showView('game');
    $('#g-peer').textContent = peerNick;
    $('#g-round-total').textContent = S.targetRounds ? '/ ' + S.targetRounds : '';
    upsertMatch(false);
    startPing();
    enterRound(1);
  }

  function startPing() {
    clearInterval(S.pingTimer);
    S.pingTimer = setInterval(function () {
      if (S.phase === 'game') { roomPub({ t: 'ping' }); }
      if (S.phase === 'game' && S.lastSeen && now() - S.lastSeen > 16000) {
        toast('对方似乎离开了，本局结束');
        goSummary(false);
      }
    }, 5000);
  }

  function enterRound(n) {
    S.round = n;
    S.myWord = null; S.peerWord = null;
    S.myChoice = null; S.peerChoice = null;
    S.roundResult = null; S.ready = { me: false, peer: false };
    S.grace = false;
    S.phase = 'game';
    $('#g-round').textContent = n;
    $('#in-word').value = '';
    $('#inspire-text').textContent = '';
    setStage('answer');
    updateScore();
    renderLog();
    setTimer(ANSWER_TIME);
    $('#peer-state-answer').textContent = '对方正在思考…';
    $('#peer-state-answer').className = 'peer-state';
  }

  function setStage(name) {
    S.stageName = name;
    showStage(name);
    if (name === 'answer') setTimeout(function () { $('#in-word').focus(); }, 60);
  }

  function updateScore() {
    var hit = 0;
    S.log.forEach(function (r) { if (r.ok) hit++; });
    $('#g-hit').textContent = hit;
    $('#g-total').textContent = S.log.length;
  }

  /* ============ 计时 ============ */
  function setTimer(sec) {
    S.deadline = now() + sec * 1000;
    S.timerTotal = sec * 1000;
    stopTicker();
    S.tickTimer = setInterval(tick, 250);
    tick();
  }
  function stopTicker() { clearInterval(S.tickTimer); S.tickTimer = null; }
  function stopTimer() { stopTicker(); S.deadline = 0; $('#g-timer').style.width = '100%'; $('#g-timer-num').textContent = ''; }

  function tick() {
    if (!S.deadline) return;
    var left = Math.max(0, S.deadline - now());
    var pct = S.timerTotal ? (left / S.timerTotal) * 100 : 100;
    var fill = $('#g-timer'), num = $('#g-timer-num');
    fill.style.width = pct + '%';
    fill.classList.toggle('urgent', left <= 10000);
    num.textContent = S.stageName === 'judge' ? '决定时间 ' + Math.ceil(left / 1000) + 's'
      : S.stageName === 'answer' || S.stageName === 'wait' ? '剩余 ' + Math.ceil(left / 1000) + 's' : '';
    num.classList.toggle('urgent', left <= 10000);
    if (left <= 0) onDeadline();
  }

  function onDeadline() {
    if (S.stageName === 'answer' || S.stageName === 'wait') {
      if (S.myWord == null) { submitWord('', true); }
      if (S.peerWord == null) {
        if (!S.grace) { S.grace = true; setTimer(GRACE_TIME); $('#wait-tip').textContent = '等待对方…（宽限 3 秒）'; return; }
        S.peerWord = '';
      }
      stopTimer();
      doReveal();
      return;
    }
    if (S.stageName === 'judge') {
      stopTimer();
      if (S.myChoice == null) { S.myChoice = 'next'; roomPub({ t: 'judge', r: S.round, c: 'next' }); }
      S.peerChoice = S.peerChoice || 'next';
      finishRound({ ok: false, type: 'timeout' });
    }
  }

  /* ============ 答题 / 揭晓 ============ */
  function submitWord(w, timeout) {
    if (S.myWord != null) return;
    S.myWord = w;
    roomPub({ t: 'answer', r: S.round, w: w });
    if (!timeout) {
      $('#wait-word').textContent = w === '' ? '（空）' : w;
      setStage('wait');
      $('#wait-tip').textContent = S.peerWord != null ? '对方也答完了，马上揭晓…' : '等待对方作答…';
      if (S.peerWord != null) doReveal();
    }
  }

  function doReveal() {
    if (S.stageName === 'reveal' || S.stageName === 'judge' || S.stageName === 'result') return;
    if (S.myWord == null) S.myWord = '';
    if (S.peerWord == null) S.peerWord = '';
    stopTimer();
    S.phase = 'game';
    setStage('reveal');

    var a = S.myWord, b = S.peerWord;
    $('#wc-me-word').textContent = a || '未作答';
    $('#wc-me-word').className = 'wc-word' + (a ? '' : ' empty');
    $('#wc-peer-word').textContent = b || '未作答';
    $('#wc-peer-word').className = 'wc-word' + (b ? '' : ' empty');
    $('#wc-peer-owner').textContent = S.peer ? S.peer.nick : '对方';

    var na = norm(a), nb = norm(b);
    var bothEmpty = na === '' && nb === '';
    var same = na !== '' && na === nb;

    if (bothEmpty) {
      banner('两个人都没有作答，本题跳过', 'bad');
      setTimeout(function () { finishRound({ ok: false, type: 'none' }); }, 1500);
      return;
    }
    if (same) {
      banner('💞 完全一样！默契命中', 'good');
      setTimeout(function () { finishRound({ ok: true, type: 'exact' }); }, 1600);
      return;
    }
    banner('词不一样 —— 你们觉得意思一致吗？', '');
    setTimeout(enterJudge, 1700);
  }

  function banner(text, cls) {
    var b = $('#reveal-banner');
    b.textContent = text;
    b.className = 'reveal-banner' + (cls ? ' ' + cls : '');
  }

  function enterJudge() {
    if (S.stageName === 'judge' || S.stageName === 'result') return;
    setStage('judge');
    $('#jw-me').textContent = S.myWord || '未作答';
    $('#jw-peer').textContent = S.peerWord || '未作答';
    $('#jw-peer-name').textContent = S.peer ? S.peer.nick : '对方';
    $('#peer-state-judge').textContent = '等待对方决定…';
    $('#peer-state-judge').className = 'peer-state';
    setTimer(JUDGE_TIME);
    if (S.peerChoice != null) trySettleJudge();
  }

  function trySettleJudge() {
    if (S.myChoice == null || S.peerChoice == null) return;
    stopTimer();
    if (S.myChoice === 'agree' && S.peerChoice === 'agree') finishRound({ ok: true, type: 'agree' });
    else finishRound({ ok: false, type: 'miss' });
  }

  /* ============ 本轮结算 ============ */
  function finishRound(res) {
    if (S.stageName === 'result') return;
    stopTimer();
    S.roundResult = res;
    S.log.push({ n: S.round, a: S.myWord, b: S.peerWord, ok: !!res.ok, type: res.type });
    upsertMatch(false);
    updateScore();
    renderLog();
    setStage('result');

    var hit = !!res.ok;
    $('#res-emoji').textContent = hit ? (res.type === 'exact' ? '💞' : '🎉') : (res.type === 'timeout' ? '⏰' : '😅');
    $('#res-title').textContent = hit ? (res.type === 'exact' ? '完全一样！' : '默契命中！') : '这一题没对上';

    var va = S.myWord || '未作答', vb = S.peerWord || '未作答';
    $('#res-words').innerHTML =
      '<div class="rw"><i>你</i>' + esc(va) + '</div><div class="rw"><i>' + esc(S.peer ? S.peer.nick : '对方') + '</i>' + esc(vb) + '</div>';
    var meta = hit ? (res.type === 'exact' ? '两个词一模一样' : '两人都认可意思一致') :
      res.type === 'timeout' ? '没人拍板，本题作废' : res.type === 'none' ? '双方均未作答' : '有人点了「下一题」';
    $('#res-meta').textContent = meta;

    var last = S.targetRounds && S.round >= S.targetRounds;
    $('#btn-continue').textContent = last ? '看看战报' : '下一轮';
    $('#peer-state-result').textContent = '';
    $('#peer-state-result').className = 'peer-state';
  }

  function renderLog() {
    var ul = $('#rl-list');
    if (!S.log.length) { ul.innerHTML = '<li class="rl-empty">还没有记录，答完第一轮就有了。</li>'; return; }
    ul.innerHTML = S.log.slice().reverse().map(function (r) {
      return '<li><span class="rl-n">#' + r.n + '</span>' +
        '<span class="rl-w">' + esc(r.a || '—') + ' · ' + esc(r.b || '—') + '</span>' +
        '<span class="rl-tag ' + (r.ok ? 'hit' : 'miss') + '">' + (r.ok ? '命中' : '未中') + '</span></li>';
    }).join('');
  }

  function tryContinue() {
    if (!S.ready || !S.ready.me || !S.ready.peer) return;
    if (S.targetRounds && S.round >= S.targetRounds) { goSummary(false); return; }
    enterRound(S.round + 1);
  }

  /* ============ 房间消息 ============ */
  function onRoom(m) {
    if (m.t === 'start') { onStart(m); return; }
    if (S.stageName === undefined || S.phase !== 'game') return;

    if (m.t === 'ping') return;

    if (m.t === 'answer') {
      if (m.r !== S.round) return;
      S.peerWord = m.w;
      if (S.stageName === 'answer') {
        var el = $('#peer-state-answer');
        el.textContent = '对方已锁定答案 ✓';
        el.className = 'peer-state ok';
      } else if (S.stageName === 'wait') {
        $('#wait-tip').textContent = '对方也答完了，马上揭晓…';
        if (S.myWord != null) doReveal();
      }
      return;
    }

    if (m.t === 'judge') {
      if (m.r !== S.round) return;
      S.peerChoice = m.c;
      if (S.stageName === 'answer' || S.stageName === 'wait') return; // 对方抢先翻页，稍后同步
      if (S.stageName === 'judge') {
        if (m.c === 'next') {
          // 对方点了下一题 → 本题作废
          stopTimer();
          finishRound({ ok: false, type: 'miss' });
        } else {
          $('#peer-state-judge').textContent = '对方觉得意思一致 ✓';
          $('#peer-state-judge').className = 'peer-state ok';
          trySettleJudge();
        }
      }
      return;
    }

    if (m.t === 'cont') {
      if (m.r !== S.round) return;
      if (!S.ready) S.ready = { me: false, peer: false };
      S.ready.peer = true;
      if (S.stageName === 'result') {
        $('#peer-state-result').textContent = '对方已准备好下一轮 ✓';
        $('#peer-state-result').className = 'peer-state ok';
        tryContinue();
      }
      return;
    }

    if (m.t === 'end') { goSummary(true); return; }

    if (m.t === 'leave') { toast('对方离开了，本局结束'); goSummary(false); return; }
  }

  /* ============ 战报 ============ */
  function rate(m) { return m.total ? Math.round(m.hit / m.total * 100) : 0; }
  function hitCount(m) { return m.rounds.filter(function (r) { return r.ok; }).length; }

  function level(pct, total) {
    if (!total) return { b: '🫥', t: '没有完成的题目', c: '下一局认真一点？' };
    if (pct >= 90) return { b: '💞', t: '灵魂伴侣', c: '你们是不是共用同一个脑子？' };
    if (pct >= 70) return { b: '✨', t: '心有灵犀', c: '相当默契，继续保持！' };
    if (pct >= 50) return { b: '🤝', t: '默契搭子', c: '一半对得上，已经很不容易了。' };
    if (pct >= 30) return { b: '🌱', t: '还在磨合', c: '再多玩几局，频率会慢慢对齐。' };
    return { b: '🪐', t: '平行宇宙', c: '你们脑回路各走各的，但也挺有意思。' };
  }

  function goSummary(remote) {
    if (S.phase === 'summary') return;
    stopTimer();
    clearInterval(S.pingTimer);
    S.phase = 'summary';
    var m = upsertMatch(true);
    m.hit = hitCount(m); m.total = m.rounds.length;
    var st = loadStore();
    for (var i = 0; i < st.matches.length; i++) { if (st.matches[i].id === m.id) st.matches[i] = m; }
    saveStore(st);
    if (S.token && S.connected) { roomPub({ t: 'end' }); cleanupRoom(); }

    var pct = rate(m), lv = level(pct, m.total);
    $('#sum-badge').textContent = lv.b;
    $('#sum-title').textContent = lv.t;
    $('#sum-rate').textContent = pct;
    $('#sum-comment').textContent = lv.c;
    $('#sum-meta').textContent = fmtTime(m.ts) + ' · 房间 ' + m.code + ' · ' + esc(m.players[0]) + ' × ' + esc(m.players[1]) + ' · 共 ' + m.total + ' 题';
    $('#sum-rounds').innerHTML = m.rounds.length ? m.rounds.map(function (r) {
      return '<div class="sr"><span class="sr-n">#' + r.n + '</span>' +
        '<span class="sr-w">' + esc(r.a || '未作答') + ' / ' + esc(r.b || '未作答') + '</span>' +
        '<span class="sr-t ' + (r.ok ? 'hit' : 'miss') + '">' + (r.ok ? (r.type === 'exact' ? '完全一样' : '意思一致') : '未中') + '</span></div>';
    }).join('') : '<div class="sr"><span class="sr-w">本局没有完成的题目</span></div>';
    showView('summary');
  }

  function cleanupRoom() {
    if (S.token) unsub(S.topicRoom());
    if (S.code) { unsub(S.topicHost()); unsub(S.topicJoin()); }
    S.token = null; S.role = null; S.peer = null;
  }

  function reportText(m) {
    var hit = hitCount(m), pct = m.total ? Math.round(hit / m.total * 100) : 0, lv = level(pct, m.total);
    var L = [];
    L.push('【心灵感应 · 默契战报】');
    L.push(fmtTime(m.ts) + ' · 房间 ' + m.code);
    L.push(m.players[0] + ' × ' + m.players[1]);
    L.push('默契度 ' + pct + '%（' + hit + '/' + m.total + '） · ' + lv.t + ' ' + lv.b);
    L.push('');
    m.rounds.forEach(function (r) {
      L.push(r.n + '. ' + (r.a || '未作答') + ' / ' + (r.b || '未作答') +
        (r.ok ? (r.type === 'exact' ? '  ✅ 完全一样' : '  ⭕ 意思一致') : '  ❌ 未中'));
    });
    return L.join('\n');
  }

  /* ============ 战报图片 ============ */
  function drawPoster(m) {
    var W = 750, rows = Math.min(m.rounds.length, 12);
    var H = 620 + rows * 52 + (m.rounds.length > 12 ? 40 : 0);
    var c = document.createElement('canvas'); c.width = W; c.height = H;
    var x = c.getContext('2d');
    var F = '"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif';

    var g = x.createLinearGradient(0, 0, W, H);
    g.addColorStop(0, '#fff2f7'); g.addColorStop(.5, '#f6f3ff'); g.addColorStop(1, '#eafaf4');
    x.fillStyle = g; x.fillRect(0, 0, W, H);
    x.fillStyle = 'rgba(255,92,138,.10)'; circle(x, -60, -40, 200);
    x.fillStyle = 'rgba(108,92,231,.10)'; circle(x, W + 70, 120, 230);
    x.fillStyle = 'rgba(0,184,148,.10)'; circle(x, 90, H + 60, 210);

    x.textAlign = 'center';
    x.fillStyle = '#6d6479'; x.font = '500 24px ' + F;
    x.fillText('心 灵 感 应', W / 2, 92);
    x.fillStyle = '#2d2438'; x.font = 'bold 52px ' + F;
    x.fillText('默契战报', W / 2, 158);

    x.font = '500 24px ' + F; x.fillStyle = '#6d6479';
    x.fillText(esc(m.players[0]) + '  ×  ' + esc(m.players[1]), W / 2, 212);
    x.font = '400 20px ' + F; x.fillStyle = '#a49db0';
    x.fillText(fmtTime(m.ts) + ' · 房间 ' + m.code, W / 2, 248);

    // 命中率卡
    var hit = hitCount(m), pct = m.total ? Math.round(hit / m.total * 100) : 0, lv = level(pct, m.total);
    rrect(x, 60, 290, W - 120, 210, 28); x.fillStyle = '#ffffff'; x.fill();
    x.fillStyle = '#2d2438'; x.font = 'bold 96px ' + F;
    x.fillText(pct + '%', W / 2 - 60, 400);
    x.font = '500 26px ' + F; x.fillStyle = '#a49db0';
    x.fillText('默契度', W / 2 + 92, 372);
    x.font = 'bold 30px ' + F; x.fillStyle = '#ff5c8a';
    x.fillText(hit + ' / ' + m.total + ' 题', W / 2 + 92, 414);
    x.font = 'bold 34px ' + F; x.fillStyle = '#6c5ce7';
    x.fillText(lv.b + ' ' + lv.t, W / 2, 468);

    // 明细
    var y = 552;
    x.textAlign = 'left';
    x.font = 'bold 24px ' + F; x.fillStyle = '#6d6479';
    x.fillText('逐题回顾', 60, y); y += 34;
    m.rounds.slice(0, 12).forEach(function (r, i) {
      var yy = y + i * 52;
      x.fillStyle = i % 2 ? 'rgba(255,255,255,.75)' : 'rgba(255,255,255,.45)';
      rrect(x, 60, yy, W - 120, 46, 14); x.fill();
      x.font = 'bold 22px ' + F; x.fillStyle = '#a49db0';
      x.fillText('#' + r.n, 78, yy + 31);
      x.font = '500 24px ' + F; x.fillStyle = '#2d2438';
      var txt = (r.a || '未作答') + '  ·  ' + (r.b || '未作答');
      if (x.measureText(txt).width > 380) txt = txt.slice(0, 20) + '…';
      x.fillText(txt, 140, yy + 31);
      x.textAlign = 'right';
      x.font = 'bold 22px ' + F;
      x.fillStyle = r.ok ? '#00b894' : '#b9b2c4';
      x.fillText(r.ok ? (r.type === 'exact' ? '完全一样 ✓' : '意思一致 ✓') : '未中', W - 80, yy + 31);
      x.textAlign = 'left';
    });
    if (m.rounds.length > 12) {
      x.textAlign = 'center'; x.font = '400 20px ' + F; x.fillStyle = '#a49db0';
      x.fillText('… 还有 ' + (m.rounds.length - 12) + ' 题', W / 2, y + rows * 52 + 30);
    }

    x.textAlign = 'center';
    x.font = '400 20px ' + F; x.fillStyle = '#c9c2d2';
    x.fillText('两台设备 · 同一个词 · 心灵感应', W / 2, H - 42);

    return c;

    function circle(ctx, cx, cy, r) { ctx.beginPath(); ctx.arc(cx, cy, r, 0, 7); ctx.fill(); }
    function rrect(ctx, px, py, w, h, r) {
      ctx.beginPath();
      ctx.moveTo(px + r, py); ctx.lineTo(px + w - r, py); ctx.quadraticCurveTo(px + w, py, px + w, py + r);
      ctx.lineTo(px + w, py + h - r); ctx.quadraticCurveTo(px + w, py + h, px + w - r, py + h);
      ctx.lineTo(px + r, py + h); ctx.quadraticCurveTo(px, py + h, px, py + h - r);
      ctx.lineTo(px, py + r); ctx.quadraticCurveTo(px, py, px + r, py); ctx.closePath();
    }
  }

  function downloadPoster(m) {
    var c = drawPoster(m);
    var name = '默契战报_' + (m.players[0] || '') + '_' + (m.players[1] || '') + '_' + new Date(m.ts).getTime() + '.png';
    if (c.toBlob) {
      c.toBlob(function (b) {
        var a = document.createElement('a');
        a.href = URL.createObjectURL(b); a.download = name;
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
        setTimeout(function () { URL.revokeObjectURL(a.href); }, 1500);
        toast('战报图片已生成');
      });
    } else {
      var a2 = document.createElement('a'); a2.href = c.toDataURL('image/png'); a2.download = name; a2.click();
    }
  }

  /* ============ 历史 ============ */
  function renderHistory() {
    var st = loadStore(), box = $('#hist-list');
    if (!st.matches.length) {
      box.innerHTML = '<p class="stage-tip" style="text-align:center;padding:24px 0">还没有存档，玩一局就有了。</p>';
      return;
    }
    box.innerHTML = st.matches.map(function (m, idx) {
      var hit = hitCount(m), pct = m.total ? Math.round(hit / m.total * 100) : 0;
      var rows = m.rounds.map(function (r) {
        return '<div class="sr"><span class="sr-n">#' + r.n + '</span>' +
          '<span class="sr-w">' + esc(r.a || '未作答') + ' / ' + esc(r.b || '未作答') + '</span>' +
          '<span class="sr-t ' + (r.ok ? 'hit' : 'miss') + '">' + (r.ok ? '命中' : '未中') + '</span></div>';
      }).join('') || '<div class="sr"><span class="sr-w">无记录</span></div>';
      return '<div class="hist-item" data-i="' + idx + '">' +
        '<div class="hist-top"><span class="hist-who">' + esc(m.players[0]) + ' × ' + esc(m.players[1]) + '</span>' +
        '<span class="hist-rate">' + pct + '%</span></div>' +
        '<div class="hist-sub">' + fmtTime(m.ts) + ' · 房间 ' + esc(m.code) + ' · ' + hit + '/' + m.total + ' 题</div>' +
        '<div class="hist-rounds">' + rows +
        '<div style="display:flex;gap:8px;margin-top:10px">' +
        '<button class="btn-ghost small" data-poster="' + idx + '" type="button">🖼 战报图片</button>' +
        '<button class="btn-ghost small" data-copy="' + idx + '" type="button">📋 复制文字</button></div></div>' +
        '</div>';
    }).join('');

    box.querySelectorAll('.hist-item').forEach(function (el) {
      el.addEventListener('click', function (e) {
        if (e.target.closest('button')) return;
        el.classList.toggle('open');
      });
    });
    box.querySelectorAll('[data-poster]').forEach(function (b) {
      b.addEventListener('click', function () { downloadPoster(st.matches[+b.dataset.poster]); });
    });
    box.querySelectorAll('[data-copy]').forEach(function (b) {
      b.addEventListener('click', function () { copyText(reportText(st.matches[+b.dataset.copy])); });
    });
  }

  function copyText(t) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(t).then(function () { toast('战报已复制，去粘贴给 TA 吧'); }, fallback);
    } else fallback();
    function fallback() {
      var ta = document.createElement('textarea');
      ta.value = t; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      try { document.execCommand('copy'); toast('战报已复制'); } catch (e) { toast('复制失败，请手动选择'); }
      document.body.removeChild(ta);
    }
  }

  /* ============ 匹配页 ============ */
  function setMatchStatus(t) { $('#match-status').textContent = t; }

  function beginMatch() {
    var nick = ($('#in-nick').value || '').trim();
    if (!nick) { toast('先给自己起个昵称'); $('#in-nick').focus(); return; }
    var code = getCode();
    if (code.length !== 4) { toast('请填满 4 位房间码'); return; }
    S.nick = nick; S.code = code;
    S.targetRounds = parseInt($('#in-rounds').value, 10) || 0;
    savePref({ nick: nick, broker: S.brokerIdx, custom: S.customUrl });

    S.phase = 'match'; S.role = null; S.peer = null; S.token = null;
    $('#match-code').textContent = code;
    $('#btn-copy-code').textContent = '复制房间码';
    showView('match');
    setMatchStatus('正在连接…');
    startMatchTimer();

    if (!S.client || !S.connected) connect(S.brokerIdx);
    else { subscribeLobby(); }
  }

  function startMatchTimer() {
    var left = MATCH_TIME;
    var ring = $('#ring-fg'), C = 2 * Math.PI * 52;
    ring.style.strokeDasharray = C;
    $('#match-count').textContent = left;
    clearInterval(S.matchTimer);
    S.matchTimer = setInterval(function () {
      if (S.phase !== 'match') { clearInterval(S.matchTimer); return; }
      left--;
      $('#match-count').textContent = Math.max(0, left);
      ring.style.strokeDashoffset = C * (1 - Math.max(0, left) / MATCH_TIME);
      if (left <= 0) {
        clearInterval(S.matchTimer);
        toast('一分钟到了，没等到人', 2600);
        setMatchStatus('超时了 —— 确认对方也输入了同样的房间码，再试一次。');
        setTimeout(function () { cancelMatch(true); }, 2200);
      }
    }, 1000);
  }

  function cancelMatch(silent) {
    clearInterval(S.matchTimer);
    if (S.code) {
      if (S.role === 'host') pub(S.topicHost(), '', true);
      unsub(S.topicHost()); unsub(S.topicJoin());
    }
    S.phase = 'idle'; S.role = null; S.token = null;
    if (!silent) toast('已取消');
    showView('home');
  }

  /* ============ 房间码输入 ============ */
  function getCode() {
    var v = '';
    document.querySelectorAll('#code-input input').forEach(function (i) { v += (i.value || '').trim(); });
    return v;
  }
  function setCode(v) {
    var ins = document.querySelectorAll('#code-input input');
    for (var i = 0; i < 4; i++) {
      ins[i].value = v[i] || '';
      ins[i].classList.toggle('filled', !!v[i]);
    }
  }

  /* ============ 绑定 ============ */
  function bind() {
    var ins = document.querySelectorAll('#code-input input');
    ins.forEach(function (inp, i) {
      inp.addEventListener('input', function () {
        var v = inp.value.replace(/\D/g, '');
        if (v.length > 1) {                    // 粘贴了一整串
          setCode(v.slice(0, 4));
          ins[Math.min(3, v.length)].focus();
          return;
        }
        inp.value = v;
        inp.classList.toggle('filled', !!v);
        if (v && i < 3) ins[i + 1].focus();
      });
      inp.addEventListener('keydown', function (e) {
        if (e.key === 'Backspace' && !inp.value && i > 0) ins[i - 1].focus();
        if (e.key === 'Enter') $('#btn-start').click();
      });
    });

    $('#btn-random-code').addEventListener('click', function () {
      setCode(String(Math.floor(1000 + Math.random() * 9000)));
    });

    $('#btn-start').addEventListener('click', beginMatch);
    $('#btn-cancel-match').addEventListener('click', function () { cancelMatch(false); });

    $('#btn-copy-code').addEventListener('click', function () {
      var t = '心灵感应房间码：' + S.code + '（一分钟内输入这个数字就能和我匹配）';
      copyText(t);
    });

    var INSPIRES = ['一种水果', '一种动物', '一种颜色', '一个城市', '一道菜', '一种运动', '一种天气',
      '一部电影', '一种职业', '一种乐器', '一种饮料', '一个身体部位', '一种交通工具', '一种花',
      '一种情绪', '一个数字', '一个节日', '一件家具', '一种甜点', '一种天气现象'];
    $('#btn-inspire').addEventListener('click', function () {
      $('#inspire-text').textContent = INSPIRES[Math.floor(Math.random() * INSPIRES.length)];
    });

    $('#btn-lock').addEventListener('click', function () {
      var w = ($('#in-word').value || '').trim();
      if (!w) { toast('写点什么再锁定吧'); return; }
      submitWord(w, false);
    });
    $('#in-word').addEventListener('keydown', function (e) { if (e.key === 'Enter') $('#btn-lock').click(); });

    $('#btn-agree').addEventListener('click', function () {
      if (S.myChoice) return;
      S.myChoice = 'agree';
      roomPub({ t: 'judge', r: S.round, c: 'agree' });
      $('#peer-state-judge').textContent = '已选择「匹配成功」，等对方确认…';
      $('#peer-state-judge').className = 'peer-state ok';
      trySettleJudge();
    });

    $('#btn-next-round').addEventListener('click', function () {
      if (S.myChoice) return;
      S.myChoice = 'next';
      roomPub({ t: 'judge', r: S.round, c: 'next' });
      stopTimer();
      finishRound({ ok: false, type: 'miss' });
    });

    $('#btn-continue').addEventListener('click', function () {
      if (!S.ready) S.ready = { me: false, peer: false };
      if (S.ready.me) return;
      S.ready.me = true;
      roomPub({ t: 'cont', r: S.round });
      $('#peer-state-result').textContent = '等待对方…';
      $('#peer-state-result').className = 'peer-state';
      tryContinue();
    });

    $('#btn-finish').addEventListener('click', function () {
      roomPub({ t: 'end' });
      goSummary(false);
    });

    $('#btn-poster').addEventListener('click', function () { if (S.lastMatch) downloadPoster(S.lastMatch); });
    $('#btn-copy-report').addEventListener('click', function () { if (S.lastMatch) copyText(reportText(S.lastMatch)); });
    $('#btn-again').addEventListener('click', function () { resetToHome(); toast('约好新的房间码，再来一局'); });
    $('#btn-to-history').addEventListener('click', function () { renderHistory(); showView('history'); });

    $('#btn-open-history').addEventListener('click', function () { renderHistory(); showView('history'); });
    $('#btn-back-home').addEventListener('click', function () { showView('home'); });
    $('#btn-export-json').addEventListener('click', function () {
      var st = loadStore();
      var blob = new Blob([JSON.stringify(st, null, 2)], { type: 'application/json' });
      var a = document.createElement('a');
      a.href = URL.createObjectURL(blob); a.download = '心灵感应存档.json';
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      toast('已导出存档');
    });
    $('#btn-clear-history').addEventListener('click', function () {
      if (!confirm('确定清空所有默契存档？此操作不可恢复。')) return;
      saveStore({ matches: [] }); renderHistory(); toast('存档已清空');
    });

    $('#btn-open-settings').addEventListener('click', function () { $('#modal-settings').classList.add('open'); });
    $('#btn-settings-close').addEventListener('click', function () { $('#modal-settings').classList.remove('open'); });
    $('#btn-settings-save').addEventListener('click', function () {
      var v = $('#sel-broker').value;
      S.customUrl = ($('#in-broker-custom').value || '').trim();
      savePref({ nick: S.nick, broker: +v, custom: S.customUrl });
      $('#modal-settings').classList.remove('open');
      connect(+v);
    });
    $('#btn-open-help').addEventListener('click', function () { $('#modal-help').classList.add('open'); });
    $('#btn-help-close').addEventListener('click', function () { $('#modal-help').classList.remove('open'); });
    [$('#modal-settings'), $('#modal-help')].forEach(function (m) {
      m.addEventListener('click', function (e) { if (e.target === m) m.classList.remove('open'); });
    });

    window.addEventListener('pagehide', function () {
      if (S.token && S.connected) roomPub({ t: 'leave' });
    });
  }

  function resetToHome() {
    stopTimer(); clearInterval(S.pingTimer); clearInterval(S.matchTimer);
    cleanupRoom();
    S.phase = 'idle'; S.round = 0; S.log = []; S.ended = false;
    $('#in-word').value = '';
    showView('home');
  }

  /* ============ 初始化 ============ */
  function init() {
    var sel = $('#sel-broker');
    sel.innerHTML = BROKERS.map(function (b, i) {
      return '<option value="' + i + '">' + b.name + '</option>';
    }).join('');

    var p = loadPref();
    if (p.nick) $('#in-nick').value = p.nick;
    if (p.custom) $('#in-broker-custom').value = p.custom;
    S.customUrl = p.custom || '';
    S.brokerIdx = (typeof p.broker === 'number' && p.broker < BROKERS.length) ? p.broker : 0;
    sel.value = S.brokerIdx;

    setCode(String(Math.floor(1000 + Math.random() * 9000)));
    bind();

    // 支持通过链接直接带上房间码：?c=1234
    var q = new URLSearchParams(location.search).get('c');
    if (q && /^\d{4}$/.test(q)) setCode(q);

    connect(S.brokerIdx);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
