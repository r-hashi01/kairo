// The run viewer's page (ADR 0088): one document, its script and style
// inline, nothing loaded from outside. What runs hold (inputs, outputs,
// errors) is put in the page as text only, never as markup.

export const PAGE = String.raw`<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>kairo runs</title>
<style>
:root {
	--bg: #f7f7f5; --panel: #ffffff; --ink: #1d1f23; --muted: #6b7078; --line: #e3e3df; --soft: #f0f0ec;
	--ok: #2f8f5b; --ok-soft: #d8efe2; --run: #2f6fd6; --run-soft: #dce8fb; --fail: #c8452f; --fail-soft: #f8ddd7;
	--warn: #b7791f; --warn-soft: #f7ead2; --wait: #7d5bc7; --wait-soft: #e9e1f8; --gap: #b9bcc2;
	--mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
	color-scheme: light;
}
@media (prefers-color-scheme: dark) {
	:root {
		--bg: #15171a; --panel: #1c1f23; --ink: #e7e8ea; --muted: #9aa0a8; --line: #2e3238; --soft: #24282d;
		--ok: #4cc283; --ok-soft: #1e3a2b; --run: #6aa0ff; --run-soft: #1d2d4a; --fail: #ff7a63; --fail-soft: #46231d;
		--warn: #e6a84a; --warn-soft: #3d2f18; --wait: #b296ff; --wait-soft: #2f2747; --gap: #5b6068;
		color-scheme: dark;
	}
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.5 system-ui, -apple-system, "Segoe UI", "Hiragino Sans", "Noto Sans JP", sans-serif; }
header { display: flex; align-items: center; gap: 12px; padding: 10px 16px; border-bottom: 1px solid var(--line); background: var(--panel); position: sticky; top: 0; z-index: 2; }
header h1 { font-size: 15px; margin: 0; font-weight: 650; letter-spacing: .02em; }
header .live { margin-left: auto; color: var(--muted); font-size: 12px; display: flex; align-items: center; gap: 6px; }
.dot { width: 8px; height: 8px; border-radius: 50%; background: var(--gap); display: inline-block; }
.dot.on { background: var(--ok); }
.layout { display: grid; grid-template-columns: 280px 1fr; min-height: calc(100vh - 45px); }
@media (max-width: 800px) { .layout { grid-template-columns: 1fr; } nav { max-height: 40vh; } }
nav { border-right: 1px solid var(--line); background: var(--panel); overflow: auto; }
nav .item { padding: 9px 14px; border-bottom: 1px solid var(--line); cursor: pointer; }
nav .item:hover { background: var(--soft); }
nav .item.sel { background: var(--run-soft); }
nav .item .name { font-weight: 600; display: flex; justify-content: space-between; gap: 8px; align-items: center; }
nav .item .sub { color: var(--muted); font-size: 12px; font-family: var(--mono); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
nav .empty { padding: 16px; color: var(--muted); }
main { padding: 16px 20px 40px; min-width: 0; }
.pill { font-size: 11px; padding: 1px 8px; border-radius: 999px; font-weight: 600; white-space: nowrap; }
.s-completed { background: var(--ok-soft); color: var(--ok); }
.s-running, .s-waiting { background: var(--run-soft); color: var(--run); }
.s-failed { background: var(--fail-soft); color: var(--fail); }
.s-blocked { background: var(--warn-soft); color: var(--warn); }
.s-cancelled { background: var(--soft); color: var(--muted); }
.head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 10px; margin-bottom: 4px; }
.head h2 { margin: 0; font-size: 20px; }
.meta { color: var(--muted); font-size: 12px; font-family: var(--mono); margin-bottom: 14px; word-break: break-all; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; margin-bottom: 14px; }
.tl { display: grid; grid-template-columns: minmax(150px, 240px) 1fr; }
.tl .lab { padding: 6px 8px 6px 0; border-bottom: 1px solid var(--line); display: flex; align-items: center; gap: 6px; min-width: 0; cursor: pointer; }
.tl .lab .nm { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.tl .lab .kd { font-size: 10px; color: var(--muted); border: 1px solid var(--line); border-radius: 4px; padding: 0 4px; flex: none; }
.tl .trk { position: relative; border-bottom: 1px solid var(--line); height: 34px; cursor: pointer; }
.tl .row-sel { background: var(--soft); }
.tl .axis { position: relative; height: 22px; border-bottom: 1px solid var(--line); color: var(--muted); font-size: 11px; }
.tick { position: absolute; top: 3px; transform: translateX(-50%); white-space: nowrap; }
.grid { position: absolute; top: 0; bottom: 0; width: 1px; background: var(--line); }
.seg { position: absolute; top: 9px; height: 16px; border-radius: 4px; }
.seg.ok { background: var(--ok-soft); border: 1px solid var(--ok); }
.seg.failed { background: var(--fail-soft); border: 1px solid var(--fail); }
.seg.running { background: var(--run-soft); border: 1px solid var(--run); background-image: repeating-linear-gradient(45deg, transparent 0 6px, rgba(127,127,127,.18) 6px 12px); }
.seg.waiting { background: var(--wait-soft); border: 1px solid var(--wait); }
.seg.blocked { background: var(--warn-soft); border: 1px solid var(--warn); }
.seg.cancelled { background: var(--soft); border: 1px solid var(--gap); }
.seg.life { top: 15px; height: 4px; border: none; background: var(--gap); border-radius: 2px; }
.seg.derived { border-left-style: dashed; }
.seg .at { position: absolute; left: 4px; top: -1px; font-size: 10px; font-weight: 700; color: var(--ink); opacity: .75; white-space: nowrap; }
.seg.past { border-right: none; border-top-right-radius: 0; border-bottom-right-radius: 0; }
.more { position: absolute; right: 4px; top: 8px; font-size: 11px; font-weight: 600; color: var(--muted); background: var(--panel); padding: 0 4px; border-radius: 4px; }
.gapline { position: absolute; top: 16px; height: 0; border-top: 2px dotted var(--gap); }
.mark { position: absolute; top: 12px; width: 10px; height: 10px; margin-left: -5px; border-radius: 50%; border: 2px solid var(--panel); background: var(--muted); }
.mark.ok { background: var(--ok); } .mark.failed { background: var(--fail); } .mark.dispatched { background: var(--run); border-radius: 2px; }
.mark.signal { background: var(--wait); } .mark.timer { background: var(--gap); } .mark.resolved { background: var(--warn); } .mark.cancelled { background: var(--muted); }
.cut { position: absolute; top: 5px; margin-left: -7px; font-size: 14px; font-weight: 800; color: var(--fail); line-height: 1; }
.legend { display: flex; flex-wrap: wrap; gap: 14px; color: var(--muted); font-size: 12px; margin-top: 10px; }
.legend span { display: inline-flex; align-items: center; gap: 6px; }
.legend i { display: inline-block; width: 18px; height: 10px; border-radius: 3px; }
.note { color: var(--muted); font-size: 12px; margin-top: 6px; }
h3 { font-size: 13px; margin: 0 0 8px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
td, th { text-align: left; padding: 4px 8px 4px 0; border-bottom: 1px solid var(--line); vertical-align: top; }
th { color: var(--muted); font-weight: 600; font-size: 12px; }
td.m { font-family: var(--mono); font-size: 12px; white-space: nowrap; }
pre { margin: 0; font: 12px/1.45 var(--mono); background: var(--soft); border-radius: 6px; padding: 8px 10px; max-height: 360px; overflow: auto; white-space: pre-wrap; word-break: break-word; }
.kv { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 900px) { .kv { grid-template-columns: 1fr; } }
.ops { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; }
.ops textarea { width: 100%; min-height: 64px; font: 12px var(--mono); border: 1px solid var(--line); border-radius: 6px; padding: 6px; background: var(--panel); color: var(--ink); }
.ops .box { flex: 1 1 280px; }
button { font: inherit; font-size: 13px; padding: 5px 12px; border-radius: 6px; border: 1px solid var(--line); background: var(--panel); color: var(--ink); cursor: pointer; }
button.primary { background: var(--run); border-color: var(--run); color: #fff; }
button.danger { color: var(--fail); border-color: var(--fail); }
button:disabled { opacity: .5; cursor: default; }
.msg { font-size: 12px; margin-top: 6px; }
.msg.err { color: var(--fail); }
.placeholder { color: var(--muted); padding: 40px 0; text-align: center; }
</style>
</head>
<body>
<header><h1>kairo</h1><span id="title" class="sub"></span><span class="live"><span id="dot" class="dot"></span><span id="live"></span></span></header>
<div class="layout">
<nav id="list"></nav>
<main id="main"><div class="placeholder" id="ph"></div></main>
</div>
<script>
(function () {
	'use strict';
	var JA = /^ja\b/.test(navigator.language || '');
	var T = JA ? {
		workflows: 'ワークフロー', none: 'まだワークフローがありません', pick: '左からワークフローを選んでください',
		live: '自動更新中', offline: '接続が切れています（再接続します）', notFound: 'この実行はありません（消えたか、ID が違います）',
		started: '開始', updated: '最終更新', id: 'ID', timeline: '経過', events: '記録されたイベント', input: '入力', output: '結果', error: 'エラー',
		kind: { workflow: 'ワークフロー', call: '呼び出し', wait: '待ち', run: '実行' },
		status: { running: '実行中', waiting: '待機中', completed: '完了', failed: '失敗', cancelled: '取り消し', blocked: '要確認' },
		ev: { started: '開始', dispatched: '試行を開始（意図を記録）', succeeded: '成功', failed: '失敗', waiting: '待機', timer: 'タイマー', signal: 'シグナル受信', recovered: '送り直し', resolved: '決着', cancelled: '取り消し', other: 'その他' },
		attempt: '試行', interrupted: '中断（実行していたプロセスが止まった）', unknown: '結果が不明', retryable: '再試行できる',
		lg: { ok: '成功した試行', failed: '失敗した試行', cut: 'プロセスが止まって中断', running: '実行中', waiting: '待っている', blocked: '要確認', gap: '再試行までの間', mark: '記録されたイベント' },
		derivedNote: '帯の始まりは、記録された直前のイベントの時刻から描いています（破線）。試行を始めた時刻そのものが記録されるのは、実の作用のステップ（■）だけです。時刻は、それぞれのイベントを記録したプロセスの時計です。',
		ops: '操作', signal: 'シグナルを送る', payload: '中身（JSON）', send: '送る', resolveOk: '成功として決着（結果の JSON）', resolveFail: '失敗として決着（メッセージ）',
		resolve: '決着', resolveFailed: '失敗として決着', cancel: 'このワークフローを取り消す', confirmCancel: '取り消しますか？', sent: '送りました', badJson: 'JSON として読めません',
		at: '時刻', what: 'できごと', detail: '詳細', since: '開始から'
	} : {
		workflows: 'Workflows', none: 'No workflows yet', pick: 'Pick a workflow on the left',
		live: 'Live', offline: 'Disconnected (reconnecting)', notFound: 'No such run (removed, or a wrong id)',
		started: 'Started', updated: 'Updated', id: 'ID', timeline: 'Progress', events: 'Recorded events', input: 'Input', output: 'Output', error: 'Error',
		kind: { workflow: 'workflow', call: 'call', wait: 'wait', run: 'run' },
		status: { running: 'running', waiting: 'waiting', completed: 'completed', failed: 'failed', cancelled: 'cancelled', blocked: 'needs review' },
		ev: { started: 'started', dispatched: 'attempt started (intent recorded)', succeeded: 'succeeded', failed: 'failed', waiting: 'waiting', timer: 'timer fired', signal: 'signal received', recovered: 'given out again', resolved: 'resolved', cancelled: 'cancelled', other: 'other' },
		attempt: 'attempt', interrupted: 'interrupted (the process running it stopped)', unknown: 'outcome unknown', retryable: 'retryable',
		lg: { ok: 'attempt succeeded', failed: 'attempt failed', cut: 'interrupted: process stopped', running: 'running', waiting: 'waiting', blocked: 'needs review', gap: 'until the retry', mark: 'recorded event' },
		derivedNote: 'A bar starts at the time of the event recorded before it (dashed). Only real steps (■) record when an attempt started. Times are by the clock of the process that recorded each event.',
		ops: 'Operations', signal: 'Send a signal', payload: 'Payload (JSON)', send: 'Send', resolveOk: 'Resolve as done (output JSON)', resolveFail: 'Resolve as failed (message)',
		resolve: 'Resolve', resolveFailed: 'Resolve as failed', cancel: 'Cancel this workflow', confirmCancel: 'Cancel it?', sent: 'Sent', badJson: 'Not valid JSON',
		at: 'Time', what: 'Event', detail: 'Detail', since: 'since start'
	};

	var KEY = new URLSearchParams(location.search).get('key') || '';
	var state = { list: [], root: null, data: null, sel: null, es: null, notFound: false };
	var DONE = { completed: 1, failed: 1, cancelled: 1 };

	function h(tag, attrs) {
		var el = document.createElement(tag);
		if (attrs) for (var k in attrs) {
			var v = attrs[k];
			if (v == null || v === false) continue;
			if (k === 'class') el.className = v;
			else if (k === 'text') el.textContent = v;
			else if (k === 'style') el.setAttribute('style', v);
			else if (k.slice(0, 2) === 'on') el.addEventListener(k.slice(2), v);
			else el.setAttribute(k, v);
		}
		for (var i = 2; i < arguments.length; i++) {
			var c = arguments[i];
			if (c == null || c === false) continue;
			if (Array.isArray(c)) c.forEach(function (x) { if (x) el.appendChild(typeof x === 'string' ? document.createTextNode(x) : x); });
			else el.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
		}
		return el;
	}
	function get(path) {
		return fetch(path, { headers: { 'x-kairo-key': KEY } }).then(function (r) {
			return r.json().then(function (b) { if (!r.ok) throw new Error(b.error || r.status); return b; });
		});
	}
	function post(path, body) {
		return fetch(path, { method: 'POST', headers: { 'x-kairo-key': KEY, 'content-type': 'application/json' }, body: JSON.stringify(body) }).then(function (r) {
			return r.json().then(function (b) { if (!r.ok) throw new Error(b.error || r.status); return b; });
		});
	}
	function pill(status) { return h('span', { class: 'pill s-' + status, text: T.status[status] || status }); }
	function clock(ms) { var d = new Date(ms); return d.toLocaleString(); }
	function hms(ms) { var d = new Date(ms); return d.toLocaleTimeString(); }
	function dur(ms) {
		if (ms < 1000) return Math.max(0, Math.round(ms)) + 'ms';
		var s = ms / 1000; if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + 's';
		var m = Math.floor(s / 60); if (m < 60) return m + 'm' + String(Math.round(s % 60)).padStart(2, '0') + 's';
		return Math.floor(m / 60) + 'h' + String(m % 60).padStart(2, '0') + 'm';
	}
	function json(v) { try { return JSON.stringify(v, null, 2); } catch (e) { return String(v); } }

	// The list of workflows, newest first; read again now and then while the page is open.
	function loadList() {
		get('/api/runs').then(function (rows) { state.list = rows; renderList(); }).catch(function () {});
	}
	function renderList() {
		var nav = document.getElementById('list');
		nav.replaceChildren();
		if (!state.list.length) return nav.appendChild(h('div', { class: 'empty', text: T.none }));
		state.list.forEach(function (r) {
			nav.appendChild(h('div', { class: 'item' + (r.run_id === state.root ? ' sel' : ''), onclick: function () { select(r.run_id); } },
				h('div', { class: 'name' }, h('span', { text: r.workflow || r.run_id }), pill(r.status)),
				h('div', { class: 'sub', text: clock(r.createdAt) + '  ' + r.run_id })));
		});
	}

	function select(id) {
		if (state.root === id) return;
		state.root = id; state.data = null; state.sel = null; state.notFound = false;
		history.replaceState(null, '', location.pathname + location.search + '#run=' + encodeURIComponent(id));
		renderList(); render();
		if (state.es) state.es.close();
		var es = new EventSource('/api/watch?id=' + encodeURIComponent(id) + '&key=' + encodeURIComponent(KEY));
		state.es = es;
		es.onopen = function () { live(true); };
		es.onerror = function () { live(false); };
		es.onmessage = function (m) {
			var d = JSON.parse(m.data);
			live(true);
			if (d.error) { state.data = null; state.notFound = true; } else { state.data = d; state.notFound = false; }
			render();
			var r = d.runs && d.runs[0];
			if (r) state.list.forEach(function (x) { if (x.run_id === r.run_id && x.status !== r.status) { x.status = r.status; renderList(); } });
		};
	}
	function live(on) {
		document.getElementById('dot').className = 'dot' + (on ? ' on' : '');
		document.getElementById('live').textContent = state.root ? (on ? T.live : T.offline) : '';
	}

	// Bars and marks of a run from its recorded events. A bar's start is the
	// event recorded before it (derived), but a real step's intent
	// ("dispatched"), which records when the attempt started.
	function shapes(run, now) {
		var out = { segs: [], marks: [], gaps: [], cuts: [] };
		var evs = run.events;
		if (run.kind === 'workflow') {
			var end = DONE[run.status] ? run.updatedAt : now;
			out.segs.push({ cls: 'life', from: run.createdAt, to: end });
			evs.forEach(function (e) { out.marks.push({ e: e }); });
			return out;
		}
		var from = null, recorded = false, lastEnd = null, attempt = 1;
		evs.forEach(function (e) {
			out.marks.push({ e: e });
			switch (e.kind) {
			case 'started': from = e.at; recorded = false; break;
			case 'dispatched': from = e.at; recorded = true; attempt = e.attempt || attempt; break;
			case 'timer': case 'recovered':
				if (from === null) { if (lastEnd !== null) out.gaps.push({ from: lastEnd, to: e.at }); from = e.at; recorded = false; }
				break;
			case 'failed':
				var cut = e.errorType === 'process_lost';
				out.segs.push({ cls: (!e.retryable && e.unknown) ? 'blocked' : 'failed', from: from === null ? e.at : from, to: e.at, derived: !recorded, attempt: e.attempt });
				if (cut) out.cuts.push({ at: e.at, e: e });
				from = null; lastEnd = e.at; attempt = (e.attempt || attempt) + 1; break;
			case 'succeeded':
				out.segs.push({ cls: 'ok', from: from === null ? e.at : from, to: e.at, derived: !recorded, attempt: e.attempt });
				from = null; lastEnd = e.at; break;
			case 'signal':
				out.segs.push({ cls: 'waiting', from: from === null ? run.createdAt : from, to: e.at, derived: false });
				from = null; lastEnd = e.at; break;
			case 'resolved': case 'cancelled':
				if (from !== null) out.segs.push({ cls: 'cancelled', from: from, to: e.at, derived: !recorded });
				from = null; lastEnd = e.at; break;
			}
		});
		if (!DONE[run.status]) {
			if (run.status === 'blocked') { /* the failed bar shows it */ }
			else if (from !== null) out.segs.push({ cls: run.kind === 'wait' ? 'waiting' : 'running', from: from, to: now, derived: !recorded && run.kind !== 'wait', attempt: run.kind === 'wait' ? null : attempt });
			else if (lastEnd !== null) out.gaps.push({ from: lastEnd, to: now });
		}
		return out;
	}

	function evText(e) {
		var s = T.ev[e.kind] || e.kind;
		if (e.attempt) s += ' (' + T.attempt + ' ' + e.attempt + ')';
		if (e.signal) s += ': ' + e.signal;
		if (e.errorType === 'process_lost') s += ' — ' + T.interrupted;
		else if (e.unknown) s += ' — ' + T.unknown;
		return s;
	}

	function depthOf(runs) {
		var parent = {}, depth = {};
		runs.forEach(function (r) { parent[r.run_id] = r.parent; });
		runs.forEach(function (r) {
			var d = 0, p = r.parent, seen = 0;
			while (p && parent.hasOwnProperty(p) && seen++ < 50) { d++; p = parent[p]; }
			depth[r.run_id] = d;
		});
		return depth;
	}
	// Each run right under its parent (creation order among siblings).
	function ordered(runs) {
		var kids = {}, out = [];
		runs.forEach(function (r, i) { if (i === 0) return; (kids[r.parent] = kids[r.parent] || []).push(r); });
		(function walk(r) { out.push(r); (kids[r.run_id] || []).forEach(walk); })(runs[0]);
		return out;
	}

	function render() {
		var main = document.getElementById('main');
		main.replaceChildren();
		if (!state.root) return main.appendChild(h('div', { class: 'placeholder', text: T.pick }));
		if (state.notFound) return main.appendChild(h('div', { class: 'placeholder', text: T.notFound }));
		if (!state.data) return;
		var runs = ordered(state.data.runs), root = runs[0], depth = depthOf(runs);
		var now = Date.now();
		var t0 = root.createdAt, t1 = t0, open = false;
		runs.forEach(function (r) {
			t0 = Math.min(t0, r.createdAt);
			t1 = Math.max(t1, r.updatedAt);
			r.events.forEach(function (e) { t0 = Math.min(t0, e.at); t1 = Math.max(t1, e.at); });
			if (!DONE[r.status]) open = true;
		});
		// The axis ends at the last thing recorded; what still goes on runs to
		// now when that is near, and past the edge (with how long) when it is not.
		if (open) t1 = now - t1 <= Math.max(5000, (t1 - t0) * 0.5) ? now : t1 + Math.max(1000, (t1 - t0) * 0.08);
		if (t1 - t0 < 1000) t1 = t0 + 1000;
		var span = t1 - t0;
		var x = function (t) { return ((Math.min(Math.max(t, t0), t1) - t0) / span * 100) + '%'; };

		main.appendChild(h('div', { class: 'head' }, h('h2', { text: root.name || root.run_id }), pill(root.status)));
		main.appendChild(h('div', { class: 'meta', text: T.id + ' ' + root.run_id + '   ' + T.started + ' ' + clock(root.createdAt) + '   ' + T.updated + ' ' + clock(root.updatedAt) }));

		// The timeline.
		var tl = h('div', { class: 'tl' });
		var axis = h('div', { class: 'axis' });
		var step = niceStep(span / 6);
		for (var t = 0; t <= span; t += step) {
			axis.appendChild(h('div', { class: 'tick', style: 'left:' + x(t0 + t), text: '+' + dur(t) }));
		}
		tl.appendChild(h('div', { class: 'axis' }));
		tl.appendChild(axis);
		runs.forEach(function (r) {
			var sel = state.sel === r.run_id;
			var pick = function () { state.sel = sel ? null : r.run_id; render(); };
			tl.appendChild(h('div', { class: 'lab' + (sel ? ' row-sel' : ''), style: 'padding-left:' + (depth[r.run_id] * 14) + 'px', onclick: pick, title: r.run_id },
				h('span', { class: 'kd', text: T.kind[r.kind] || r.kind }), h('span', { class: 'nm', text: r.name || r.run_id }), pill(r.status)));
			var trk = h('div', { class: 'trk' + (sel ? ' row-sel' : ''), onclick: pick });
			for (var g = step; g < span; g += step) trk.appendChild(h('div', { class: 'grid', style: 'left:' + x(t0 + g) }));
			var s = shapes(r, now);
			s.gaps.forEach(function (gp) { trk.appendChild(h('div', { class: 'gapline', style: 'left:' + x(gp.from) + ';width:calc(' + x(gp.to) + ' - ' + x(gp.from) + ')' })); });
			s.segs.forEach(function (sg) {
				var w = 'max(3px, calc(' + x(sg.to) + ' - ' + x(sg.from) + '))';
				var tip = (sg.attempt ? T.attempt + ' ' + sg.attempt + ': ' : '') + hms(sg.from) + ' – ' + (sg.to === now ? '…' : hms(sg.to)) + ' (' + dur(sg.to - sg.from) + ')';
				var past = sg.to > t1;
				trk.appendChild(h('div', { class: 'seg ' + sg.cls + (sg.derived ? ' derived' : '') + (past ? ' past' : ''), style: 'left:' + x(sg.from) + ';width:' + w, title: tip },
					sg.attempt && sg.cls !== 'life' ? h('span', { class: 'at', text: '#' + sg.attempt }) : null));
				if (past && sg.cls !== 'life') trk.appendChild(h('div', { class: 'more', text: '→ ' + dur(sg.to - sg.from) }));
			});
			s.marks.forEach(function (m) {
				var e = m.e, cls = { succeeded: 'ok', failed: 'failed', dispatched: 'dispatched', signal: 'signal', timer: 'timer', resolved: 'resolved', cancelled: 'cancelled' }[e.kind] || '';
				trk.appendChild(h('div', { class: 'mark ' + cls, style: 'left:' + x(e.at), title: hms(e.at) + '  ' + evText(e) + (e.error ? '\n' + e.error : '') }));
			});
			s.cuts.forEach(function (c) { trk.appendChild(h('div', { class: 'cut', style: 'left:' + x(c.at), title: T.interrupted, text: '✕' })); });
			tl.appendChild(trk);
		});
		var legend = h('div', { class: 'legend' },
			h('span', null, h('i', { style: 'background:var(--ok-soft);border:1px solid var(--ok)' }), T.lg.ok),
			h('span', null, h('i', { style: 'background:var(--fail-soft);border:1px solid var(--fail)' }), T.lg.failed),
			h('span', null, h('b', { style: 'color:var(--fail)', text: '✕' }), T.lg.cut),
			h('span', null, h('i', { class: 'seg running', style: 'position:static' }), T.lg.running),
			h('span', null, h('i', { style: 'background:var(--wait-soft);border:1px solid var(--wait)' }), T.lg.waiting),
			h('span', null, h('i', { style: 'background:var(--warn-soft);border:1px solid var(--warn)' }), T.lg.blocked),
			h('span', null, h('i', { style: 'border-top:2px dotted var(--gap);height:0' }), T.lg.gap),
			h('span', null, h('i', { style: 'width:10px;border-radius:50%;background:var(--muted)' }), T.lg.mark));
		main.appendChild(h('div', { class: 'card' }, h('h3', { text: T.timeline }), tl, legend, h('div', { class: 'note', text: T.derivedNote })));

		// Operations on the workflow.
		if (!DONE[root.status]) main.appendChild(h('div', { class: 'card' }, h('h3', { text: T.ops }), opsFor(root, root)));

		// The selected run (or the root).
		var r = runs.filter(function (x) { return x.run_id === state.sel; })[0] || root;
		main.appendChild(detail(r, root));
	}

	function niceStep(raw) {
		var steps = [100, 250, 500, 1000, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000, 600000, 900000, 1800000, 3600000, 7200000, 21600000, 43200000, 86400000];
		for (var i = 0; i < steps.length; i++) if (steps[i] >= raw) return steps[i];
		return Math.ceil(raw / 86400000) * 86400000;
	}

	function detail(r, root) {
		var card = h('div', { class: 'card' });
		card.appendChild(h('div', { class: 'head' }, h('h2', { style: 'font-size:16px', text: (T.kind[r.kind] || r.kind) + ': ' + (r.name || r.run_id) }), pill(r.status)));
		card.appendChild(h('div', { class: 'meta', text: r.run_id }));
		var rows = r.events.map(function (e) {
			var det = [e.error, e.errorType && e.errorType !== 'process_lost' ? e.errorType : null, e.retryable ? T.retryable : null, e.retryAfterMs ? 'retry after ' + dur(e.retryAfterMs) : null, e.deadline ? clock(e.deadline) : null].filter(Boolean).join(' · ');
			return h('tr', null, h('td', { class: 'm', text: clock(e.at) }), h('td', { class: 'm', text: '+' + dur(e.at - root.createdAt) }), h('td', { text: evText(e) }), h('td', { text: det }));
		});
		card.appendChild(h('h3', { style: 'margin-top:10px', text: T.events }));
		card.appendChild(h('table', null, h('tr', null, h('th', { text: T.at }), h('th', { text: T.since }), h('th', { text: T.what }), h('th', { text: T.detail })), rows));
		var kv = h('div', { class: 'kv', style: 'margin-top:12px' });
		kv.appendChild(h('div', null, h('h3', { text: T.input }), h('pre', { text: r.input === undefined ? '—' : json(r.input) })));
		kv.appendChild(h('div', null, h('h3', { text: r.error ? T.error : T.output }), h('pre', { text: r.error ? r.error : r.output === undefined ? '—' : json(r.output) })));
		card.appendChild(kv);
		if (r !== root && !DONE[r.status] && (r.kind === 'wait' || r.status === 'blocked')) {
			card.appendChild(h('h3', { style: 'margin-top:12px', text: T.ops }));
			card.appendChild(opsFor(r, root));
		}
		return card;
	}

	function opsFor(r, root) {
		var box = h('div', { class: 'ops' });
		var msg = h('div', { class: 'msg' });
		var done = function (p) {
			msg.className = 'msg'; msg.textContent = '…';
			p.then(function () { msg.textContent = T.sent; }).catch(function (e) { msg.className = 'msg err'; msg.textContent = e.message; });
		};
		var parse = function (text) { if (!text.trim()) return null; try { return JSON.parse(text); } catch (e) { throw new Error(T.badJson); } };
		if (r.kind === 'wait') {
			var ta = h('textarea', { placeholder: 'null' });
			box.appendChild(h('div', { class: 'box' }, h('div', { text: T.signal + ': ' + r.name }), ta,
				h('button', { class: 'primary', text: T.send, onclick: function () {
					try { done(post('/api/signal', { id: root.run_id, name: r.name, payload: parse(ta.value) })); } catch (e) { msg.className = 'msg err'; msg.textContent = e.message; }
				} })));
		}
		if (r.status === 'blocked') {
			var out = h('textarea', { placeholder: 'null' });
			var why = h('textarea', { placeholder: 'resolved as failed' });
			box.appendChild(h('div', { class: 'box' }, h('div', { text: T.resolveOk }), out,
				h('button', { class: 'primary', text: T.resolve, onclick: function () {
					try { done(post('/api/resolve', { callId: r.run_id, output: parse(out.value) })); } catch (e) { msg.className = 'msg err'; msg.textContent = e.message; }
				} })));
			box.appendChild(h('div', { class: 'box' }, h('div', { text: T.resolveFail }), why,
				h('button', { class: 'danger', text: T.resolveFailed, onclick: function () { done(post('/api/resolve', { callId: r.run_id, failed: true, error: why.value })); } })));
		}
		if (r === root) {
			box.appendChild(h('button', { class: 'danger', text: T.cancel, onclick: function () {
				if (confirm(T.confirmCancel)) done(post('/api/cancel', { id: root.run_id }));
			} }));
		}
		return h('div', null, box, msg);
	}

	document.getElementById('ph').textContent = T.pick;
	loadList();
	setInterval(loadList, 5000);
	setInterval(function () { if (state.data) render(); }, 1000);
	var m = /#run=([^&]+)/.exec(location.hash);
	if (m) select(decodeURIComponent(m[1]));
})();
</script>
</body>
</html>
`;
