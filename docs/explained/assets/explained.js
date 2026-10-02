// How RiftCluster works — shared figures. Every block returns early when its figure is not on the page.
(() => {
"use strict";
const reduce = matchMedia("(prefers-reduced-motion: reduce)").matches;
const NS = "http://www.w3.org/2000/svg";
const esc = s => String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
const cssv = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

/* ---------- packet animation helper ---------- */
function animatePackets(svg){
  if (reduce) return;
  svg.querySelectorAll("[data-dx]").forEach(p => {
    const dx = +p.dataset.dx, dy = +(p.dataset.dy || 0), dur = +(p.dataset.dur || 900);
    p.animate([{transform:"translate(0px,0px)"},{transform:`translate(${dx}px,${dy}px)`}],{duration:dur,easing:"cubic-bezier(.4,.1,.3,1)",fill:"forwards"});
  });
}

/* ---------- Sequence scene ---------- */
function SeqScene(id, cfg){
  const root = document.getElementById(id);
  if (!root) return;
  const variants = cfg.variants || [{name:"", steps:cfg.steps}];
  let vi = 0, i = 0, timer = null;
  root.innerHTML = `
    <div class="fig-head"><span class="fig-title">${esc(cfg.title)}</span><span class="fig-note">${esc(cfg.note || "step through")}</span></div>
    ${variants.length > 1 ? `<div class="controls" style="margin:0 0 10px"><label class="fig-note" for="${id}-v">Scenario</label><select id="${id}-v">${variants.map((v,k)=>`<option value="${k}">${esc(v.name)}</option>`).join("")}</select></div>` : ""}
    <div class="svgwrap"><svg></svg></div>
    <div class="seq-state" style="grid-template-columns:repeat(${cfg.actors.length},minmax(0,1fr))"></div>
    <div class="caption" aria-live="polite"></div>
    <div class="controls">
      <button type="button" data-a="prev" aria-label="Previous step">◀ Back</button>
      <button type="button" class="primary" data-a="next">Next ▶</button>
      <button type="button" data-a="play">Play</button>
      <span class="spacer"></span>
      <button type="button" data-a="reset">Restart</button>
    </div>`;
  const svg = root.querySelector("svg"), cap = root.querySelector(".caption"), st = root.querySelector(".seq-state");
  const btnPrev = root.querySelector("[data-a=prev]"), btnNext = root.querySelector("[data-a=next]"), btnPlay = root.querySelector("[data-a=play]");
  const A = cfg.actors, W = 760, top = 64, rowH = 38;
  const ax = k => 30 + (W-60)/A.length*(k+.5);
  const idx = Object.fromEntries(A.map((a,k)=>[a.id,k]));

  function layout(steps){
    let row = 0; const rows = [];
    steps.forEach((s,si) => (s.items||[]).forEach(it => rows.push({...it, si, row: row++})));
    return {rows, H: top + 20 + Math.max(row,1)*rowH};
  }
  function render(){
    const steps = variants[vi].steps, {rows, H} = layout(steps);
    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    let g = "";
    A.forEach((a,k) => {
      const x = ax(k), col = a.color || "var(--ink)";
      g += `<line x1="${x}" y1="${top-8}" x2="${x}" y2="${H-6}" stroke="var(--line)" stroke-width="1.5" stroke-dasharray="3 4"/>`;
      g += `<rect x="${x-62}" y="8" width="124" height="44" rx="9" fill="var(--bg)" stroke="${col}" stroke-width="1.6"/>`;
      g += `<text x="${x}" y="27" text-anchor="middle" font-size="13" font-weight="700" style="fill:${col}">${esc(a.label)}</text>`;
      if (a.sub) g += `<text x="${x}" y="43" text-anchor="middle" font-size="10" class="mono" style="fill:var(--muted)">${esc(a.sub)}</text>`;
    });
    rows.filter(r => r.si <= i).forEach(r => {
      const y = top + 14 + r.row*rowH, cls = r.si < i ? "past" : "";
      if (r.note !== undefined){
        const ks = [].concat(r.note).map(n => idx[n]);
        const x1 = ax(Math.min(...ks)), x2 = ax(Math.max(...ks));
        const w = Math.max(x2-x1+120, r.text.length*6.3+22), cx = (x1+x2)/2;
        const fill = {bad:"var(--bad-soft)", good:"var(--ok-soft)", commit:"var(--accent-soft)", warn:"var(--warn-soft)"}[r.kind] || "var(--paper)";
        const stroke = {bad:"var(--bad)", good:"var(--ok)", commit:"var(--accent)", warn:"var(--warn)"}[r.kind] || "var(--line)";
        g += `<g class="${cls}"><rect x="${cx-w/2}" y="${y-13}" width="${w}" height="26" rx="6" fill="${fill}" stroke="${stroke}"/><text x="${cx}" y="${y+4}" text-anchor="middle" font-size="11.5">${esc(r.text)}</text></g>`;
        return;
      }
      const x1 = ax(idx[r.f]), x2 = ax(idx[r.t]), dir = Math.sign(x2-x1);
      const kind = r.kind || "msg";
      const col = {bad:"var(--bad)", reply:"var(--muted)", ok:"var(--ok)", lost:"var(--bad)"}[kind] || "var(--accent)";
      const end = kind === "lost" ? (x1+x2)/2 : x2 - dir*4;
      g += `<g class="${cls}">`;
      g += `<line x1="${x1}" y1="${y}" x2="${end}" y2="${y}" stroke="${col}" stroke-width="1.8" ${kind==="reply"?'stroke-dasharray="5 4"':""}/>`;
      if (kind === "lost") g += `<text x="${end}" y="${y+5}" text-anchor="middle" font-size="16" font-weight="800" style="fill:var(--bad)">✕</text>`;
      else g += `<path d="M${x2-dir*2},${y} l${-dir*9},-5 v10 z" fill="${col}"/>`;
      const lx = (x1+end)/2;
      g += `<text x="${lx}" y="${y-7}" text-anchor="middle" font-size="11" class="mono" style="fill:${col === "var(--muted)" ? "var(--ink)" : col}">${esc(r.label)}</text>`;
      if (r.si === i) g += `<circle cx="${x1}" cy="${y}" r="5" fill="${col}" data-dx="${end-x1}"/>`;
      g += `</g>`;
    });
    svg.innerHTML = g;
    animatePackets(svg);
    // cumulative state
    const state = {}, changed = new Set();
    steps.slice(0, i+1).forEach((s,si) => Object.entries(s.state||{}).forEach(([k,v]) => { state[k] = v; if (si === i) changed.add(k); }));
    st.innerHTML = A.map(a => `<div class="cell ${changed.has(a.id)?"changed":""}"><b>${esc(a.label)}</b>${state[a.id] ? state[a.id] : '<span style="color:var(--muted)">—</span>'}</div>`).join("");
    const s = steps[i];
    cap.innerHTML = `<span class="stepno">STEP ${i+1} / ${steps.length}</span>${s.caption}`;
    btnPrev.disabled = i === 0; btnNext.disabled = i === steps.length-1;
  }
  function stop(){ clearInterval(timer); timer = null; btnPlay.textContent = "Play"; }
  root.addEventListener("click", e => {
    const a = e.target.closest("button")?.dataset.a; if (!a) return;
    const n = variants[vi].steps.length;
    if (a === "prev"){ stop(); i = Math.max(0,i-1); }
    if (a === "next"){ stop(); i = Math.min(n-1,i+1); }
    if (a === "reset"){ stop(); i = 0; }
    if (a === "play"){
      if (timer){ stop(); return; }
      if (i >= n-1) i = 0;
      btnPlay.textContent = "Pause";
      timer = setInterval(() => { if (i >= variants[vi].steps.length-1){ stop(); return; } i++; render(); }, 2300);
    }
    render();
  });
  root.querySelector("select")?.addEventListener("change", e => { stop(); vi = +e.target.value; i = 0; render(); });
  render();
}

/* ---------- Scenes ---------- */
const N = {A:"var(--nA)", B:"var(--nB)", C:"var(--nC)", D:"var(--nD)", E:"var(--nE)"};

const WA = [{id:"c",label:"Client (CI)"},{id:"B",label:"Node B",sub:"receives",color:N.B},{id:"L",label:"Node A",sub:"Raft leader",color:N.A},{id:"F",label:"Node C",sub:"follower",color:N.C}];
const wHead = [
  {caption:"The client sends <code>POST /imposters</code> with <code>Idempotency-Key: k1</code>. The balancer picks node B.", items:[{f:"c",t:"B",label:"POST /imposters  k1"}]},
  {caption:"<b>Park before forward.</b> B authenticates the request and durably records the intent in its own <code>pending_intents</code> table. From now on the request can't be lost.", items:[{note:"B",text:"park intent k1 (fsync)",kind:"commit"}], state:{B:"pending_intents: {k1}"}},
  {caption:"B forwards <code>ControlOp::PutImposter{op_id:k1}</code> to the leader.", items:[{f:"B",t:"L",label:"forward PutImposter k1"}]},
  {caption:"The leader checks the dedup map (<code>sm_op_dedup[k1]</code>: not seen) and validates the config. Anything the fleet would reject never enters the log.", items:[{note:"L",text:"dedup miss · validate ✓ · append entry N"}], state:{L:"log: …, N (uncommitted)"}},
];
const wReplicate = [
  {caption:"The leader sends entry N to both followers in parallel.", items:[{f:"L",t:"F",label:"AppendEntries(N)"},{f:"L",t:"B",label:"AppendEntries(N)"}]},
  {caption:"Each follower fsyncs N into its redb log before acknowledging.", items:[{f:"F",t:"L",label:"fsync'd ✓",kind:"reply"},{f:"B",t:"L",label:"fsync'd ✓",kind:"reply"}], state:{B:"pending_intents: {k1}<br>log: …, N",F:"log: …, N"}},
  {caption:"<b>Committed.</b> A majority has N on disk. A full power loss from here on replays it.", items:[{note:["B","F"],text:"entry N COMMITTED (majority, on disk)",kind:"commit"}], state:{L:"log: …, N ✓ committed"}},
];
SeqScene("seq-write", {
  title:"POST /imposters across three nodes", note:"choose a scenario",
  actors:WA,
  variants:[
    {name:"Normal path", steps:[...wHead, ...wReplicate,
      {caption:"The leader applies N: config into <code>sm_configs</code>, revision := N, <code>sm_op_dedup[k1] = N</code>, and the local engine binds :8080.", items:[{note:"L",text:"apply N · bind :8080",kind:"good"}], state:{L:"log ✓ N · applied N<br>dedup k1→N"}},
      {caption:"The leader returns the committed outcome to B, which clears the parked intent: the request is now in the log, so the intent has done its job.", items:[{f:"L",t:"B",label:"ok {revision: N}",kind:"reply"}], state:{B:"intent k1: done<br>log: …, N"}},
      {caption:"<b>The barrier.</b> Commit is not apply. B polls every member's applied index (<code>/internal/v1/applied</code>, every 25 ms) until each reports ≥ N, for up to 2 s.", items:[{f:"B",t:"L",label:"applied?"},{f:"L",t:"B",label:"applied N",kind:"ok"},{f:"B",t:"F",label:"applied?"},{f:"F",t:"B",label:"applied N",kind:"ok"}], state:{B:"intent k1: done<br>applied N",F:"applied N"}},
      {caption:"B answers <code>201</code> with proof: <code>Rift-Cluster-Revision: 8080@N</code> and <code>Rift-Cluster-Op-Id: k1</code>. The next request, to any node, is served from config N.", items:[{f:"B",t:"c",label:"201 · Revision 8080@N",kind:"ok"}]},
    ]},
    {name:"Leader dies after commit", steps:[...wHead, ...wReplicate,
      {caption:"The leader crashes after commit but before answering. The entry is safe: a majority has it.", items:[{note:"L",text:"✕ leader crashes",kind:"bad"}], state:{L:"<span style='color:var(--bad)'>down</span>"}},
      {caption:"Within about 150–300 ms, C's election timer fires. Its log contains N, so it can win; C becomes leader for a new term.", items:[{f:"F",t:"B",label:"RequestVote(term+1)"},{f:"B",t:"F",label:"vote ✓",kind:"reply"},{note:"F",text:"C is leader · commits N in its term",kind:"commit"}], state:{F:"leader · log …, N"}},
      {caption:"B's forward times out, so B retries the same op against the new leader.", items:[{f:"B",t:"F",label:"retry PutImposter k1"}]},
      {caption:"Dedup hits: <code>sm_op_dedup[k1] = N</code> is in the replicated state machine. C returns the <em>recorded</em> outcome and applies nothing twice.", items:[{note:"F",text:"dedup HIT k1 → revision N",kind:"good"},{f:"F",t:"B",label:"ok {revision: N}",kind:"reply"}]},
      {caption:"The client gets a slightly slower <code>201</code> carrying the same revision N. Exactly-once effect, despite the crash.", items:[{f:"B",t:"c",label:"201 · Revision 8080@N",kind:"ok"}]},
    ]},
    {name:"B is on the minority side", steps:[...wHead.slice(0,2),
      {caption:"B is partitioned away from the leader and C. It can't reach a quorum.", items:[{f:"B",t:"L",label:"forward k1",kind:"lost"}]},
      {caption:"B answers <code>503</code> with <code>Retry-After</code> and <code>Rift-Cluster-Op-Id: k1</code>. The request is refused, but it is not lost: it is durably queued.", items:[{f:"B",t:"c",label:"503 · Op-Id k1",kind:"bad"}], state:{c:"holds op-id k1"}},
      {caption:"The client can poll <code>GET /_cluster/ops/k1</code> (<code>pending</code>), or retry with the same key. Either is safe.", items:[{f:"c",t:"B",label:"GET /_cluster/ops/k1"},{f:"B",t:"c",label:"{state: pending}",kind:"reply"}]},
      {caption:"The partition heals. B's recovery loop replays the parked intent to the leader.", items:[{f:"B",t:"L",label:"replay PutImposter k1"}]},
      {caption:"The leader commits and applies it like any other write. Had the client also retried, both would hit the same dedup entry: one application.", items:[{note:["L","F"],text:"commit + apply entry N",kind:"commit"},{f:"L",t:"B",label:"ok {revision: N}",kind:"reply"}], state:{B:"intent k1: applied",L:"applied N",F:"applied N"}},
      {caption:"Now <code>GET /_cluster/ops/k1</code> answers <code>{state: applied, revision: N}</code>. Refused is not the same as lost.", items:[{f:"c",t:"B",label:"GET /_cluster/ops/k1"},{f:"B",t:"c",label:"{applied, revision N}",kind:"ok"}]},
    ]},
    {name:"Follower C is slow to apply", steps:[...wHead, ...wReplicate,
      {caption:"The leader applies N. C has N on disk but its state machine is wedged and hasn't applied it.", items:[{note:"L",text:"apply N",kind:"good"},{note:"F",text:"apply stalled…",kind:"warn"}], state:{F:"log …, N · applied N-1"}},
      {caption:"The leader returns the committed outcome to B, which clears its parked intent.", items:[{f:"L",t:"B",label:"ok {revision: N}",kind:"reply"}], state:{B:"intent k1: done"}},
      {caption:"B polls every member. The leader and B itself report applied ≥ N; C keeps answering N-1. The barrier waits up to <code>--cluster-write-barrier-timeout</code> (2 s).", items:[{f:"B",t:"L",label:"applied?"},{f:"L",t:"B",label:"applied N",kind:"ok"},{f:"B",t:"F",label:"applied?"},{f:"F",t:"B",label:"applied N-1",kind:"reply"}]},
      {caption:"The cap expires. The write still succeeds: it is committed and durable, and only C's apply is late.", items:[{note:"B",text:"barrier timeout · unapplied: C",kind:"warn"}]},
      {caption:"The response names the lagging node: <code>201</code> plus <code>Rift-Cluster-Warnings: unapplied=nodeC</code>. Success with a named asterisk. The pull-on-miss net covers requests that hit C meanwhile.", items:[{f:"B",t:"c",label:"201 · Warnings: unapplied=nodeC",kind:"ok"}]},
    ]},
  ]});

SeqScene("seq-handoff", {
  title:"Ownership handoff when a node leaves", note:"graceful leave (SIGTERM)",
  actors:[{id:"L",label:"Raft leader"},{id:"B",label:"Node B",sub:"owner of f",color:N.B},{id:"C",label:"Node C",sub:"successor #1",color:N.C},{id:"A",label:"Node A",sub:"successor #2",color:N.A}],
  steps:[
    {caption:"B owns flow <code>f</code> under membership index M−1. Every write it accepts is pushed to its two HRW successors, C and A.", items:[{f:"B",t:"C",label:"replicate f (M-1, 42, B)"},{f:"B",t:"A",label:"replicate f (M-1, 42, B)"}], state:{B:"f = AwaitingPayment<br>(M-1, v42, B)",C:"replica (M-1, 42, B)",A:"replica (M-1, 42, B)"}},
    {caption:"B receives SIGTERM. It drains readiness so the balancer stops sending it traffic, then asks to leave. There is no separate flush: every write was already pushed when it was applied.", items:[{note:"B",text:"SIGTERM · drain readiness",kind:"warn"},{f:"B",t:"L",label:"leave"}]},
    {caption:"The leader commits membership entry M: B removed. This entry, not any negotiation, is what transfers ownership.", items:[{note:"L",text:"commit M: members − B",kind:"commit"},{f:"L",t:"C",label:"AppendEntries(M)"},{f:"L",t:"A",label:"AppendEntries(M)"}], state:{B:"<span style='color:var(--muted)'>departed</span>"}},
    {caption:"C applies M and recomputes the ring over the new voter set: it is now the highest-scoring voter for <code>f</code>. A computes the same answer independently.", items:[{note:"C",text:"ring @ M: owner(f) = C",kind:"good"},{note:"A",text:"ring @ M: owner(f) = C"}]},
    {caption:"C pulls <code>f</code> from the replicas and takes the highest <code>(m_idx, v, origin)</code> it finds.", items:[{f:"C",t:"A",label:"pull range f"},{f:"A",t:"C",label:"f = (M-1, 42, B)",kind:"reply"}]},
    {caption:"C adopts and serves. Its writes now carry <code>m_idx = M</code>, so anything B wrote under M−1 would lose any later comparison by arithmetic. Staleness is at most one replication round behind B's last accepted write.", items:[{note:"C",text:"adopt · serve f · writes stamped m_idx = M",kind:"good"}], state:{C:"OWNER f = AwaitingPayment<br>next write (M, 1, C)"}},
  ]});

SeqScene("seq-isolated", {
  title:"The isolated-owner rule during a partition", note:"D-17",
  actors:[{id:"t",label:"Test",sub:"flow f"},{id:"B",label:"Node B",sub:"owner of f",color:N.B},{id:"L",label:"Leader A",color:N.A},{id:"C",label:"Node C",color:N.C}],
  steps:[
    {caption:"A partition cuts B off from the leader and C. B still owns <code>f</code>, and the balancer may still route to it.", items:[{f:"L",t:"B",label:"heartbeat",kind:"lost"}], state:{B:"owner f · leader: A",L:"leader",C:"follower"}},
    {caption:"B stops hearing heartbeats. About 450–600 ms after the last one, it clears <code>current_leader</code> and campaigns. It can't win: it can't reach a majority.", items:[{note:"B",text:"no leader → is_isolated() = true",kind:"warn"}], state:{B:"owner f · <b>isolated</b>"}},
    {caption:"A request for <code>f</code> reaches B. B is the owner, but it refuses its own owner-side read rather than answer from state a healed majority may disagree with.", items:[{f:"t",t:"B",label:"GET /status (flow f)"},{f:"B",t:"t",label:"503 owner is isolated",kind:"bad"}]},
    {caption:"On the quorum side, nothing about <code>f</code> changes until membership does. A and C stay healthy and keep serving every stateless request and every flow they own.", items:[{note:["L","C"],text:"quorum side keeps serving",kind:"good"}]},
    {caption:"The partition heals. B hears the leader again, <code>is_isolated()</code> turns false, and B serves <code>f</code>. No write was accepted on the wrong side, so there is nothing to reconcile.", items:[{f:"L",t:"B",label:"heartbeat"},{note:"B",text:"isolated = false · serve f",kind:"good"}], state:{B:"owner f · leader: A"}},
  ]});

/* ---------- Topology animation ---------- */
(function topo(){
  if (!document.getElementById("fig-topo")) return;
  const fig = document.getElementById("fig-topo"), svg = fig.querySelector("svg");
  const nodes = [{id:"A",x:150},{id:"B",x:380},{id:"C",x:610}], ny = 215, lbY = 112;
  let g = "";
  [["Test runner 1",130],["Test runner 2",300],["CI job",460],["Test runner 3",630]].forEach(([l,x]) => {
    g += `<rect x="${x-62}" y="10" width="124" height="32" rx="8" fill="var(--bg)" stroke="var(--line)"/><text x="${x}" y="31" text-anchor="middle" font-size="12">${l}</text>`;
  });
  g += `<rect x="230" y="${lbY-20}" width="300" height="40" rx="10" fill="var(--accent-soft)" stroke="var(--accent)"/><text x="380" y="${lbY+5}" text-anchor="middle" font-size="13" font-weight="700" style="fill:var(--accent)">Load balancer · round-robin</text>`;
  g += `<path d="M150,${ny} Q380,${ny+95} 610,${ny}" fill="none" stroke="var(--muted)" stroke-dasharray="4 4"/>`;
  g += `<line x1="150" y1="${ny}" x2="380" y2="${ny}" stroke="var(--muted)" stroke-dasharray="4 4"/><line x1="380" y1="${ny}" x2="610" y2="${ny}" stroke="var(--muted)" stroke-dasharray="4 4"/>`;
  g += `<text x="380" y="292" text-anchor="middle" font-size="10.5" class="mono" style="fill:var(--muted)">cluster port · Raft + owner RPC + replication · HMAC-SHA256</text>`;
  nodes.forEach(n => {
    const c = `var(--n${n.id})`;
    g += `<rect x="${n.x-82}" y="${ny-34}" width="164" height="68" rx="11" fill="var(--paper)" stroke="${c}" stroke-width="2"/>`;
    g += `<text x="${n.x}" y="${ny-12}" text-anchor="middle" font-size="14" font-weight="700" style="fill:${c}">Node ${n.id}</text>`;
    g += `<text x="${n.x}" y="${ny+6}" text-anchor="middle" font-size="10" class="mono" style="fill:var(--muted)">:8080 :9090 gateway</text>`;
    g += `<text x="${n.x}" y="${ny+21}" text-anchor="middle" font-size="10" class="mono" style="fill:var(--muted)">admin · cluster port</text>`;
  });
  g += `<g id="topo-pk"></g>`;
  svg.innerHTML = g;
  const layer = svg.querySelector("#topo-pk");
  let on = !reduce, rr = 0, h = null;
  const srcX = [130,300,460,630];
  function shot(){
    const sx = srcX[Math.floor(Math.random()*4)], n = nodes[rr++ % 3];
    const c = document.createElementNS(NS,"circle");
    c.setAttribute("r","4.5"); c.setAttribute("cx", sx); c.setAttribute("cy", 44); c.setAttribute("fill","var(--accent)");
    layer.appendChild(c);
    c.animate([{transform:"translate(0,0)"},{transform:`translate(${380-sx}px,${lbY-44}px)`, offset:.45},{transform:`translate(${n.x-sx}px,${ny-36-44}px)`}],{duration:1500,easing:"ease-in-out",fill:"forwards"}).onfinish = () => c.remove();
  }
  function run(){ clearInterval(h); if (on) h = setInterval(shot, 420); }
  const btn = fig.querySelector("[data-act=toggle]");
  btn.setAttribute("aria-pressed", String(on)); btn.textContent = on ? "Pause traffic" : "Start traffic";
  btn.addEventListener("click", () => { on = !on; btn.setAttribute("aria-pressed", String(on)); btn.textContent = on ? "Pause traffic" : "Start traffic"; run(); });
  run();
})();

/* ---------- Raft simulation ---------- */
(function raft(){
  if (!document.getElementById("fig-raft")) return;
  const fig = document.getElementById("fig-raft"), svg = fig.querySelector("svg"), evBox = fig.querySelector(".events");
  const IDS = ["A","B","C","D","E"], CX = 280, CY = 175, R = 128, MAJ = 3;
  const SCALE = 15, HB = 50, LAT = 24;
  let nodes, msgs, simT, last, paused = false, partitioned = false, wseq = 0, raf;
  const rnd = (a,b) => a + Math.random()*(b-a);
  const pos = k => ({x: CX + R*Math.sin(k*2*Math.PI/5), y: CY - R*Math.cos(k*2*Math.PI/5)});
  const side = id => (id === "A" || id === "B") ? 0 : 1;
  const linked = (a,b) => !partitioned || side(a) === side(b);
  function init(){
    nodes = IDS.map((id,k) => ({id, ...pos(k), alive:true, role:"follower", term:0, votedFor:null, log:[], commit:0, deadline:rnd(150,300)+300, votes:new Set(), match:{}, nextHb:0, leader:null}));
    msgs = []; simT = 0; last = null; wseq = 0; evBox.innerHTML = "";
    log("cluster starts: 5 voters, no leader yet", "");
  }
  const by = id => nodes.find(n => n.id === id);
  function log(text, cls){
    const d = document.createElement("div");
    d.innerHTML = `<span class="t">${String(Math.round(simT)).padStart(5," ")}ms</span> <span class="${cls ? "ev-"+cls : ""}">${text}</span>`;
    evBox.prepend(d);
    while (evBox.children.length > 60) evBox.lastChild.remove();
  }
  function send(from, to, type, body){
    msgs.push({from, to, type, body, t0:simT, t1:simT + LAT + rnd(0,8), cut: !linked(from,to)});
  }
  function lastTerm(n){ return n.log.length ? n.log[n.log.length-1].term : 0; }
  function resetTimer(n){ n.deadline = simT + rnd(150,300); }
  function becomeFollower(n, term){ n.role = "follower"; if (term > n.term){ n.term = term; n.votedFor = null; } }
  function broadcastAE(n){ nodes.forEach(o => { if (o !== n) send(n.id, o.id, "AE", {term:n.term, log:n.log.slice(), commit:n.commit}); }); n.nextHb = simT + HB; }
  function deliver(m){
    const n = by(m.to); if (!n.alive || m.cut || !linked(m.from, m.to)) return;
    const b = m.body;
    if (b.term > n.term){ const wasLeader = n.role === "leader"; becomeFollower(n, b.term); if (wasLeader) log(`${n.id} sees term ${b.term} and steps down`, "warn"); }
    if (m.type === "RV"){
      const upToDate = b.lastTerm > lastTerm(n) || (b.lastTerm === lastTerm(n) && b.lastIdx >= n.log.length);
      const grant = b.term === n.term && (n.votedFor === null || n.votedFor === m.from) && upToDate;
      if (grant){ n.votedFor = m.from; resetTimer(n); }
      else if (b.term === n.term && !upToDate && n.votedFor === null) log(`${n.id} refuses ${m.from}: its log is behind`, "warn");
      send(n.id, m.from, "RVR", {term:n.term, granted:grant});
    } else if (m.type === "RVR"){
      if (n.role === "candidate" && b.term === n.term && b.granted){
        n.votes.add(m.from);
        if (n.votes.size >= MAJ){
          n.role = "leader"; n.leader = n.id; n.match = {[n.id]: n.log.length};
          log(`<b>${n.id} wins term ${n.term}</b> with ${n.votes.size} votes`, "ok");
          broadcastAE(n);
        }
      }
    } else if (m.type === "AE"){
      if (b.term < n.term){ send(n.id, m.from, "AER", {term:n.term, ok:false}); return; }
      if (n.role !== "follower") n.role = "follower";
      n.leader = m.from; resetTimer(n);
      n.log = b.log.slice();
      n.commit = Math.min(Math.max(n.commit, b.commit), n.log.length);
      send(n.id, m.from, "AER", {term:n.term, ok:true, match:n.log.length});
    } else if (m.type === "AER"){
      if (n.role !== "leader" || b.term !== n.term || !b.ok) return;
      n.match[m.from] = b.match; n.match[n.id] = n.log.length;
      for (let k = n.log.length; k > n.commit; k--){
        if (n.log[k-1].term !== n.term) break;
        const cnt = Object.values(n.match).filter(v => v >= k).length;
        if (cnt >= MAJ){
          for (let j = n.commit; j < k; j++) log(`entry ${j+1} (${n.log[j].label}) committed → 201, Rift-Cluster-Revision: 8080@${j+1}`, "ok");
          n.commit = k; break;
        }
      }
    }
  }
  function step(dt){
    simT += dt;
    msgs.sort((a,b) => a.t1 - b.t1);
    while (msgs.length && msgs[0].t1 <= simT){ deliver(msgs.shift()); }
    nodes.forEach(n => {
      if (!n.alive) return;
      if (n.role === "leader"){ if (simT >= n.nextHb) broadcastAE(n); return; }
      if (simT >= n.deadline){
        n.role = "candidate"; n.term++; n.votedFor = n.id; n.votes = new Set([n.id]); n.leader = null; resetTimer(n);
        log(`${n.id} timed out → candidate for term ${n.term}`, "warn");
        nodes.forEach(o => { if (o !== n) send(n.id, o.id, "RV", {term:n.term, lastIdx:n.log.length, lastTerm:lastTerm(n)}); });
      }
    });
  }
  const termHue = t => `hsl(${(t*67 + 210) % 360} 55% 52%)`;
  function draw(){
    let g = "";
    for (let a = 0; a < 5; a++) for (let b = a+1; b < 5; b++){
      const p = nodes[a], q = nodes[b], cut = !linked(p.id, q.id);
      g += `<line x1="${p.x}" y1="${p.y}" x2="${q.x}" y2="${q.y}" stroke="${cut ? "var(--bad)" : "var(--line)"}" stroke-width="${cut?1.5:1}" ${cut ? 'stroke-dasharray="3 5"' : ""}/>`;
    }
    if (partitioned) g += `<text x="${CX}" y="${CY+4}" text-anchor="middle" font-size="11" class="mono" style="fill:var(--bad)">partition</text>`;
    msgs.forEach(m => {
      const p = by(m.from), q = by(m.to);
      let f = (simT - m.t0) / (m.t1 - m.t0); f = Math.max(0, Math.min(1, f));
      if (m.cut && f > .5) return;
      const x = p.x + (q.x-p.x)*f, y = p.y + (q.y-p.y)*f;
      const col = m.type === "RV" ? "var(--warn)" : m.type === "RVR" ? (m.body.granted ? "var(--ok)" : "var(--bad)") : (m.type === "AE" ? (m.body.log.length > by(m.to).log.length ? "var(--accent)" : "var(--muted)") : "var(--muted)");
      const r = m.type === "AER" ? 2.5 : (m.type === "AE" && col !== "var(--accent)") ? 3 : 5;
      if (m.type === "RVR" && !m.body.granted) return;
      g += `<circle cx="${x}" cy="${y}" r="${r}" fill="${col}" opacity="${m.type==="AER"?.5:1}"/>`;
    });
    nodes.forEach(n => {
      const col = !n.alive ? "var(--muted)" : n.role === "leader" ? "var(--accent)" : n.role === "candidate" ? "var(--warn)" : "var(--ink)";
      g += `<g class="raft-node" data-id="${n.id}" tabindex="0" role="button" aria-label="Node ${n.id}, ${n.alive ? n.role : "crashed"}, term ${n.term}. Activate to ${n.alive ? "crash" : "restart"}.">`;
      g += `<circle cx="${n.x}" cy="${n.y}" r="27" fill="${n.alive ? "var(--paper)" : "var(--faint)"}" stroke="${col}" stroke-width="${n.role==="leader"?3.5:2}"/>`;
      if (n.alive && n.role !== "leader"){
        const frac = Math.max(0, Math.min(1, (n.deadline - simT)/300)), rr = 33, L = 2*Math.PI*rr;
        g += `<circle cx="${n.x}" cy="${n.y}" r="${rr}" fill="none" stroke="${n.role==="candidate"?"var(--warn)":"var(--line)"}" stroke-width="3" stroke-dasharray="${L*frac} ${L}" transform="rotate(-90 ${n.x} ${n.y})"/>`;
      }
      g += `<text x="${n.x}" y="${n.y+1}" text-anchor="middle" font-size="17" font-weight="800" style="fill:${col}">${n.alive ? n.id : "✕"}</text>`;
      g += `<text x="${n.x}" y="${n.y+15}" text-anchor="middle" font-size="9" class="mono" style="fill:var(--muted)">term ${n.term}</text>`;
      const lab = !n.alive ? "crashed" : n.role === "leader" ? "LEADER" : n.role === "candidate" ? `candidate ${n.votes.size}/5` : (n.votedFor ? `voted ${n.votedFor}` : "follower");
      const above = n.y < CY - 20;
      g += `<text x="${n.x}" y="${above ? n.y-40 : n.y+47}" text-anchor="middle" font-size="10.5" font-weight="700" style="fill:${col}">${lab}</text>`;
      // log
      const show = n.log.slice(-7), off = n.log.length - show.length, bw = 13;
      const lx = n.x - (7*bw)/2, ly = above ? n.y - 66 : n.y + 54;
      for (let k = 0; k < 7; k++){
        const e = show[k], x = lx + k*bw;
        if (!e){ g += `<rect x="${x}" y="${ly}" width="${bw-2}" height="11" rx="2" fill="none" stroke="var(--faint)"/>`; continue; }
        const committed = off + k < n.commit, c = termHue(e.term);
        g += `<rect x="${x}" y="${ly}" width="${bw-2}" height="11" rx="2" fill="${committed ? c : "none"}" stroke="${c}" stroke-width="1.4" ${committed ? "" : 'stroke-dasharray="2 1.5"'}/>`;
      }
      g += `</g>`;
    });
    g += `<text x="10" y="392" font-size="10" class="mono" style="fill:var(--muted)">cluster time ${Math.round(simT)} ms · ×${SCALE} slower · log squares coloured by term · solid = committed</text>`;
    svg.innerHTML = g;
  }
  function frame(ts){
    if (last !== null && !paused){ const dt = Math.min(ts - last, 60) / SCALE; step(dt); }
    last = ts; draw(); raf = requestAnimationFrame(frame);
  }
  function toggle(id){
    const n = by(id);
    if (n.alive){ n.alive = false; log(`${id} crashed (its log and vote stay on disk)`, "bad"); }
    else { n.alive = true; n.role = "follower"; n.leader = null; resetTimer(n); n.deadline += 100; log(`${id} restarts as follower, term ${n.term}`, "acc"); }
  }
  svg.addEventListener("click", e => { const g = e.target.closest(".raft-node"); if (g) toggle(g.dataset.id); });
  svg.addEventListener("keydown", e => { if ((e.key === "Enter" || e.key === " ") && e.target.closest(".raft-node")){ e.preventDefault(); toggle(e.target.closest(".raft-node").dataset.id); } });
  fig.querySelector(".controls").addEventListener("click", e => {
    const b = e.target.closest("button"); if (!b) return;
    const a = b.dataset.act;
    if (a === "write"){
      const alive = nodes.filter(n => n.alive); if (!alive.length) return;
      const recv = alive[Math.floor(Math.random()*alive.length)];
      const leaderId = recv.role === "leader" ? recv.id : recv.leader;
      const L = leaderId && by(leaderId);
      const label = `w${++wseq}`;
      if (L && L.alive && L.role === "leader" && linked(recv.id, L.id)){
        L.log.push({term:L.term, label}); L.match[L.id] = L.log.length;
        log(`client → ${recv.id}${recv.id !== L.id ? ` → leader ${L.id}` : " (leader)"}: ${label} appended at index ${L.log.length}`, "acc");
        broadcastAE(L);
      } else {
        log(`client → ${recv.id}: no reachable leader → 503, Rift-Cluster-Op-Id: ${label} (intent parked, replays later)`, "bad");
      }
    }
    if (a === "killleader"){
      const L = nodes.filter(n => n.alive && n.role === "leader").sort((x,y) => y.term - x.term)[0];
      if (L) toggle(L.id); else log("no live leader to crash right now", "warn");
    }
    if (a === "partition"){
      partitioned = !partitioned; b.setAttribute("aria-pressed", String(partitioned));
      b.textContent = partitioned ? "Heal the partition" : "Partition {A,B} | {C,D,E}";
      log(partitioned ? "network partition: {A,B} cannot reach {C,D,E}" : "partition healed", partitioned ? "bad" : "ok");
    }
    if (a === "pause"){ paused = !paused; b.setAttribute("aria-pressed", String(paused)); b.textContent = paused ? "Resume" : "Pause"; }
    if (a === "reset"){ partitioned = false; const p = fig.querySelector("[data-act=partition]"); p.setAttribute("aria-pressed","false"); p.textContent = "Partition {A,B} | {C,D,E}"; init(); }
  });
  init(); raf = requestAnimationFrame(frame);
})();

/* ---------- Request explorer ---------- */
(function req(){
  if (!document.getElementById("fig-req")) return;
  const STAGES = [
    ["entry","Enabled?","imposter enabled bit"],
    ["entry","Resolve flow_id","port or X-Flow-Id header"],
    ["stateless","Select + match","indexes, predicates, in order"],
    ["owner","Scenario gate","owner read: state == required?"],
    ["stateless","Sequencer","next() of cycling responses"],
    ["stateless","Build response","behaviors, templates, scripts"],
    ["owner","Transition","owner CAS: willSetStateTo"],
    ["local","Journal","record request, this node only"],
  ];
  const TYPES = {
    plain:{label:"Plain stub", on:[0,1,2,5,7], rpc:[], status:"200", text:"The whole request runs in-process: unmodified engine code. The cluster never appears, which is why clustering costs the stateless path ≤ 2%."},
    gated:{label:"Scenario-gated stub", on:[0,1,2,3,5,7], rpc:[3], status:"200", text:"The stub matches only if flow <code>f</code> is in its required state. The state is read from the flow's owner, always, so it is correct under any load balancer, including plain round-robin."},
    trans:{label:"Gate + transition", on:[0,1,2,3,5,6,7], rpc:[3,6], status:"200", text:"Read the state at the owner, then compare-and-set it to <code>willSetStateTo</code>. Two round-trips of the same class, each sub-millisecond on a LAN."},
    seq:{label:"Cycling responses", on:[0,1,2,4,5,7], rpc:[], status:"200", text:"The response cursor is local by default. An imposter can opt into an owner-routed cursor (D-47); if that owner can't answer, it falls back to the local cursor and counts the fallback rather than failing."},
    down:{label:"Gated, owner unreachable", on:[0,1,2,3], rpc:[3], fail:3, status:"503", text:"The owner is unreachable, or refuses because it is isolated. The default answer is a fast <code>503</code> with the reason in the error body. A stale answer would make the test pass falsely."},
    off:{label:"Imposter disabled", on:[0], rpc:[], fail:0, status:"503", text:"<code>enabled: false</code> is replicated config: an operator decision that has to survive restarts and apply on every node, so it travels through Raft like any other config."},
  };
  const types = document.getElementById("req-types"), pipe = document.getElementById("req-pipe"), self = document.getElementById("req-self");
  let cur = "gated";
  types.innerHTML = Object.entries(TYPES).map(([k,t]) => `<button type="button" data-k="${k}" aria-pressed="${k===cur}">${t.label}</button>`).join("");
  function render(){
    const t = TYPES[cur], mine = self.checked;
    pipe.innerHTML = STAGES.map(([z,b,w],k) => {
      let cls = "off";
      if (t.on.includes(k)) cls = t.fail === k ? "fail" : t.rpc.includes(k) ? (mine ? "mem" : "rpc") : "local";
      let what = w;
      if (t.rpc.includes(k) && t.on.includes(k)) what = t.fail === k ? "owner unreachable → 503" : mine ? "owner == self: memory" : "1 LAN RPC to the owner";
      return `<div class="stage ${cls}"><span class="zn">${z}</span><b>${b}</b><span class="what">${what}</span></div>`;
    }).join("");
    const rpcs = t.rpc.filter(k => t.on.includes(k)).length;
    document.getElementById("req-rpc").textContent = mine ? 0 : rpcs;
    const s = document.getElementById("req-status"); s.textContent = t.status; s.style.color = t.status === "200" ? "var(--ok)" : "var(--bad)";
    document.getElementById("req-text").innerHTML = t.text + (mine && rpcs ? " <em>This node owns the flow, so the owner calls short-circuit to memory. That happens for about 1/N of flows; the design never relies on it.</em>" : "");
    types.querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.k === cur)));
  }
  types.addEventListener("click", e => { const b = e.target.closest("button"); if (b){ cur = b.dataset.k; render(); } });
  self.addEventListener("change", render);
  render();
})();

/* ---------- xxh64 (BigInt) ---------- */
const M64 = (1n<<64n) - 1n;
const P1 = 0x9E3779B185EBCA87n, P2 = 0xC2B2AE3D27D4EB4Fn, P3 = 0x165667B19E3779F9n, P4 = 0x85EBCA77C2B2AE63n, P5 = 0x27D4EB2F165667C5n;
const rotl = (x,r) => ((x << r) | (x >> (64n - r))) & M64;
const mul = (a,b) => (a*b) & M64;
const add = (a,b) => (a+b) & M64;
function rd64(b,i){ let v = 0n; for (let k = 7; k >= 0; k--) v = (v << 8n) | BigInt(b[i+k]); return v; }
function rd32(b,i){ return BigInt((b[i] | (b[i+1]<<8) | (b[i+2]<<16)) >>> 0) + (BigInt(b[i+3]) << 24n); }
function round(acc, input){ acc = add(acc, mul(input, P2)); acc = rotl(acc, 31n); return mul(acc, P1); }
function mergeRound(acc, val){ val = round(0n, val); acc ^= val; return add(mul(acc, P1), P4); }
function xxh64(b, seed = 0n){
  const len = b.length; let i = 0, h;
  if (len >= 32){
    let v1 = add(add(seed,P1),P2), v2 = add(seed,P2), v3 = seed, v4 = (seed - P1) & M64;
    while (i <= len - 32){ v1 = round(v1, rd64(b,i)); v2 = round(v2, rd64(b,i+8)); v3 = round(v3, rd64(b,i+16)); v4 = round(v4, rd64(b,i+24)); i += 32; }
    h = add(add(rotl(v1,1n), rotl(v2,7n)), add(rotl(v3,12n), rotl(v4,18n)));
    h = mergeRound(h,v1); h = mergeRound(h,v2); h = mergeRound(h,v3); h = mergeRound(h,v4);
  } else h = add(seed, P5);
  h = add(h, BigInt(len));
  while (i + 8 <= len){ h ^= round(0n, rd64(b,i)); h = add(mul(rotl(h,27n), P1), P4); i += 8; }
  if (i + 4 <= len){ h ^= mul(rd32(b,i), P1); h = add(mul(rotl(h,23n), P2), P3); i += 4; }
  while (i < len){ h ^= mul(BigInt(b[i]), P5); h = mul(rotl(h,11n), P1); i++; }
  h ^= h >> 33n; h = mul(h, P2); h ^= h >> 29n; h = mul(h, P3); h ^= h >> 32n;
  return h;
}
window.__xxh64 = xxh64;

/* ---------- Ring playground ---------- */
(function ring(){
  if (!document.getElementById("fig-ring")) return;
  const enc = new TextEncoder();
  const NAMES = ["rift-0","rift-1","rift-2","rift-3","rift-4"], COLS = ["var(--nA)","var(--nB)","var(--nC)","var(--nD)","var(--nE)"];
  const nodes = NAMES.map((name,k) => { let id = xxh64(enc.encode(name)); if (id === 0n) id = 1n; return {name, id, color:COLS[k], member:true}; });
  const le = id => { const a = new Uint8Array(8); let v = id; for (let k = 0; k < 8; k++){ a[k] = Number(v & 0xffn); v >>= 8n; } return a; };
  const score = (id, key) => { const kb = enc.encode(key), b = new Uint8Array(9 + kb.length); b.set(le(id)); b[8] = 1; b.set(kb, 9); return xxh64(b); };
  const ranked = key => nodes.filter(n => n.member).map(n => ({n, s:score(n.id, key)})).sort((x,y) => x.s === y.s ? (y.n.id > x.n.id ? 1 : -1) : (y.s > x.s ? 1 : -1));
  const names = ["checkout","cart","login","pay","search","refund"];
  const FLOWS = Array.from({length:30}, (_,k) => `i8080:${names[k % 6]}-${100 + k*7}`);
  const cols = document.getElementById("ring-cols"), cap = document.getElementById("ring-caption");
  let owners = {};
  function compute(){ const o = {}; FLOWS.forEach(f => { const r = ranked(f); o[f] = r.length ? r[0].n.name : null; }); return o; }
  function build(){
    cols.innerHTML = nodes.map((n,k) => `<div class="rcol" data-n="${n.name}"><header><span class="nm"><i style="background:${n.color}"></i>${n.name}</span><span class="id">id 0x${n.id.toString(16).padStart(16,"0")}</span><button type="button" data-k="${k}">Leave membership</button></header><div class="chips"></div></div>`).join("");
  }
  function place(moved){
    const before = new Map();
    cols.querySelectorAll(".chip").forEach(c => before.set(c.dataset.f, c.getBoundingClientRect()));
    cols.querySelectorAll(".chips").forEach(c => c.innerHTML = "");
    FLOWS.forEach(f => {
      const col = cols.querySelector(`.rcol[data-n="${owners[f]}"] .chips`); if (!col) return;
      const c = document.createElement("span"); c.className = "chip" + (moved.has(f) ? " moved" : ""); c.dataset.f = f; c.textContent = f.replace("i8080:","");
      c.title = f; col.appendChild(c);
    });
    if (reduce) return;
    cols.querySelectorAll(".chip").forEach(c => {
      const b = before.get(c.dataset.f); if (!b) return;
      const a = c.getBoundingClientRect(), dx = b.left - a.left, dy = b.top - a.top;
      if (dx || dy) c.animate([{transform:`translate(${dx}px,${dy}px)`},{transform:"translate(0,0)"}],{duration:moved.has(c.dataset.f) ? 700 : 350, easing:"cubic-bezier(.3,.1,.2,1)"});
    });
  }
  function refresh(changedNode){
    const prev = owners; owners = compute();
    const moved = new Set(FLOWS.filter(f => prev[f] && prev[f] !== owners[f]));
    cols.querySelectorAll(".rcol").forEach((c,k) => { c.classList.toggle("gone", !nodes[k].member); const b = c.querySelector("button"); b.textContent = nodes[k].member ? "Leave membership" : "Rejoin"; b.disabled = nodes[k].member && nodes.filter(n => n.member).length <= 2; });
    place(moved);
    const members = nodes.filter(n => n.member).length;
    if (changedNode){
      const verb = changedNode.member ? "joining" : "leaving";
      cap.innerHTML = `<span class="stepno">MEMBERSHIP ENTRY COMMITTED · ${members} VOTERS</span><b>${changedNode.name}</b> ${verb} moved <b>${moved.size} of ${FLOWS.length}</b> flows (highlighted)${changedNode.member ? `, exactly the ones it now wins` : `, exactly the ones it owned`}. Every other flow kept its owner.${!changedNode.member ? " The leader refuses a departure that would leave fewer than two voters (D-25), so the last two can't leave." : ""}`;
    }
    lookup();
  }
  cols.addEventListener("click", e => { const b = e.target.closest("button"); if (!b) return; const n = nodes[+b.dataset.k]; n.member = !n.member; refresh(n); });
  const keyIn = document.getElementById("ring-key"), tbl = document.getElementById("ring-scores");
  function lookup(){
    const key = keyIn.value; const r = ranked(key); const max = 18446744073709551615;
    tbl.innerHTML = `<thead><tr><th>rank</th><th>voter</th><th>role for this key</th><th>score = xxh64(id ‖ 0x01 ‖ key)</th><th style="width:28%"></th></tr></thead><tbody>` +
      r.map((x,k) => `<tr><td>${k+1}</td><td><span style="color:${x.n.color};font-weight:700">${x.n.name}</span></td><td class="role" style="color:${k===0?"var(--accent)":k<3?"var(--ink)":"var(--muted)"}">${k===0?"owner":k<3?"replica successor":"—"}</td><td>0x${x.s.toString(16).padStart(16,"0")}</td><td><div class="bar" style="width:${(Number(x.s)/max*100).toFixed(1)}%;${k===0?"":"opacity:.35"}"></div></td></tr>`).join("") + `</tbody>`;
  }
  keyIn.addEventListener("input", lookup);
  build(); owners = compute(); refresh(null);
})();

SeqScene("seq-wal-raft", {
  title:"Entry N reaches a follower's disk", note:"raft.redb · every step is real",
  actors:[{id:"L",label:"Leader A",color:N.A},{id:"core",label:"openraft on C",sub:"follower",color:N.C},{id:"db",label:"raft.redb",sub:"C's state dir"},{id:"eng",label:"Engine on C",sub:"bound imposters"}],
  steps:[
    {caption:"The leader sends AppendEntries carrying entry N: a JSON <code>ControlRequest</code> with <code>op_id</code> k1, a <code>PutImposter</code> for :8080, and <code>issued_at_secs</code>.", items:[{f:"L",t:"core",label:"AppendEntries(N)"}]},
    {caption:"openraft hands the entry to storage. One redb write transaction inserts <code>raft_log[N]</code> and commits at <code>Immediate</code>, so <code>commit()</code> returns only after the fsync.", items:[{f:"core",t:"db",label:"append N · commit(Immediate)"},{note:"db",text:"fsync",kind:"commit"}], state:{db:"raft_log: …, N  (on disk)"}},
    {caption:"Only now does storage report the I/O complete, and only now does C acknowledge. An acknowledgement is a promise that the entry survives a power cut, which is what lets “committed” mean “on disk on a majority”.", items:[{f:"db",t:"core",label:"log_io_completed",kind:"reply"},{f:"core",t:"L",label:"ack N (durable)",kind:"reply"}]},
    {caption:"With a majority's acks the leader advances the commit index; its next message tells C.", items:[{note:"L",text:"commit index = N",kind:"commit"},{f:"L",t:"core",label:"heartbeat · commit = N"}], state:{L:"committed N"}},
    {caption:"C applies N in <b>one</b> redb transaction: dedup check, write <code>sm_configs[8080]</code>, record <code>sm_op_dedup[k1]</code>, and move <code>sm_applied</code> (the applied pointer and current membership) in the same commit. After a crash, the state and “how far I have applied” can never disagree.", items:[{f:"core",t:"db",label:"apply N (one transaction)"}], state:{db:"raft_log …, N<br>sm_configs[8080] · sm_applied = N"}},
    {caption:"After that commit, apply drives the local engine: bind :8080, swap the stub list. A bind that fails is recorded as status on this node; it never stalls the log.", items:[{f:"core",t:"eng",label:"drive_engine(N)"},{note:"eng",text:"bind :8080",kind:"good"}], state:{eng:"serving :8080 @ N"}},
  ]});

SeqScene("seq-flowconv", {
  title:"Flow copies converging", note:"choose a scenario",
  actors:[{id:"t",label:"Test",sub:"flow f"},{id:"B",label:"Node B",sub:"owner of f",color:N.B},{id:"C",label:"Node C",sub:"successor #1",color:N.C},{id:"A",label:"Node A",sub:"successor #2",color:N.A}],
  variants:[
    {name:"A lost push, repaired", steps:[
      {caption:"B owns <code>f</code>. A request moves the scenario on; B writes <code>(M, v42, B)</code> into its shard and acknowledges at once.", items:[{f:"t",t:"B",label:"POST /pay  (flow f)"},{f:"B",t:"t",label:"202",kind:"reply"}], state:{B:"f = Paid (M, 42, B)",C:"f = Pending (M, 41, B)",A:"f = Pending (M, 41, B)"}},
      {caption:"The write is pushed to both successors, fire-and-forget. C gets it; the push to A is dropped.", items:[{f:"B",t:"C",label:"replicate (M, 42, B)"},{f:"B",t:"A",label:"replicate",kind:"lost"}], state:{C:"f = Paid (M, 42, B)"}},
      {caption:"The next request lands on A. A strong read goes to the owner, so A's stale copy is never consulted.", items:[{f:"t",t:"A",label:"GET /status  (flow f)"},{f:"A",t:"B",label:"read f"},{f:"B",t:"A",label:"Paid (M, 42, B)",kind:"reply"},{f:"A",t:"t",label:"200 Paid",kind:"ok"}]},
      {caption:"Within 5 s, A's anti-entropy pull asks the owner for the flows A holds but doesn't own, and merges by the highest <code>(m_idx, v, origin)</code>.", items:[{f:"A",t:"B",label:"sync: flows I replicate"},{f:"B",t:"A",label:"f = (M, 42, B)",kind:"reply"}], state:{A:"f = Paid (M, 42, B)"}},
      {caption:"All three copies agree again. Nothing waited on the repair: the owner's answer was right throughout, and only a <code>readConsistency: \"local\"</code> read on A could have seen the gap.", items:[{note:["B","A"],text:"converged at (M, 42, B)",kind:"good"}]},
    ]},
    {name:"The owner is lost for good (D-95)", steps:[
      {caption:"B owns <code>f</code> at membership index M. Its last write reached C, but its push to A was lost.", items:[{f:"B",t:"C",label:"replicate (M, 42, B)"},{f:"B",t:"A",label:"replicate",kind:"lost"}], state:{B:"f = Paid (M, 42, B)",C:"f = Paid (M, 42, B)",A:"f = Pending (M, 41, B)"}},
      {caption:"B's disk is destroyed. A crash is not a departure: B is still a member and still owns <code>f</code>, so requests for <code>f</code> fail fast (D-94).", items:[{note:"B",text:"✕ host lost",kind:"bad"},{f:"t",t:"C",label:"GET /status  (flow f)"},{f:"C",t:"t",label:"503 owner unreachable",kind:"bad"}], state:{B:"<span style='color:var(--bad)'>gone</span>"}},
      {caption:"The operator retires B. The leader probes B's advertised address: nothing answers as B, so it commits membership entry M′ without B.", items:[{note:["C","A"],text:"cluster remove-node B → probe: silent → commit M′",kind:"commit"}]},
      {caption:"At M′ every node computes the same ring, in which C wins <code>f</code>. On its first touch, C adopts: it pulls <code>f</code> from the other holder and keeps the highest tuple.", items:[{f:"C",t:"A",label:"pull f"},{f:"A",t:"C",label:"(M, 41, B)",kind:"reply"},{note:"C",text:"keep (M, 42, B) > (M, 41, B)",kind:"good"}], state:{C:"OWNER f = Paid (M, 42, B)"}},
      {caption:"C serves <code>f</code>. Its writes are stamped <code>m_idx = M′</code>, so anything B wrote under M loses any later comparison. What is lost for good is only what B acknowledged and never pushed to anyone.", items:[{f:"t",t:"C",label:"GET /status  (flow f)"},{f:"C",t:"t",label:"200 Paid",kind:"ok"}]},
    ]},
  ]});

SeqScene("seq-form", {
  title:"Founding a cluster and growing it to three", note:"every step is a committed entry",
  actors:[{id:"A",label:"Node A",sub:"--cluster-allow-solo",color:N.A},{id:"B",label:"Node B",sub:"--cluster-seeds A",color:N.B},{id:"C",label:"Node C",sub:"--cluster-seeds A",color:N.C},{id:"fleet",label:"Fleet"}],
  steps:[
    {caption:"A starts with an empty state dir, <code>--cluster-allow-solo</code> and no seeds. It mints its node id (from <code>--cluster-node-name</code>, or the clock), persists it, and initializes a Raft group whose membership is just <code>{A}</code>.", items:[{note:"A",text:"cluster_init: membership {A}",kind:"commit"}], state:{A:"id minted · empty log",fleet:"voters {A}"}},
    {caption:"A single voter is exempt from the restart hold, so A elects itself at once. It is a complete cluster: a majority of one is itself, so a write commits as soon as A's own fsync returns. The ring is <code>{A}</code>, and A owns every flow.", items:[{note:"A",text:"leader · term 1",kind:"good"}], state:{A:"leader · owns every flow",fleet:"voters {A} · majority 1 · survives 0"}},
    {caption:"A is already useful. Imposters created now go through the same log every later node will replay.", items:[{note:"A",text:"POST /imposters → entries 2…N",kind:"commit"}], state:{A:"leader · log …N"}},
    {caption:"B starts with an empty state dir and <code>--cluster-seeds</code> naming A. It mints its own id and sends one HMAC-signed join request with its id and advertise address. The seed is re-resolved through DNS on every attempt and retried for up to 30 s, so starting before A is up is fine.", items:[{f:"B",t:"A",label:"POST /internal/v1/cluster/join {id, advertise}"}], state:{B:"id minted · joining"}},
    {caption:"<b>Phase 1.</b> The leader commits a membership entry adding B as a <em>learner</em>. A learner receives the log but has no vote, so admitting one can never cost the cluster its quorum.", items:[{note:"A",text:"commit: add learner B",kind:"commit"}], state:{fleet:"voters {A} · learners {B}"}},
    {caption:"A starts replicating to B: the whole log by backfill, or a snapshot if the old entries have been purged. B fsyncs each batch and applies it in order.", items:[{f:"A",t:"B",label:"AppendEntries (or InstallSnapshot)"},{f:"B",t:"A",label:"ack",kind:"reply"}], state:{B:"learner · applied N"}},
    {caption:"<b>Promotion.</b> The leader waits up to 500 ms for B to be current (within 16 entries). A small fleet's joiner is current in milliseconds, so in the same call A commits <code>AddVoterIds{B}</code>: a joint configuration, then <code>{A,B}</code>.", items:[{note:"A",text:"commit: AddVoterIds {B}",kind:"commit"},{f:"A",t:"B",label:"admitted: voter",kind:"ok"}], state:{fleet:"voters {A,B} · majority 2 · survives 0"}},
    {caption:"Note the Fleet column: two voters survive no more failures than one, because a majority of two is both. Two is a transition, never a resting state. The ring is now <code>{A,B}</code>: about half the flows move to B, which adopts each from A on first touch.", items:[{note:["A","B"],text:"ring {A,B} · ~½ of flows now owned by B",kind:"warn"}], state:{A:"leader · owns ~½",B:"voter · owns ~½"}},
    {caption:"B's <code>/readyz</code> turns 200 only when it knows the leader <em>and</em> has applied up to the leader's index and bound every imposter. Only then does the load balancer route to it.", items:[{note:"B",text:"/readyz 200 → in the balancer",kind:"good"}], state:{B:"voter · ready"}},
    {caption:"C joins the same way: a learner, caught up, promoted. Three voters, majority two: <b>the fleet now survives one failure</b>. Each flow is held by its owner and two replicas, which with three voters is every node.", items:[{f:"C",t:"A",label:"join"},{note:"A",text:"commit: add learner C · AddVoterIds {C}",kind:"commit"},{f:"A",t:"C",label:"admitted: voter",kind:"ok"}], state:{C:"voter · ready",fleet:"voters {A,B,C} · majority 2 · survives 1"}},
  ]});

SeqScene("seq-proxy", {
  title:"The first proxyOnce call, cluster-wide", note:"claim at the owner, record through Raft",
  actors:[{id:"c1",label:"Client 1"},{id:"B",label:"Node B",sub:"took the request",color:N.B},{id:"C",label:"Node C",sub:"owns claim (9090, sig)",color:N.C},{id:"up",label:"Upstream",sub:"proxy.to"},{id:"A",label:"Node A",color:N.A}],
  steps:[
    {caption:"Client 1's <code>GET /orders/7</code> reaches B and matches imposter 9090's <code>proxyOnce</code> stub. Nothing is recorded for this signature yet.", items:[{f:"c1",t:"B",label:"GET /orders/7"}]},
    {caption:"B asks the claim's owner. The claim key is the port plus a hash of the request signature, so every node asks the same node: C, by HRW.", items:[{f:"B",t:"C",label:"try_claim(9090, sig)"},{f:"C",t:"B",label:"Claimed(token)",kind:"reply"}], state:{C:"sig: Pending (token, 60 s TTL)"}},
    {caption:"A concurrent first request on A also asks C, and is told the signature is in flight. A forwards it upstream too, but will not record: “once” is about the recording, not about blocking traffic.", items:[{f:"A",t:"C",label:"try_claim(9090, sig)"},{f:"C",t:"A",label:"InFlight",kind:"reply"}]},
    {caption:"B, the claim holder, calls the real service and answers client 1 with the live response.", items:[{f:"B",t:"up",label:"GET /orders/7"},{f:"up",t:"B",label:"200 {order 7}",kind:"reply"},{f:"B",t:"c1",label:"200 {order 7}",kind:"ok"}]},
    {caption:"B hands the response back to C with its token. C submits one Raft op, <code>ProxyRecorded</code>, carrying the new stub and the “recorded” marker together (D-40).", items:[{f:"B",t:"C",label:"complete(token, response)"},{note:["C","A"],text:"commit ProxyRecorded {stub, marker}",kind:"commit"}], state:{C:"sig: Recorded"}},
    {caption:"Every node applies the entry like any stub edit: the recorded stub is inserted <em>before</em> the proxy stub. From now on any node matches it first, and upstream is never called again for this signature.", items:[{note:"B",text:"stub proxy-recorded-sig",kind:"good"},{note:"A",text:"stub proxy-recorded-sig",kind:"good"}], state:{B:"recorded stub ✓",A:"recorded stub ✓"}},
  ]});

SeqScene("seq-scenario", {
  title:"A checkout scenario across three nodes", note:"flow i8080:ord-7 · owner B",
  actors:[{id:"t",label:"Test",sub:"X-Flow-Id: ord-7"},{id:"A",label:"Node A",color:N.A},{id:"B",label:"Node B",sub:"owner of the flow",color:N.B},{id:"C",label:"Node C",color:N.C}],
  variants:[
    {name:"Three requests, three nodes", steps:[
      {caption:"Request 1 lands on A. A's ring says B owns <code>i8080:ord-7</code>, so the gate's read goes to B. The <code>checkout</code> key is absent, which reads as <code>Started</code>.", items:[{f:"t",t:"A",label:"POST /cart/checkout"},{f:"A",t:"B",label:"get(ord-7, checkout)"},{f:"B",t:"A",label:"absent → Started",kind:"reply"}], state:{B:"checkout: —"}},
      {caption:"The <code>Started</code> stub matches. Its transition is a compare-and-set at the owner: expect <code>Started</code> (or absence), set <code>AwaitingPayment</code>. B applies it as version 1 and pushes it to its successors, C and A.", items:[{f:"A",t:"B",label:"CAS Started → AwaitingPayment"},{note:"B",text:"applied (M, v1, B)",kind:"commit"},{f:"B",t:"C",label:"replicate v1"},{f:"A",t:"t",label:"202 awaiting payment",kind:"ok"}], state:{B:"checkout: AwaitingPayment (v1)",C:"replica v1"}},
      {caption:"Request 2 lands on C. C holds a replica, but a gate read is strong: it goes to the owner, so a lost or late push could not mislead it.", items:[{f:"t",t:"C",label:"POST /pay"},{f:"C",t:"B",label:"get(ord-7, checkout)"},{f:"B",t:"C",label:"AwaitingPayment",kind:"reply"}]},
      {caption:"The <code>AwaitingPayment</code> stub matches and moves the flow to <code>Paid</code> with another compare-and-set: version 2.", items:[{f:"C",t:"B",label:"CAS AwaitingPayment → Paid"},{note:"B",text:"applied (M, v2, B)",kind:"commit"},{f:"C",t:"t",label:"200 paid",kind:"ok"}], state:{B:"checkout: Paid (v2)",C:"replica v2",A:"replica v2"}},
      {caption:"Request 3 lands on B itself. B is the owner, so the read happens in-process with no RPC, and the <code>Paid</code> stub answers.", items:[{f:"t",t:"B",label:"GET /order/status"},{note:"B",text:"local read: Paid",kind:"good"},{f:"B",t:"t",label:"200 shipped soon",kind:"ok"}]},
    ]},
    {name:"Two requests race", steps:[
      {caption:"Two requests for the same flow arrive at the same moment, one on A and one on C. Both gates read the owner's value: <code>Started</code>.", items:[{f:"A",t:"B",label:"get → Started"},{f:"C",t:"B",label:"get → Started"}], state:{B:"checkout: —"}},
      {caption:"Both matched the <code>Started</code> stub, and both send its transition. B serializes them under the flow's lock. A's compare-and-set arrives first and applies.", items:[{f:"A",t:"B",label:"CAS Started → AwaitingPayment"},{note:"B",text:"applied (v1)",kind:"commit"}], state:{B:"checkout: AwaitingPayment (v1)"}},
      {caption:"C's compare-and-set expected <code>Started</code>, but the state has moved. It is refused with the current value, and C <em>drops</em> its transition (logged at debug) instead of overwriting. No error reaches C's client.", items:[{f:"C",t:"B",label:"CAS Started → AwaitingPayment"},{f:"B",t:"C",label:"conflict: AwaitingPayment",kind:"reply"},{note:"C",text:"transition dropped",kind:"warn"}]},
      {caption:"Both clients got the <code>Started</code> stub's response, which is what each one's gate saw, and the state machine advanced exactly once. Last-writer-wins would have given the same result here, but it would also let a slow, stale request roll a later state back.", items:[{note:["A","C"],text:"state advanced once: AwaitingPayment",kind:"good"}]},
    ]},
  ]});

SeqScene("seq-election", {
  title:"The leader dies; a new one is elected", note:"choose a scenario",
  actors:[{id:"A",label:"Node A",sub:"leader, term 7",color:N.A},{id:"B",label:"Node B",sub:"follower",color:N.B},{id:"C",label:"Node C",sub:"follower",color:N.C}],
  variants:[
    {name:"Leader crash", steps:[
      {caption:"Steady state. Every 50 ms A sends each follower an AppendEntries carrying its vote <code>(term 7, A)</code> and its commit index, with no entries when they are caught up. Each one resets the follower's election timer.", items:[{f:"A",t:"B",label:"append {vote 7/A, commit 1042, entries: []}"},{f:"A",t:"C",label:"append {vote 7/A, commit 1042, entries: []}"},{f:"B",t:"A",label:"success",kind:"reply"},{f:"C",t:"A",label:"success",kind:"reply"}], state:{A:"leader · term 7",B:"follower · term 7 · leader A",C:"follower · term 7 · leader A"}},
      {caption:"A crashes. Nothing arrives at B or C, and nothing tells them A is gone; silence is the only signal.", items:[{note:"A",text:"✕ process dies",kind:"bad"}], state:{A:"<span style='color:var(--bad)'>down</span>"}},
      {caption:"Each follower waits its leader lease (300 ms) plus a random 150–300 ms. B drew 470 ms, C drew 560 ms, so B's timer fires first. Within about 900 ms of A's last heartbeat both B and C report themselves isolated, and owner-side flow operations pause.", items:[{note:"B",text:"timer fires at 470 ms",kind:"warn"},{note:"C",text:"still waiting (560 ms)"}], state:{B:"no leader · isolated",C:"no leader · isolated"}},
      {caption:"B becomes a candidate: term 8, vote for itself. It writes that vote to <code>raft.redb</code> with an <code>Immediate</code> commit <em>before</em> it asks anyone, so a crash now can't make it vote twice in term 8.", items:[{note:"B",text:"term 8 · vote B · fsync",kind:"commit"}], state:{B:"candidate · term 8 · voted B"}},
      {caption:"B sends RequestVote with its last log position. The send to A fails to connect; that is just a missing vote.", items:[{f:"B",t:"C",label:"vote? {term 8, B, last_log 1042@7}"},{f:"B",t:"A",label:"vote?",kind:"lost"}]},
      {caption:"C checks two things. Has its lease from A expired? Yes, it last heard A more than 300 ms ago. Is B's log at least as up to date as its own? Yes. C saves its vote for B, again with an fsync, and only then answers.", items:[{note:"C",text:"lease expired ✓ · log ok ✓ · save vote B (fsync)",kind:"commit"},{f:"C",t:"B",label:"granted",kind:"ok"}], state:{C:"follower · term 8 · voted B"}},
      {caption:"Two votes out of three is a majority. B is leader for term 8. It appends a blank entry in its own term, because a leader can only commit earlier entries once an entry of its own term commits, and starts heartbeating. C sees a leader again, and both stop reporting isolated.", items:[{note:"B",text:"leader · term 8 · blank entry 1043",kind:"good"},{f:"B",t:"C",label:"append {vote 8/B, entries: [1043]}"},{f:"C",t:"B",label:"success",kind:"reply"}], state:{B:"leader · term 8",C:"follower · term 8 · leader B"}},
    ]},
    {name:"A restarted voter must not disrupt", steps:[
      {caption:"B leads term 8. A comes back after a restart. Its disk still says term 7, leader A: its own persisted state, not news from the wire.", items:[{note:"A",text:"restart · persisted: term 7",kind:"warn"}], state:{A:"follower · term 7 · elections held",B:"leader · term 8",C:"follower · term 8"}},
      {caption:"A holds elections for up to 3 s. The current leader is not in A's metrics yet, so A releases the hold only on evidence from the wire: its vote moving, or a log entry arriving.", items:[{note:"A",text:"elect = false (restart grace)"}]},
      {caption:"B's liveness ticker has been sending to A's address every 50 ms through the probe path, which ignores the peer-health mark A's crash left behind. The first one that lands carries <code>vote 8/B</code>.", items:[{f:"B",t:"A",label:"append {vote 8/B} (probe)"},{f:"A",t:"B",label:"success",kind:"reply"}], state:{A:"follower · term 8 · leader B"}},
      {caption:"A adopts term 8 and B as leader; its vote moved, so the hold is released. Normal replication catches it up. No term was burned.", items:[{f:"B",t:"A",label:"append {entries 1040…1050}"},{note:"A",text:"caught up · no election",kind:"good"}]},
      {caption:"Without the grace and the ticker, A would campaign at about 0.5 s with term 8, then 9, and so on. C refuses it because its lease from B is fresh, and B refuses it too, so A's term climbs alone. When B's replication finally reaches A, A answers with its inflated term, B adopts the higher vote and steps down, and the whole fleet re-elects for nothing. The grace and the probe exist to prevent exactly this.", items:[{note:["A","C"],text:"what the grace prevents: a term standoff",kind:"bad"}]},
    ]},
  ]});

SeqScene("seq-heartbeat", {
  title:"One heartbeat, end to end", note:"leader A → follower B",
  actors:[{id:"core",label:"openraft on A",color:N.A},{id:"net",label:"A's network layer",sub:"PeerClient · RpcClient"},{id:"srv",label:"B's cluster port",sub:"RpcServer",color:N.B},{id:"rb",label:"openraft on B",color:N.B}],
  steps:[
    {caption:"A's 50 ms tick fires. B is caught up, so openraft builds an AppendEntries with no entries: A's vote, the log position B already matches, and the commit index.", items:[{note:"core",text:"tick → heartbeat for B",kind:"commit"},{f:"core",t:"net",label:"append_entries(rpc)"}]},
    {caption:"The network layer records that it sent something to B (so the liveness ticker stays quiet), resolves B's advertise address afresh, and serializes the request as JSON.", items:[{note:"net",text:"note_sent · resolve B · to JSON"}]},
    {caption:"It signs: a timestamp, a fresh nonce, method, path and body, HMAC-SHA256 under the cluster secret, in <code>x-rift-cluster-auth</code>; plus <code>x-rift-cluster-proto: 1.0</code>. It then POSTs over a pooled HTTP/1.1 connection.", items:[{f:"net",t:"srv",label:"POST /internal/v1/raft/append"}]},
    {caption:"B's server checks in order: protocol version, body size (32 MiB cap), then the signature (format, clock skew within 30 s, MAC in constant time, nonce not seen). Only then does it look up the route.", items:[{note:"srv",text:"proto ✓ size ✓ skew ✓ MAC ✓ nonce ✓ route ✓",kind:"good"}]},
    {caption:"openraft on B checks the vote first. It is A's, at the current term, so B refreshes its election timer and its leader lease. It then checks that its log matches at <code>prev_log_id</code>, and learns the commit index.", items:[{f:"srv",t:"rb",label:"append_entries(rpc)"},{note:"rb",text:"vote ok · timer reset · prev_log ok",kind:"good"}], state:{rb:"leader A · lease fresh"}},
    {caption:"B answers 200 with <code>Success</code>. A's client marks B healthy, and openraft updates B's match index and the time of its last quorum acknowledgement, which is what A's own isolation check reads.", items:[{f:"rb",t:"srv",label:"Success",kind:"reply"},{f:"srv",t:"net",label:"200 {Success}",kind:"reply"},{f:"net",t:"core",label:"Success",kind:"reply"}], state:{core:"B matched · quorum ack fresh"}},
  ]});

/* ---------- Wire inspector ---------- */
(function wire(){
  const fig = document.getElementById("fig-wire"); if (!fig) return;
  const $ = id => document.getElementById(id);
  const SECRET = "demo-cluster-secret";
  const MSG = {
    hb:{path:"/internal/v1/raft/append", body:{vote:{leader_id:{term:7,node_id:1},committed:true},prev_log_id:{leader_id:{term:7,node_id:1},index:1042},leader_commit:{leader_id:{term:7,node_id:1},index:1042},entries:[]}, ok:"200 · AppendEntries Success: B resets its election timer"},
    vote:{path:"/internal/v1/raft/vote", body:{vote:{leader_id:{term:8,node_id:2},committed:false},last_log_id:{leader_id:{term:7,node_id:1},index:1042}}, ok:"200 · a vote answer, granted or not; a refusal is a normal answer, not an error"},
    join:{path:"/internal/v1/cluster/join", body:{node_id:204991441056123,advertise:"rift-3.rift.svc:4790"}, ok:"200 · {admitted: true, role: \"Voter\", catching_up: false}"},
    fwd:{path:"/internal/v1/cluster/write", body:{op_id:"0b6c3f0e-…",principal:null,issued_at_secs:1790952600,expected_revision:null,op:{PutImposter:{config:{port:8080,protocol:"http",stubs:["…"]}}}}, ok:"200 · {\"ForwardTo\": {\"leader_addr\": \"rift-0.rift.svc:4790\"}}: this follower isn't the leader, so the forwarder re-sends to the address it names"},
  };
  const STAGES = [["proto","x-rift-cluster-proto"],["size","body ≤ 32 MiB"],["parse","auth header"],["skew","clock within 30 s"],["mac","HMAC matches"],["nonce","nonce unseen"],["route","route + handler"]];
  const FAIL = {
    none:null,
    body:["mac","401 · bad_mac","The MAC covers the whole body, so changing one byte anywhere in it makes the signature wrong. The request is refused before any handler sees the bytes."],
    replay:["nonce","401 · replayed_nonce","Same timestamp, same nonce, same MAC: the signature is valid, which is exactly why a replay needs its own check. The nonce was recorded when the original was accepted, and is kept for 61 s, longer than any timestamp stays inside the skew window."],
    skew:["skew","401 · stale_timestamp","The timestamp is outside ±30 s of the receiver's clock. Checked before the MAC, and before anything is recorded."],
    secret:["mac","401 · bad_mac","Without the cluster secret there is no way to produce a MAC the receiver accepts. A node from another fleet, or one with a mistyped secret, is refused on every request."],
    proto:["proto","426 · version skew","Major versions must match. This is checked before the body is even read, so an incompatible node costs one header comparison."],
    big:["size","413 · body too large","The body is capped at 32 MiB while it is being read, before authentication. For AppendEntries the leader reacts by halving its next batch."],
  };
  const enc = new TextEncoder();
  async function hmac(text, key){
    try {
      const k = await crypto.subtle.importKey("raw", enc.encode(key), {name:"HMAC", hash:"SHA-256"}, false, ["sign"]);
      const sig = new Uint8Array(await crypto.subtle.sign("HMAC", k, enc.encode(text)));
      return [...sig].map(b => b.toString(16).padStart(2,"0")).join("");
    } catch (e) { return null; }
  }
  let seq = 0;
  async function render(){
    const my = ++seq;
    const m = MSG[$("wire-msg").value], tamper = $("wire-tamper").value;
    const t = 1790952600 + (tamper === "skew" ? 45 : 0);
    const nonce = "9f2c41d07ab35e88c1d6a0f4b27e93c5";
    const body = JSON.stringify(m.body);
    // The real canonical form length-prefixes each field (u64 LE); the demo signs the same fields joined by newlines.
    const mac = await hmac([t, nonce, "POST", m.path, body].join("\n"), tamper === "secret" ? "wrong-secret" : SECRET);
    if (my !== seq) return;
    const macTxt = mac ? mac.slice(0, 32) + "…" : "(computed in the browser)";
    const shownBody = tamper === "body" ? body.replace(/1042/, '<span class="bad">1043</span>') : esc(body);
    const proto = tamper === "proto" ? '<span class="bad">2.0</span>' : "1.0";
    const len = tamper === "big" ? '<span class="bad">41943040</span>' : String(enc.encode(body).length);
    $("wire-req").innerHTML =
      `<span class="hl">POST ${esc(m.path)} HTTP/1.1</span>\nhost: 10.0.3.12:4790\ncontent-type: application/json\ncontent-length: ${len}\n` +
      `x-rift-cluster-proto: ${proto}\nx-rift-cluster-auth: t=${tamper === "skew" ? `<span class="bad">${t}</span>` : t},n=${nonce},mac=${macTxt}\n\n${shownBody}` +
      (tamper === "replay" ? `\n\n<span class="bad">(the identical request again, 5 s later)</span>` : "");
    const f = FAIL[tamper];
    const stop = f ? STAGES.findIndex(([k]) => k === f[0]) : STAGES.length;
    $("wire-pipe").innerHTML = STAGES.map(([k,l],i) => `<div class="stage ${i < stop ? "local" : i === stop ? "fail" : "off"}"><span class="zn">${i+1}</span><b>${i < stop ? "✓" : i === stop ? "✗" : "·"}</b><span class="what">${l}</span></div>`).join("");
    $("wire-why").innerHTML = f ? `<span class="stepno">${esc(f[1])}</span>${f[2]}` : `<span class="stepno">ACCEPTED</span>${esc(m.ok)}`;
  }
  fig.addEventListener("change", render);
  render();
})();

/* ---------- Route playground ---------- */
(function routes(){
  const fig = document.getElementById("fig-route"); if (!fig) return;
  const R = [
    {id:"orders-v2", priority:10, match:{host:"api.shop.test", path_prefix:"/orders", headers:[["x-api-version","2"]]}, target:{port:9092}},
    {id:"orders", priority:0, match:{host:"api.shop.test", path_prefix:"/orders"}, target:{port:9090}},
    {id:"tenants", priority:0, match:{host:"*.shop.test"}, target:{port:9100}},
    {id:"payments", priority:0, match:{path_prefix:"/payments", method:"POST"}, target:{port:9200, strip_prefix:true}},
    {id:"api", priority:0, match:{path_prefix:"/api"}, target:{port:9300, strip_prefix:true}},
  ];
  const hostRank = r => r.match.host ? (r.match.host.startsWith("*.") ? 1 : 0) : 2;
  const order = R.slice().sort((a,b) => (b.priority - a.priority) || (hostRank(a) - hostRank(b))
    || ((b.match.path_prefix||"").length - (a.match.path_prefix||"").length)
    || ((b.match.headers||[]).length - (a.match.headers||[]).length) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const subdomain = (host, suffix) => { const cut = host.length - suffix.length; if (cut < 2) return false; return host.slice(cut).toLowerCase() === suffix.toLowerCase() && host[cut-1] === "."; };
  const prefixOk = (path, prefix) => { const p = prefix.replace(/\/+$/, ""); if (!path.startsWith(p)) return false; const rest = path.slice(p.length); return rest === "" || rest.startsWith("/"); };
  const presets = [
    ["v2 client", {host:"api.shop.test", method:"GET", path:"/orders/7", ver:"2"}],
    ["v1 client", {host:"api.shop.test:8443", method:"GET", path:"/orders/7", ver:"1"}],
    ["tenant host", {host:"acme.shop.test", method:"GET", path:"/orders/7", ver:""}],
    ["bare domain", {host:"shop.test", method:"GET", path:"/orders/7", ver:""}],
    ["payment", {host:"pay.example", method:"POST", path:"/payments/charge?id=9", ver:""}],
    ["prefix trap", {host:"pay.example", method:"GET", path:"/ordersX", ver:""}],
    ["gateway", {host:"pay.example", method:"GET", path:"/__rift/9090/orders/7", ver:""}],
  ];
  const $ = id => document.getElementById(id);
  document.getElementById("route-presets").innerHTML = `<span class="fig-note">Try:</span>` + presets.map(([n],k) => `<button type="button" data-p="${k}">${n}</button>`).join("");
  const cell = (st, txt) => `<td class="${st}">${st === "ok" ? "✓ " : st === "no" ? "✗ " : ""}${esc(txt)}</td>`;
  function render(){
    const rawHost = $("rt-host").value.trim(), method = $("rt-method").value, full = $("rt-path").value.trim() || "/", ver = $("rt-ver").value;
    const host = rawHost.replace(/:\d+$/, "");
    const [path, query] = full.split("?");
    let winner = null, rows = "";
    order.forEach(r => {
      const m = r.match;
      const h = !m.host ? ["na","any"] : (m.host.startsWith("*.") ? subdomain(host, m.host.slice(2)) : host.toLowerCase() === m.host.toLowerCase()) ? ["ok", m.host] : ["no", m.host];
      const me = !m.method ? ["na","any"] : method === m.method ? ["ok", m.method] : ["no", m.method];
      const p = !m.path_prefix ? ["na","any"] : prefixOk(path, m.path_prefix) ? ["ok", m.path_prefix] : ["no", m.path_prefix];
      const hd = !(m.headers||[]).length ? ["na","—"] : m.headers.every(([k,v]) => k === "x-api-version" && ver === v) ? ["ok", m.headers.map(([k,v]) => `${k}: ${v}`).join(", ")] : ["no", m.headers.map(([k,v]) => `${k}: ${v}`).join(", ")];
      const ok = [h,me,p,hd].every(c => c[0] !== "no");
      const isWin = ok && !winner; if (isWin) winner = r;
      rows += `<tr class="${isWin ? "win" : winner && !isWin ? "skip" : ""}"><td class="role">${esc(r.id)}</td><td>${r.priority}</td>${cell(...h)}${cell(...me)}${cell(...p)}${cell(...hd)}<td>:${r.target.port}${r.target.strip_prefix ? " · strip" : ""}</td></tr>`;
    });
    $("rt-table").innerHTML = `<thead><tr><th>route (in effective order)</th><th>prio</th><th>host</th><th>method</th><th>path prefix</th><th>headers</th><th>target</th></tr></thead><tbody>${rows}</tbody>`;
    const q = query !== undefined ? "?" + query : "";
    let out, why;
    if (winner){
      let fwd = path;
      if (winner.target.strip_prefix){ const p = winner.match.path_prefix.replace(/\/+$/, ""); fwd = path.slice(p.length) || "/"; }
      out = `→ imposter :${winner.target.port}, path <code>${esc(fwd + q)}</code>`;
      why = `<span class="stepno">ROUTE ${esc(winner.id).toUpperCase()}</span>The first route in effective order whose every clause matches. ${winner.target.strip_prefix ? "Its target strips the matched prefix before dispatch." : "The path is passed through unchanged."} The request is then dispatched in-process to the imposter on this node, where the flow id and stub matching take over (<a href='requests.html#read'>section 7</a>).`;
    } else if (path.startsWith("/__rift/")){
      const m = path.match(/^\/__rift\/(\d+)(\/.*)?$/);
      out = m ? `→ gateway: imposter :${m[1]}, path <code>${esc((m[2] || "/") + q)}</code>` : "→ gateway: 404 (no port in the path)";
      why = `<span class="stepno">NO ROUTE · GATEWAY</span>No route matched, but the path is gateway-addressed, so the port comes from the path and the rest is passed to that imposter.`;
    } else {
      out = `→ <span style="color:var(--bad)">404 · x-rift-front-door: no-route</span>`;
      why = `<span class="stepno">NO ROUTE</span>Nothing matched and the path isn't gateway-addressed. The header tells this apart from an imposter that answered 404 itself.`;
    }
    if (rawHost !== host) why += ` The port in the Host header (<code>${esc(rawHost.slice(host.length))}</code>) is ignored for matching.`;
    $("rt-out").innerHTML = out; $("rt-why").innerHTML = why;
  }
  fig.addEventListener("input", render);
  fig.addEventListener("change", render);
  fig.addEventListener("click", e => {
    const b = e.target.closest("button[data-p]"); if (!b) return;
    const p = presets[+b.dataset.p][1];
    $("rt-host").value = p.host; $("rt-method").value = p.method; $("rt-path").value = p.path; $("rt-ver").value = p.ver; render();
  });
  render();
})();

/* ---------- Flow writer, live ---------- */
(function wal(){
  const fig = document.getElementById("fig-wal"); if (!fig) return;
  const svg = fig.querySelector("svg"), modesEl = document.getElementById("wal-modes"), rateIn = document.getElementById("wal-rate");
  const runBtn = fig.querySelector("[data-a=run]"), plugBtn = fig.querySelector("[data-a=plug]");
  const MODES = {sync:"sync", async:"async (default)", none:"none"};
  const TICK = 50, WIN = 300, SPEED = 0.25, FSYNC = 2, COMMIT = 0.2, STEP = 0.1, BATCH = 256;
  let mode = "async", t, writes, fsyncs, nextTick, nextArr, busyUntil, inflight, running = !reduce, plugged, lost, seed;
  const rand = () => (seed = (seed*1103515245 + 12345) % 2147483648) / 2147483648;
  modesEl.innerHTML = Object.entries(MODES).map(([k,l]) => `<button type="button" data-k="${k}" aria-pressed="${k===mode}">${l}</button>`).join("");
  function reset(){ t = 0; writes = []; fsyncs = []; nextTick = TICK; nextArr = 0; busyUntil = 0; inflight = null; plugged = false; lost = 0; seed = 11; }
  function step(now){
    const rate = +rateIn.value / 1000;
    while (nextArr <= now){
      const w = {t: nextArr, ack: null, cached: null, durable: null, queued: mode !== "none"};
      if (mode !== "sync") w.ack = nextArr;
      writes.push(w);
      nextArr += -Math.log(1 - rand()) / rate;
    }
    if (mode === "none") return;
    if (inflight && now >= busyUntil){
      if (inflight.kind === "imm"){
        inflight.ws.forEach(w => { w.durable = busyUntil; if (w.ack === null) w.ack = busyUntil; if (w.cached === null) w.cached = busyUntil; });
        fsyncs.push(busyUntil);
      } else inflight.ws.forEach(w => { w.cached = busyUntil; });
      inflight = null;
    }
    if (inflight) return;
    if (mode === "async" && now >= nextTick){
      const covered = writes.filter(w => w.cached !== null && w.durable === null);
      inflight = {kind:"imm", ws:covered}; busyUntil = now + FSYNC;
      nextTick += TICK; return;
    }
    const queue = writes.filter(w => w.queued); if (!queue.length) return;
    const batch = queue.slice(0, BATCH); batch.forEach(w => { w.queued = false; });
    if (mode === "sync"){ inflight = {kind:"imm", ws:batch}; busyUntil = now + FSYNC; }
    else { inflight = {kind:"none", ws:batch}; busyUntil = now + COMMIT; }
  }
  const X0 = 30, X1 = 740, x = tm => X0 + (X1-X0)*(tm - (t - WIN))/WIN;
  function render(){
    document.getElementById("wal-rate-lab").textContent = rateIn.value + "/s";
    let g = `<line x1="${X0}" y1="168" x2="${X1}" y2="168" stroke="var(--line)"/>`;
    g += `<text x="${X0}" y="16" font-size="11" class="mono" style="fill:var(--muted)">acknowledged writes</text>`;
    g += `<text x="${X0}" y="96" font-size="11" class="mono" style="fill:var(--muted)">committed at None (page cache)</text>`;
    g += `<text x="${X0}" y="134" font-size="11" class="mono" style="fill:var(--muted)">fsynced (Immediate commit)</text>`;
    for (let k = Math.ceil((t-WIN)/50)*50; k <= t; k += 50){ if (k < 0) continue; g += `<line x1="${x(k)}" y1="164" x2="${x(k)}" y2="172" stroke="var(--muted)"/><text x="${x(k)}" y="186" text-anchor="middle" font-size="10" class="mono" style="fill:var(--muted)">${k} ms</text>`; }
    fsyncs.filter(f => f >= t-WIN).forEach(f => { g += `<line x1="${x(f)}" y1="24" x2="${x(f)}" y2="150" stroke="var(--ok)" stroke-width="1.6" opacity=".8"/>`; });
    writes.filter(w => w.t >= t-WIN).forEach(w => {
      if (w.ack !== null){
        const col = mode === "none" ? "var(--muted)" : w.durable !== null ? "var(--ok)" : "var(--bad)";
        g += `<rect x="${x(w.t)-1.5}" y="26" width="3" height="40" rx="1" fill="${col}"/>`;
      } else g += `<rect x="${x(w.t)-1.5}" y="44" width="3" height="22" rx="1" fill="var(--faint)" stroke="var(--line)"/>`;
      if (w.cached !== null && w.cached >= t-WIN) g += `<rect x="${x(w.cached)-1}" y="102" width="2" height="12" fill="var(--accent)" opacity=".7"/>`;
    });
    if (plugged) g += `<line x1="${x(t)}" y1="20" x2="${x(t)}" y2="168" stroke="var(--bad)" stroke-width="2.5"/><text x="${x(t)-6}" y="160" text-anchor="end" font-size="11" font-weight="700" style="fill:var(--bad)">⚡ power cut</text>`;
    svg.innerHTML = g;
    const lag = mode === "none" ? null : writes.filter(w => w.ack !== null && w.durable === null).length;
    document.getElementById("wal-lag").textContent = lag === null ? "n/a" : lag;
    const fs = fsyncs.filter(f => f > t-1000).length * (t >= 1000 ? 1 : 1000/Math.max(t,1));
    document.getElementById("wal-fs").textContent = mode === "none" ? "0" : Math.round(fs);
    document.getElementById("wal-ack").textContent = {sync:"an fsync (~2 ms)", async:"a channel send", none:"nothing"}[mode];
    const T = {
      sync:"Every acknowledgement waits for an <code>Immediate</code> commit. The writer still batches: whatever arrived during one fsync goes into the next transaction, so fsyncs per second level off while each ack pays disk latency. Grey bars are writes still waiting for theirs.",
      async:"The caller's ack is one channel send. The writer commits batches at <code>None</code> as fast as they come, and a ticker issues one <code>Immediate</code> commit every 50 ms that makes everything before it durable. Raise the write rate: fsyncs per second don't move. The red bars are the WAL lag.",
      none:"<code>flow.redb</code> is never touched. The value lives in memory here and on the two replicas, which also keep it in memory, because the origin's mode travels with the replication.",
    };
    let text = T[mode];
    if (plugged){
      text = mode === "sync" ? "<b>Power cut: nothing acknowledged is lost.</b> Every ack waited for its fsync; writes still waiting for theirs were never acknowledged, so their callers saw an error, not a success."
        : mode === "async" ? `<b>Power cut: ${lost} acknowledged write${lost===1?"":"s"} lost from this disk</b>, exactly the gauge at the moment of the cut. The two replicas got the same writes by push and run their own tickers, so the cluster loses them only if all three holders lose power inside the same 50 ms.`
        : "<b>Power cut: every flow value this node held is gone</b>, as the imposter chose. Its replicas still hold the values in memory unless they lose power too.";
    }
    document.getElementById("wal-text").innerHTML = text;
    modesEl.querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.k === mode)));
    runBtn.textContent = plugged ? "Power on" : running ? "Pause" : "Run";
    plugBtn.disabled = plugged;
  }
  let last = null;
  function frame(ts){
    if (running && !plugged){
      if (last !== null){
        const target = t + Math.min(ts - last, 100) * SPEED;
        while (t < target){ t += STEP; step(t); }
        writes = writes.filter(w => w.t >= t - WIN - 120 || w.durable === null && mode !== "none");
        fsyncs = fsyncs.filter(f => f > t - 1200);
      }
      last = ts; render();
    } else last = null;
    requestAnimationFrame(frame);
  }
  modesEl.addEventListener("click", e => { const b = e.target.closest("button"); if (b){ mode = b.dataset.k; reset(); render(); } });
  rateIn.addEventListener("input", render);
  runBtn.addEventListener("click", () => { if (plugged){ reset(); running = true; } else running = !running; render(); });
  plugBtn.addEventListener("click", () => {
    plugged = true;
    lost = mode === "sync" ? 0 : writes.filter(w => w.ack !== null && (mode === "none" || w.durable === null)).length;
    render();
  });
  reset();
  if (reduce){ for (let k = 0; k < 2500; k++){ t += STEP; step(t); } }
  render();
  requestAnimationFrame(frame);
})();

/* ---------- Convergence, live ---------- */
(function conv(){
  const fig = document.getElementById("fig-conv"); if (!fig) return;
  const svg = fig.querySelector("svg"), evEl = fig.querySelector(".events");
  const SNAP = 20, KEEP = 6, TICKMS = 150, BARRIER_TICKS = 13, BATCH = 4, SHOW = 26;
  const COL = {A:N.A, B:N.B, C:N.C, D:N.D, E:N.E, F:"var(--muted)"};
  let nodes, L, S, purged, pending, tick, timer, commit = 3;
  const ev = (cls, msg) => {
    const d = document.createElement("div");
    d.innerHTML = `<span class="t">${(tick*TICKMS/1000).toFixed(1)}s</span> <span class="${cls}">${msg}</span>`;
    evEl.prepend(d); while (evEl.children.length > 60) evEl.lastChild.remove();
  };
  function reset(){
    nodes = ["A","B","C","D","E"].map(id => ({id, up:true, slow:false, voter:true, member:true, log:3, applied:3, snap:0, slowAcc:0}));
    nodes.push({id:"F", up:false, slow:false, voter:false, member:false, log:0, applied:0, snap:0, slowAcc:0});
    L = 3; S = 0; purged = 0; pending = []; tick = 0; commit = 3; evEl.innerHTML = "";
    ev("ev-acc", "A leads a five-voter fleet; every member has applied index 3.");
    sync();
  }
  const leader = () => nodes[0];
  const members = () => nodes.filter(n => n.member);
  const voters = () => nodes.filter(n => n.member && n.voter);
  function commitIndex(){
    const vs = voters(), need = Math.floor(vs.length/2) + 1;
    const logs = vs.filter(n => n.up).map(n => n.log).sort((a,b) => b-a);
    return logs.length >= need ? logs[need-1] : 0;
  }
  function write(){
    const need = Math.floor(voters().length/2) + 1;
    if (voters().filter(n => n.up).length < need){ ev("ev-bad","503: no quorum, the intent stays parked"); return; }
    L += 1; leader().log = L;
    pending.push({n:L, start:tick});
  }
  function step(){
    tick++;
    nodes.forEach(n => {
      if (n === leader() || !n.member || !n.up) return;
      if (n.log >= L) return;
      if (n.log < purged){
        n.log = S; n.applied = S; n.snap = S;
        ev("ev-warn", `${n.id} needs entries the leader has purged: installs snapshot @${S}`);
      } else n.log = Math.min(L, n.log + BATCH);
    });
    commit = Math.max(commit, commitIndex());
    nodes.forEach(n => {
      if (!n.member || !n.up) return;
      const target = Math.min(commit, n.log);
      if (n.slow){ n.slowAcc += 0.05; while (n.slowAcc >= 1 && n.applied < target){ n.applied++; n.slowAcc -= 1; } }
      else n.applied = target;
    });
    const la = leader().applied;
    if (la - S >= SNAP){
      S = Math.floor(la / SNAP) * SNAP; purged = Math.max(0, S - KEEP);
      nodes.forEach(n => { if (n.member && n.up && n.applied >= S) n.snap = S; });
      ev("ev-warn", `Members snapshot @${S}; the leader purges entries ≤ ${purged}`);
    }
    const f = nodes[5];
    if (f.member && !f.voter && f.up && L - f.log <= 3 && voters().length < 9){
      f.voter = true; ev("ev-ok", "Promotion sweep: F is caught up, promoted to voter");
    }
    pending = pending.filter(p => {
      if (p.n > commit) return true;
      const behind = members().filter(n => n.applied < p.n).map(n => n.id);
      const waited = tick - p.start;
      if (!behind.length){ ev("ev-ok", `write ${p.n}: 201 (every member applied, ${(waited*TICKMS/1000).toFixed(2)} s)`); return false; }
      if (waited >= BARRIER_TICKS){ ev("ev-warn", `write ${p.n}: 201 + Rift-Cluster-Warnings: unapplied=${behind.join(",")}`); return false; }
      return true;
    });
    sync();
  }
  function sync(){
    const lo = Math.max(1, L - SHOW + 1), sq = 15, x0 = 118, rowH = 44;
    let g = "";
    g += `<text x="${x0}" y="16" font-size="10.5" class="mono" style="fill:var(--muted)">log index →  (commit = ${commit}, last snapshot = ${S})</text>`;
    nodes.forEach((n, r) => {
      const y = 32 + r*rowH, col = COL[n.id];
      const status = !n.member ? "not a member" : !n.up ? "crashed" : n === leader() ? "leader" : n.voter ? "voter" : "learner";
      g += `<g opacity="${n.member ? (n.up ? 1 : .45) : .3}">`;
      g += `<circle cx="16" cy="${y+8}" r="8" fill="${col}"/><text x="31" y="${y+12}" font-size="13" font-weight="700">${n.id}</text>`;
      g += `<text x="48" y="${y+12}" font-size="10" class="mono" style="fill:var(--muted)">${status}${n.slow ? " · slow" : ""}</text>`;
      for (let k = lo; k <= L; k++){
        const xx = x0 + (k-lo)*(sq+3);
        const inLog = n.log >= k;
        if (k <= n.snap && n.snap) { g += `<rect x="${xx}" y="${y}" width="${sq}" height="${sq}" rx="3" fill="var(--warn)" opacity="${n.applied >= k ? .85 : .4}"/>`; continue; }
        if (n.applied >= k) g += `<rect x="${xx}" y="${y}" width="${sq}" height="${sq}" rx="3" fill="var(--accent)"/>`;
        else if (inLog) g += `<rect x="${xx+.75}" y="${y+.75}" width="${sq-1.5}" height="${sq-1.5}" rx="3" fill="none" stroke="var(--accent)" stroke-width="1.5"/>`;
        else g += `<rect x="${xx}" y="${y}" width="${sq}" height="${sq}" rx="3" fill="var(--faint)"/>`;
      }
      g += `<text x="${x0}" y="${y+30}" font-size="9.5" class="mono" style="fill:var(--muted)">log ${n.log} · applied ${n.applied}</text>`;
      g += `</g>`;
    });
    if (commit >= lo){ const cx = x0 + (commit-lo)*(sq+3) + sq + 1.5; g += `<line x1="${cx}" y1="24" x2="${cx}" y2="${32 + 6*rowH - 10}" stroke="var(--ok)" stroke-width="1.5" stroke-dasharray="4 3"/><text x="${cx+3}" y="${32 + 6*rowH - 4}" font-size="9.5" class="mono" style="fill:var(--ok)">commit</text>`; }
    svg.setAttribute("viewBox", `0 0 640 ${32 + 6*rowH + 6}`);
    svg.innerHTML = g;
    const D = nodes[3], F = nodes[5];
    fig.querySelector("[data-a=crash]").textContent = D.up ? "Crash D" : "Restart D";
    fig.querySelector("[data-a=crash]").setAttribute("aria-pressed", String(!D.up));
    fig.querySelector("[data-a=slow]").setAttribute("aria-pressed", String(nodes[2].slow));
    fig.querySelector("[data-a=join]").disabled = F.member;
  }
  fig.addEventListener("click", e => {
    const a = e.target.closest("button")?.dataset.a; if (!a) return;
    if (a === "write"){ write(); ev("ev-acc", `A appends entry ${L}`); }
    if (a === "burst"){ for (let k = 0; k < 25; k++) write(); ev("ev-acc", `A appends entries up to ${L}`); }
    if (a === "slow"){ const C = nodes[2]; C.slow = !C.slow; ev(C.slow ? "ev-warn" : "ev-ok", C.slow ? "C's apply slows: its log keeps up, its applied index doesn't" : "C's apply is back to normal"); }
    if (a === "crash"){ const D = nodes[3]; D.up = !D.up; ev(D.up ? "ev-ok" : "ev-bad", D.up ? `D restarts at log ${D.log}; the leader resumes sending from there` : "D crashes. Still a member: it counts in the quorum and in the barrier"); }
    if (a === "join"){ const F = nodes[5]; F.member = true; F.up = true; ev("ev-acc", "F joins: the leader commits it as a learner (empty log)"); }
    if (a === "reset"){ reset(); return; }
    sync();
  });
  reset();
  timer = setInterval(step, TICKMS);
})();

/* ---------- Durability timeline ---------- */
(function dur(){
  if (!document.getElementById("fig-dur")) return;
  const fig = document.getElementById("fig-dur"), svg = fig.querySelector("svg"), modesEl = document.getElementById("dur-modes"), crashIn = document.getElementById("dur-crash");
  const MODES = {
    sync:{label:"sync", d:"fsync before every ack"},
    async:{label:"async (default)", d:"group fsync every 50 ms"},
    none:{label:"none", d:"memory only"},
  };
  let mode = "async";
  // deterministic write times (ms) via a small LCG
  let seed = 7; const rand = () => (seed = (seed*1103515245 + 12345) % 2147483648) / 2147483648;
  const WR = []; for (let t = 4; t < 250; t += 4 + rand()*13) WR.push(+t.toFixed(1));
  modesEl.innerHTML = Object.entries(MODES).map(([k,m]) => `<button type="button" data-k="${k}" aria-pressed="${k===mode}">${m.label}</button>`).join("") + `<span class="fig-note" id="dur-mdesc"></span>`;
  const X0 = 30, X1 = 740, x = t => X0 + (X1-X0)*t/250;
  function render(){
    const crash = +crashIn.value; document.getElementById("dur-crash-lab").textContent = crash + " ms";
    document.getElementById("dur-mdesc").textContent = MODES[mode].d;
    const fsyncs = mode === "async" ? [50,100,150,200] : [];
    let g = `<line x1="${X0}" y1="150" x2="${X1}" y2="150" stroke="var(--line)"/>`;
    for (let t = 0; t <= 250; t += 50) g += `<line x1="${x(t)}" y1="146" x2="${x(t)}" y2="154" stroke="var(--muted)"/><text x="${x(t)}" y="170" text-anchor="middle" font-size="10" class="mono" style="fill:var(--muted)">${t} ms</text>`;
    g += `<text x="${X0}" y="22" font-size="11" class="mono" style="fill:var(--muted)">acknowledged CAS writes (owner + 2 replicas)</text>`;
    g += `<text x="${X0}" y="112" font-size="11" class="mono" style="fill:var(--muted)">on disk</text>`;
    fsyncs.forEach(t => { g += `<line x1="${x(t)}" y1="34" x2="${x(t)}" y2="140" stroke="${t <= crash ? "var(--ok)" : "var(--line)"}" stroke-width="2"/><text x="${x(t)+3}" y="138" font-size="9.5" class="mono" style="fill:${t <= crash ? "var(--ok)" : "var(--muted)"}">fsync</text>`; });
    let acked = 0, lost = 0;
    WR.forEach(t => {
      if (t > crash){ g += `<rect x="${x(t)-2}" y="40" width="4" height="34" rx="1.5" fill="var(--faint)"/>`; return; }
      acked++;
      const durable = mode === "sync" ? true : mode === "none" ? false : fsyncs.some(f => f >= t && f <= crash);
      if (!durable) lost++;
      g += `<rect x="${x(t)-2}" y="40" width="4" height="34" rx="1.5" fill="${durable ? "var(--ok)" : "var(--bad)"}"/>`;
      if (durable) g += `<rect x="${x(t)-2}" y="92" width="4" height="10" rx="1.5" fill="var(--ok)" opacity=".55"/>`;
    });
    if (mode === "async"){ const lastF = Math.max(0, ...fsyncs.filter(f => f <= crash)); g += `<rect x="${x(lastF)}" y="34" width="${x(crash)-x(lastF)}" height="46" fill="var(--bad-soft)" opacity=".7"/>`; }
    g += `<line x1="${x(crash)}" y1="28" x2="${x(crash)}" y2="150" stroke="var(--bad)" stroke-width="2.5"/><text x="${x(crash)}" y="20" text-anchor="middle" font-size="11" font-weight="700" style="fill:var(--bad)">⚡ all 3 holders die</text>`;
    svg.innerHTML = g;
    document.getElementById("dur-acked").textContent = acked;
    document.getElementById("dur-lost").textContent = lost;
    const T = {
      sync:"Every compare-and-set waits for an fsync before it is acknowledged. Zero loss, and the price is disk latency on every stateful write.",
      async:`Writes commit in order without fsync, and one group fsync runs every 50 ms. Only the writes since the last fsync are at risk (shaded), and only because <em>all three</em> holders died inside that window. Lose one or two nodes and the replicas still have everything. Hot-path cost: one channel send.`,
      none:"Disk is bypassed. Losing nodes one at a time is survivable through replicas, but a full-cluster crash loses everything. That's the point: throwaway CI imposters that opted in.",
    };
    document.getElementById("dur-text").innerHTML = T[mode];
    modesEl.querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.k === mode)));
  }
  modesEl.addEventListener("click", e => { const b = e.target.closest("button"); if (b){ mode = b.dataset.k; render(); } });
  crashIn.addEventListener("input", render);
  render();
})();

/* ---------- Lifecycle ---------- */
(function life(){
  if (!document.getElementById("fig-life")) return;
  const S = [
    ["Discovering","Started with <code>--cluster-seeds</code>. Seeds are re-resolved through DNS on <em>every</em> attempt, because pod IPs churn and a cached-IP join loop after a full restart would brick the fleet. Bootstrap is explicit: exactly one node, once, starts with <code>--cluster-allow-solo</code> and no seeds. A node with peers configured never forms its own group, which rules out split-brain-on-blip by construction."],
    ["Learner","The joining node calls the leader over the HMAC-signed cluster port. The leader commits a membership entry adding it as a learner and answers <code>admitted</code> at once, with a catch-up estimate. The joiner never waits out its own catch-up inside an RPC deadline."],
    ["Catching up","Snapshot install plus log replay. <code>/readyz</code> stays non-200 until the applied index reaches the leader's commit index observed at join and its imposters are bound or reported, so the balancer never routes to a node serving yesterday's config."],
    ["Voter","The leader's promotion sweep (1 s cadence) makes a caught-up learner a voter, while there are fewer than 9 voters. Past 9, nodes stay learners: full data-plane citizens with no election weight. A promotion only ever adds voter ids; it never evicts one (D-27)."],
    ["Leaving","SIGTERM: drain readiness, leave the membership, and let flow ownership move with the committed entry (no pre-leave flush; every write was already replicated). The leader refuses a departure that would leave fewer than two voters (D-25). A node that departed writes a <code>departed</code> marker that decides resume, rejoin or bootstrap on its next start; the state directory is never wiped to force a clean join (D-26)."],
    ["Crashed","A crash is not a departure. The voter stays in the membership and keeps owning its flows, which answer <code>503</code> meanwhile (D-94); peers elect a new leader if needed, within about a second. It restarts with the same node id (persisted in the state dir), gets a 3 s restart grace before campaigning, replays its log, and reopens its flow shard."],
    ["Retired","For a crashed member that will never return. The operator runs <code>cluster remove-node &lt;id&gt; --via &lt;any live member&gt;</code> with the cluster secret; the admin key can't reach it and the console doesn't offer it. The leader confirms it still leads, then probes the member's advertised address and refuses while anything there answers as that node, or can't say who it is. Silence, or a different node id at a reused address, lets the removal commit like a graceful leave (D-95). If the retired node ever does come back on its old disk, it asks its peers on start, sees a newer membership without itself, and rejoins as a learner instead of campaigning."],
  ];
  const el = document.getElementById("life-states"), det = document.getElementById("life-detail");
  let cur = 0;
  function render(){
    el.innerHTML = S.map(([n],k) => (k && k < 5 || k === 6 ? `<span class="ar">→</span>` : k === 5 ? `<span class="ar" style="margin-left:10px">or</span>` : "") + `<button type="button" class="st" data-k="${k}" aria-pressed="${k===cur}">${n}</button>`).join("");
    det.innerHTML = `<span class="stepno">${S[cur][0].toUpperCase()}</span>${S[cur][1]}`;
  }
  el.addEventListener("click", e => { const b = e.target.closest("button"); if (b){ cur = +b.dataset.k; render(); } });
  render();
})();
})();
