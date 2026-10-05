"""HTML-дашборд сервера заданий.

Одностраничный HTML без внешних библиотек и сети: стили/JS инлайн.
Опрашивает /api/status и /api/events каждые 3 с. Данных в себе не содержит.
"""

HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Tile Downloader — дашборд</title>
<style>
  :root { --bg:#0f1115; --card:#1a1d24; --fg:#e6e6e6; --muted:#8a93a2; --bar:#3b82f6; --done:#22c55e; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:system-ui,'Segoe UI',Roboto,sans-serif; background:var(--bg); color:var(--fg); }
  .wrap { max-width:1000px; margin:0 auto; padding:20px; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:var(--muted); font-size:13px; margin-bottom:18px; }
  .cards { display:flex; gap:12px; flex-wrap:wrap; margin-bottom:18px; }
  .card { background:var(--card); border-radius:10px; padding:14px 16px; min-width:120px; flex:1; }
  .card .label { color:var(--muted); font-size:12px; }
  .card .value { font-size:22px; font-weight:600; margin-top:4px; }
  section { background:var(--card); border-radius:10px; padding:16px; margin-bottom:18px; }
  section h2 { font-size:14px; margin:0 0 12px; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; }
  .zoom-row { margin-bottom:10px; }
  .zoom-head { display:flex; justify-content:space-between; font-size:13px; margin-bottom:4px; gap:8px; }
  .zoom-head .pct { font-weight:600; }
  .bar { height:10px; background:#2a2f3a; border-radius:6px; overflow:hidden; }
  .bar > div { height:100%; background:var(--bar); transition:width .3s; }
  .bar > div.done { background:var(--done); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid #2a2f3a; }
  th { color:var(--muted); font-weight:500; }
  .events { font-family:ui-monospace,Consolas,monospace; font-size:12px; max-height:240px; overflow:auto; }
  .events div { padding:3px 0; border-bottom:1px solid #232833; }
  .events .ts { color:var(--muted); margin-right:8px; }
  .events .kind { display:inline-block; min-width:92px; color:var(--bar); }
  .tag-done { color:var(--done); font-size:11px; margin-left:6px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>Tile Downloader — дашборд</h1>
  <div class="sub" id="updated">загрузка…</div>
  <div class="cards">
    <div class="card"><div class="label">Активный зум</div><div class="value" id="active-zoom">–</div></div>
    <div class="card"><div class="label">Готово</div><div class="value" id="done">–</div></div>
    <div class="card"><div class="label">Скорость (tiles/s)</div><div class="value" id="rate">–</div></div>
    <div class="card"><div class="label">Активных клиентов</div><div class="value" id="clients">–</div></div>
    <div class="card"><div class="label">Скачано всего</div><div class="value" id="total">–</div></div>
  </div>
  <section>
    <h2>Слои</h2>
    <div id="zooms"></div>
  </section>
  <section>
    <h2>Клиенты</h2>
    <table id="clients-table">
      <thead><tr><th>ID</th><th>Задач</th><th>Last seen</th><th>Сдано</th><th>tiles/s</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>
  <section>
    <h2>События</h2>
    <div class="events" id="events"></div>
  </section>
</div>
<script>
function fmtTime(ts){ try{ return new Date(ts*1000).toLocaleTimeString(); }catch(e){ return ts; } }
function fmtN(n){ return Number(n).toLocaleString('ru-RU'); }
async function refresh(){
  try {
    const st = await (await fetch('/api/status?window=300')).json();
    const ev = await (await fetch('/api/events?limit=50')).json();
    document.getElementById('active-zoom').textContent = (st.active_zoom == null ? '–' : st.active_zoom);
    document.getElementById('done').textContent = st.done ? 'да' : 'нет';
    document.getElementById('rate').textContent = st.global.rate_tps.toFixed(1);
    document.getElementById('clients').textContent = st.global.active_clients;
    document.getElementById('total').textContent = fmtN(st.global.downloaded_tiles);
    document.getElementById('updated').textContent = 'обновлено ' + new Date().toLocaleTimeString();
    const zooms = document.getElementById('zooms');
    zooms.innerHTML = '';
    for (const z of st.zooms) {
      const row = document.createElement('div');
      row.className = 'zoom-row';
      row.innerHTML =
        '<div class="zoom-head"><span>z' + z.zoom +
        (z.done ? ' <span class="tag-done">done</span>' : '') +
        '</span><span>' + fmtN(z.downloaded) + ' / ' + fmtN(z.total) +
        ' · задач: ' + z.active_tasks + ' · <span class="pct">' + z.percent.toFixed(2) + '%</span></span></div>' +
        '<div class="bar"><div class="' + (z.done ? 'done' : '') + '" style="width:' + z.percent + '%"></div></div>';
      zooms.appendChild(row);
    }
    const tb = document.querySelector('#clients-table tbody');
    tb.innerHTML = '';
    for (const c of st.clients) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td>' + c.client_id + '</td><td>' + c.active_tasks + '</td><td>' +
        fmtTime(c.last_seen) + '</td><td>' + fmtN(c.submitted_tiles) + '</td><td>' + c.rate_tps.toFixed(1) + '</td>';
      tb.appendChild(tr);
    }
    const evEl = document.getElementById('events');
    evEl.innerHTML = '';
    for (const e of (ev.events || [])) {
      const d = document.createElement('div');
      d.innerHTML = '<span class="ts">' + fmtTime(e.ts) + '</span><span class="kind">' + e.kind +
        '</span> z' + e.zoom + (e.client_id ? (' ' + e.client_id) : '') + (e.detail ? (' · ' + e.detail) : '');
      evEl.appendChild(d);
    }
  } catch (e) {
    document.getElementById('updated').textContent = 'ошибка: ' + e;
  }
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""
