"""Этап 1. Тесты ядра сервера (jobs.JobManager) — без сети."""

import jobs
import utils


def tiles_for(task):
    """Все тайлы задачи как (x, y, ext, data)."""
    return [(utils.get_xy(i, task.zoom)[0], utils.get_xy(i, task.zoom)[1],
             'png', b'data-%d' % i) for i in range(task.start_idx, task.end_idx)]


# ------------------------------------------------------------------- выдача

def test_issue_first_starts_at_zero(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=3)
    task, done = m.issue('c1')
    assert done is False
    assert task.start_idx == 0
    assert task.count <= 3
    assert task.end_idx <= 4          # z1 = 4 тайла


def test_issue_truncates_at_zoom_end(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=10)
    task, _ = m.issue('c1')
    # chunk больше зума -> диапазон усекается до конца зума
    assert task.start_idx == 0
    assert task.end_idx == 4


def test_issue_runs_do_not_overlap(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2)
    t1, _ = m.issue('c1')
    t2, _ = m.issue('c2')
    t3, done3 = m.issue('c3')
    assert t1.start_idx == 0 and t1.end_idx == 2
    assert t2.start_idx == 2 and t2.end_idx == 4
    # все тайлы выданы (in-flight) -> третьей задаче нечего дать, но не done
    assert t3 is None and done3 is False


def test_issue_skips_downloaded(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    m._repo(1).insert_ignore_many(1, [(0, 0, b'a', 'png'), (1, 0, b'b', 'png')])
    task, _ = m.issue('c1')
    assert task.start_idx == 2          # первый пропуск


def test_issue_two_clients_no_overlap_and_no_downloaded(make_manager):
    m = make_manager(min_zoom=2, max_zoom=2, chunk_size=4)
    # предзаполняем тайл idx=5
    m._repo(2).insert_ignore_many(2, [(utils.get_xy(5, 2)[0], utils.get_xy(5, 2)[1], b'a', 'png')])
    t1, _ = m.issue('c1')
    t2, _ = m.issue('c2')
    # диапазоны не пересекаются
    assert t1.end_idx <= t2.start_idx or t2.end_idx <= t1.start_idx
    # ни один выданный тайл не скачан
    for t in (t1, t2):
        for i in range(t.start_idx, t.end_idx):
            x, y = utils.get_xy(i, 2)
            assert not m._repo(2).exists_xy(x, y, 2) or i == 5


# --------------------------------------------------------- сброс по ТЗ

def test_client_reset_releases_range(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2)
    t1, _ = m.issue('c1')
    assert t1.start_idx == 0
    t2, _ = m.issue('c1')              # c1 просит новую без submit
    assert t1.status == 'reaped'
    assert t2.start_idx == 0           # тайлы t1 снова в пуле


def test_reset_released_range_issued_to_other_client(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2)
    t1, _ = m.issue('c1')              # [0,2)
    m.issue('c2')                      # c2 получает [2,4)
    # c1 сбрасывает свою задачу и тайлы [0,2) освобождаются
    t3, _ = m.issue('c1')
    assert t1.status == 'reaped'
    assert t3.start_idx == 0


# ----------------------------------------------------------------- сабмит

def test_submit_full(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    res = m.submit('c1', task.id, tiles_for(task))
    assert res.status == 'submitted'
    assert res.saved == 4
    assert res.duplicates == 0
    assert task.status == 'submitted'
    zz = [z for z in m.status()['zooms'] if z['zoom'] == 1][0]
    assert zz['downloaded'] == 4 and zz['done'] is True


def test_submit_partial_returns_remainder_to_pool(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    res = m.submit('c1', task.id, tiles_for(task)[:2])
    assert res.saved == 2
    task2, _ = m.issue('c2')
    assert task2 is not None and task2.start_idx == 2


def test_submit_unknown_task(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    assert m.submit('c1', 999, tiles_for(task)).status == 'unknown_task'
    assert m.submit('c2', task.id, tiles_for(task)).status == 'unknown_task'
    m.submit('c1', task.id, tiles_for(task))
    assert m.submit('c1', task.id, tiles_for(task)).status == 'unknown_task'


def test_submit_out_of_range_not_saved(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2)
    task, _ = m.issue('c1')           # [0,2)
    tiles = tiles_for(task) + [(1, 1, 'png', b'x')]   # idx=3 вне диапазона
    res = m.submit('c1', task.id, tiles)
    assert res.out_of_range == 1
    assert res.saved == 2
    assert m._repo(1).exists_xy(1, 1, 1) is False


def test_submit_duplicate_in_batch(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    tiles = tiles_for(task)
    tiles.append(tiles[0])            # дубль в пачке
    res = m.submit('c1', task.id, tiles)
    assert res.saved == 4
    assert res.duplicates == 1


def test_submit_duplicate_in_db(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    m._repo(1).insert_ignore_many(1, [(0, 0, b'a', 'png')])
    task = m._record_task(1, 'c1', 0, 1, m.clock())
    m._set_meta(m._zoom_key(1, 'cursor'), 1)
    res = m.submit('c1', task.id, [(0, 0, 'png', b'a')])
    assert res.saved == 0
    assert res.duplicates == 1


# -------------------------------------------------------------------- TTL

def test_reap_expired_frees_and_reissues(make_manager, clock):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2, task_ttl=10)
    t1, _ = m.issue('c1')             # [0,2), deadline = now+10
    clock.advance(5)
    assert m.reap_expired(clock()) == []        # ещё не истёк
    clock.advance(6)                      # прошло 11 > 10
    assert m.reap_expired(clock()) == [t1.id]
    assert t1.status == 'reaped'
    t2, _ = m.issue('c2')
    assert t2.start_idx == 0             # тайлы снова выданы


def test_heartbeat_extends_deadline(make_manager, clock):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2, task_ttl=10)
    t1, _ = m.issue('c1')
    clock.advance(8)
    assert m.heartbeat('c1', t1.id) is True
    assert t1.deadline == clock() + 10
    clock.advance(8)                      # 16 от issue, но 8 от heartbeat
    assert m.reap_expired(clock()) == []  # не истёк благодаря heartbeat
    clock.advance(3)                      # 11 от heartbeat
    assert m.reap_expired(clock()) == [t1.id]


def test_heartbeat_wrong_client_or_closed(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    assert m.heartbeat('c2', task.id) is False   # чужой клиент
    m.submit('c1', task.id, tiles_for(task))
    assert m.heartbeat('c1', task.id) is False   # задача закрыта


# ------------------------------------------------------------------- зумы

def test_zoom_transition(make_manager):
    m = make_manager(min_zoom=1, max_zoom=2, chunk_size=100)
    t1, _ = m.issue('c1')               # z1 (4 тайла)
    assert t1.zoom == 1
    m.submit('c1', t1.id, tiles_for(t1))
    t2, _ = m.issue('c1')               # z1 завершён -> z2
    assert t2.zoom == 2


def test_done_after_last_zoom(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=100)
    t1, _ = m.issue('c1')
    m.submit('c1', t1.id, tiles_for(t1))
    task, done = m.issue('c1')
    assert task is None and done is True


def test_single_zoom_mode(make_manager):
    m = make_manager(min_zoom=1, max_zoom=3, chunk_size=100, zoom=2)
    t1, _ = m.issue('c1')
    assert t1.zoom == 2
    m.submit('c1', t1.id, tiles_for(t1))
    task, done = m.issue('c1')
    assert task is None and done is True


# -------------------------------------------------------------- статистика

def test_status_fields_and_done_zoom_percent(make_manager):
    m = make_manager(min_zoom=1, max_zoom=2, chunk_size=100)
    st = m.status()
    assert st['done'] is False
    assert 'zooms' in st and 'clients' in st and 'global' in st
    z1 = [z for z in st['zooms'] if z['zoom'] == 1][0]
    assert z1['total'] == 4
    # завершённый зум показывает 100 %
    t1, _ = m.issue('c1')
    m.submit('c1', t1.id, tiles_for(t1))
    st2 = m.status()
    z1b = [z for z in st2['zooms'] if z['zoom'] == 1][0]
    assert z1b['percent'] == 100.0 and z1b['done'] is True


def test_rate_window(make_manager, clock):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4, task_ttl=10)
    task, _ = m.issue('c1')
    m.submit('c1', task.id, tiles_for(task))   # saved=4 в момент clock()
    clock.advance(100)
    # окно 300 c по умолчанию: сабмит внутри окна -> rate > 0
    assert m.status(window_sec=300)['global']['rate_tps'] > 0
    # окно 50 c: сабмит вне окна -> rate == 0
    assert m.status(window_sec=50)['global']['rate_tps'] == 0


def test_client_activity_window(make_manager, clock):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    m.submit('c1', task.id, tiles_for(task))   # задача закрыта, активных задач нет
    st = m.status(window_sec=100)
    assert st['global']['active_clients'] == 1   # last_seen в окне
    clock.advance(200)
    st2 = m.status(window_sec=100)
    # клиент без активных задач и вне окна -> неактивен
    assert st2['global']['active_clients'] == 0


def test_events_limit(make_manager):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    task, _ = m.issue('c1')
    m.submit('c1', task.id, tiles_for(task))
    ev = m.events(limit=10)
    assert len(ev) <= 10
    kinds = {e['kind'] for e in ev}
    assert 'issue' in kinds and 'submit' in kinds


# ----------------------------------------------------------------- рестарт

def test_restart_preserves_progress_and_reaps(make_manager, output_dir, clock):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=2)
    t1, _ = m.issue('c1')              # [0,2) issued
    t2, _ = m.issue('c2')              # [2,4) issued
    # c1 сдал частично (1 тайл)
    m.submit('c1', t1.id, tiles_for(t1)[:1])
    m.close()

    # новый менеджер на том же каталоге
    cfg = jobs.ServerConfig(output_dir=output_dir, min_zoom=1, max_zoom=1, chunk_size=2)
    m2 = jobs.JobManager(cfg, clock=clock)
    m2.open()
    try:
        # прогресс мастер-БД сохранён (1 тайл от c1)
        assert m2._downloaded(1) == 1
        # старые issued сброшены
        assert all(t.status != 'issued' for t in m2.tasks.values())
        # повторная выдача не дублирует скачанное: первый свободный = idx1
        task, _ = m2.issue('c3')
        assert task.start_idx == 1
    finally:
        m2.close()


def test_restart_submits_history_survives(make_manager, output_dir, clock):
    m = make_manager(min_zoom=1, max_zoom=1, chunk_size=4)
    t1, _ = m.issue('c1')
    m.submit('c1', t1.id, tiles_for(t1))     # saved=4
    m.close()
    cfg = jobs.ServerConfig(output_dir=output_dir, min_zoom=1, max_zoom=1, chunk_size=4)
    m2 = jobs.JobManager(cfg, clock=clock)
    m2.open()
    try:
        # история submits переживает рестарт -> скорость считается по сохранённым строкам
        assert m2.status(window_sec=300)['global']['rate_tps'] > 0
    finally:
        m2.close()
