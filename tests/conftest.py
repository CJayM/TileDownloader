"""Общие фикстуры тестов.

Каталоги БД — временные (tmp_path). Время и сеть в ядре инжектируются
(см. jobs.py), реальных ожиданий и внешних запросов в тестах нет.
"""

import pytest

import db
import jobs


class FakeClock:
    """Управляемые часы для тестов TTL и окон."""

    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, secs):
        self.now += secs


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def output_dir(tmp_path):
    """Временный каталог для мастер-БД и jobs.db3."""
    return str(tmp_path)


@pytest.fixture
def make_manager(output_dir, clock):
    """Фабрика JobManager: создаёт конфиг, открывает менеджер.

    Все созданные менеджеры закрываются после теста.
    """
    managers = []

    def _make(min_zoom=1, max_zoom=2, chunk_size=5, task_ttl=900, zoom=None):
        cfg = jobs.ServerConfig(output_dir=output_dir, min_zoom=min_zoom,
                                max_zoom=max_zoom, chunk_size=chunk_size,
                                task_ttl=task_ttl, zoom=zoom)
        m = jobs.JobManager(cfg, clock=clock)
        m.open()
        managers.append(m)
        return m

    yield _make

    for m in managers:
        m.close()


@pytest.fixture
def make_repo(output_dir):
    """Фабрика Repository: make_repo(zoom) -> открытый репозиторий с таблицей.

    Все созданные репозитории закрываются после теста.
    """
    repos = []

    def _make(zoom=1):
        r = db.Repository(output_dir)
        r.open(zoom)
        r.create_table(zoom)
        repos.append(r)
        return r

    yield _make

    for r in repos:
        r.close()
