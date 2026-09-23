import pytest
from chert_reference_agent.journal import Journal


def test_singleton_and_project_binding(tmp_path):
    path = str(tmp_path / 'journal.json')
    a = Journal(path, 'project-a')
    a.open()
    a.add('namespace-owned-room')
    b = Journal(path, 'project-a')
    with pytest.raises(RuntimeError):
        b.open()
    a.close()
    with pytest.raises(RuntimeError):
        Journal(path, 'project-b').open()
    c = Journal(path, 'project-a')
    c.open()
    assert list(c.receipts) == ['namespace-owned-room']
    c.remove('namespace-owned-room')
    c.close()
