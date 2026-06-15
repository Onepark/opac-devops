from restore_tooling.sanitizer_names import first_names, last_names


def test_first_names_non_empty():
    names = first_names()
    assert len(names) > 0
    assert all(isinstance(n, str) and len(n) > 0 for n in names)


def test_last_names_non_empty():
    names = last_names()
    assert len(names) > 0
    assert all(isinstance(n, str) and len(n) > 0 for n in names)


def test_first_names_deterministic():
    assert first_names() is first_names()


def test_last_names_deterministic():
    assert last_names() is last_names()
