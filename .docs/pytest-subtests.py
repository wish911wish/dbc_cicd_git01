# conftest.py
import contextlib
from dataclasses import dataclass, field

@dataclass
class SubtestCollector:
    failures: list = field(default_factory=list)

    @contextlib.contextmanager
    def test(self, msg: str):
        try:
            yield
            print(f"  [PASS] {msg}")
        except AssertionError as e:
            self.failures.append((msg, e))
            print(f"  [FAIL] {msg}: {e}")

    def assert_all(self):
        if self.failures:
            summary = "\n".join(f"  - {msg}: {err}" for msg, err in self.failures)
            raise AssertionError(f"{len(self.failures)} assertion(s) failed:\n{summary}")

@pytest.fixture
def subtests():
    collector = SubtestCollector()
    yield collector
    collector.assert_all()
