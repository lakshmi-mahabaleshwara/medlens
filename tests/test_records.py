from medlens.config import Source
from medlens.ingestion import inventory, records
import pytest


def test_occurrences_and_malformed(tmp_path):
    path = tmp_path / "source.csv"
    path.write_bytes(b'a,b\r\n01,"x\ny"\r\n01,"x\ny"\r\nq,\\N\r\nq,\r\noops\r\n')
    spec = Source(files=["*.csv"], columns=["a", "b"], null_token=r"\N")
    (entry,) = inventory(tmp_path, spec)

    def scan():
        return list(records(tmp_path, entry, spec, "test", "v1", "records"))

    rows = scan()
    assert rows == scan()
    assert rows[0][:2] == ("01", "x\ny")
    assert rows[0][2] != rows[1][2] and rows[0][3] == rows[1][3]
    assert rows[2][1] is None and rows[3][1] == ""
    assert rows[-1][9] and rows[-1][6] == 5
    for row in rows:
        assert len(path.read_bytes()[row[7] : row[7] + row[8]]) == row[8]
    path.write_bytes(b"a,b\nbad,change")
    with pytest.raises(ValueError, match="checksum"):
        scan()


def test_unbounded_bad_quote(tmp_path):
    (tmp_path / "x.csv").write_bytes(b'a,b\n1,"open\n2,x\n')
    spec = Source(files=["x.csv"], columns=["a", "b"])
    with pytest.raises(ValueError, match="bytes"):
        list(records(tmp_path, inventory(tmp_path, spec)[0], spec, "d", "v", "t"))
