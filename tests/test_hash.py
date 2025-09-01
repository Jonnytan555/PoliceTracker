from app.utils import sha256_row
def test_hash_deterministic():
    a = {"x": 1, "y": [2,3]}
    b = {"y": [2,3], "x": 1}
    assert sha256_row(a) == sha256_row(b)
