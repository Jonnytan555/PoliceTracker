from dataclasses import dataclass

@dataclass
class FileDownload:
    remote_file: str
    local_file: str
    params: dict | None = None
    headers: dict | None = None
    auth: tuple | None = None
    expected_size_byte: int = 0
    timeout_seconds: int = 60
    always_overwrite: bool = False
