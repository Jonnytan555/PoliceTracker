from dataclasses import dataclass

@dataclass
class DownloaderConfig:
    max_workers: int = 4
    timeout_seconds: int = 60 * 30  # 30 minutes
