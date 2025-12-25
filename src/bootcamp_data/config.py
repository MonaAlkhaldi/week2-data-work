from pathlib import Path
from dataclasses import dataclass

from types import SimpleNamespace



PROJECT_ROOT = Path(__file__).resolve().parents[2]  # bootcamp_data/config.py -> root


@dataclass(frozen=True)
class Paths:
    root : Path
    raw : Path
    cache: Path
    processed: Path
    external: Path

def make_paths(root: Path = PROJECT_ROOT):
    return SimpleNamespace(
        root=root,
        data=root / "data",
        raw=root / "data" / "raw",
        processed=root / "data" / "processed",
        reports=root / "reports",
        figures=root / "reports" / "figures",
    )


    

