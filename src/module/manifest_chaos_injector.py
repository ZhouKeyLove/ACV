"""Lifecycle manager for user-owned, static Chaos Mesh manifests."""

from __future__ import annotations

import subprocess
from pathlib import Path

from .base import Base


class ManifestChaosInjector(Base):
    """Apply only experiment CRs and delete only those CRs on cleanup."""

    def __init__(self, manifests: list[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self.manifests = [str(Path(path).resolve()) for path in manifests]
        self._applied: list[str] = []

    def start_experiment(self) -> None:
        for manifest in self.manifests:
            if not Path(manifest).is_file():
                raise FileNotFoundError(f'Chaos manifest not found: {manifest}')
            self.info(f'Injecting Chaos Mesh fault from {manifest}')
            try:
                subprocess.run(
                    ['kubectl', 'apply', '-f', manifest],
                    check=True,
                )
            except BaseException:
                self.delete_experiment()
                raise
            self._applied.append(manifest)

    def delete_experiment(self) -> None:
        failures = []
        for manifest in reversed(self._applied):
            self.info(f'Recovering fault by deleting {manifest}')
            result = subprocess.run(
                ['kubectl', 'delete', '-f', manifest, '--ignore-not-found=true'],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                failures.append(f'{manifest}: {result.stderr.strip()}')
        self._applied.clear()
        if failures:
            raise RuntimeError('Failed to recover one or more faults: ' + '; '.join(failures))
