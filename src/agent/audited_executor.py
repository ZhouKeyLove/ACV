"""AutoGen code executor that records state/action/state experiment samples."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autogen import UserProxyAgent

from ..module.system_state_collector import SystemStateCollector
from ..module.utils import load_config


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serializable(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def _describe_blocks(code_blocks: Any) -> list[dict[str, str]]:
    descriptions = []
    for block in code_blocks:
        if hasattr(block, 'code'):
            language = getattr(block, 'language', '')
            code = block.code
        elif isinstance(block, (tuple, list)) and len(block) >= 2:
            language, code = block[0], block[1]
        else:
            language, code = '', str(block)
        descriptions.append({'language': str(language), 'code': str(code)})
    return descriptions


class AuditedUserProxyAgent(UserProxyAgent):
    """Capture one record for every executor turn, including read-only actions."""

    def __init__(self, *args, audit_path: str, service_name: str, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        config = load_config()
        delay = config.get('observability', {}).get('post_action_delay_seconds', 15)
        self._post_action_delay = float(delay)
        self._state_collector = SystemStateCollector(
            namespace=config['project']['namespace']
        )
        base = Path(audit_path)
        self._audit_path = base.with_name(
            f'{base.stem}-{service_name}{base.suffix or ".jsonl"}'
        )
        self._audit_path.parent.mkdir(parents=True, exist_ok=True)
        self._audited_service_name = service_name

    def _append_record(self, record: dict) -> None:
        encoded = (json.dumps(record, ensure_ascii=False, default=repr) + '\n').encode('utf-8')
        descriptor = os.open(
            self._audit_path,
            os.O_APPEND | os.O_CREAT | os.O_WRONLY,
            0o644,
        )
        try:
            os.write(descriptor, encoded)
        finally:
            os.close(descriptor)

    def execute_code_blocks(self, code_blocks, *args, **kwargs):
        operation_id = (
            f'{self._audited_service_name}-'
            f'{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")}'
        )
        before = self._state_collector.capture()
        action_started_at = _utc_now()
        result = None
        failure = None
        started = time.monotonic()
        try:
            result = super().execute_code_blocks(code_blocks, *args, **kwargs)
            return result
        except BaseException as exc:
            failure = f'{type(exc).__name__}: {exc}'
            raise
        finally:
            action_finished_at = _utc_now()
            action_duration = time.monotonic() - started
            time.sleep(self._post_action_delay)
            after = self._state_collector.capture()
            self._append_record({
                'operation_id': operation_id,
                'service_maintainer': self._audited_service_name,
                'before': before,
                'action': {
                    'started_at': action_started_at,
                    'finished_at': action_finished_at,
                    'duration_seconds': action_duration,
                    'code_blocks': _describe_blocks(code_blocks),
                    'result': _serializable(result),
                    'error': failure,
                },
                'after_15_seconds': after,
            })
