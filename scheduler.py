#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多资源日程调度工具（纯标准库，单文件）

用法:
    python3 scheduler.py input.json          # 文本报告
    python3 scheduler.py input.json --json   # JSON 报告
    cat input.json | python3 scheduler.py -  # 从标准输入读取

输入格式 (JSON):
{
  "resources": ["room-A", "room-B", ...],          // 或 [{"name": "room-A", ...}, ...]
  "events": [
    {"id": "e1", "resource": "room-A",
     "start": "2026-09-27 09:00", "end": "2026-09-27 10:00", "priority": 5},
    ...
  ]
}

时间格式: ISO 8601，"YYYY-MM-DD HH:MM[:SS]" 或 "YYYY-MM-DDTHH:MM[:SS]"（可带时区偏移）。

裁决规则:
  - 事件按输入流顺序处理，处理即生效（状态即时更新）。
  - 同资源时间重叠即冲突；优先级高者保留，低者被挤掉（驱逐）并报告。
  - 优先级相同：先到者保留（first-come-first-served），后来者被拒绝。
  - 新事件只有严格高于其重叠的所有在册事件时才能入场，否则整体被拒绝。
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


# ---------------------------------------------------------------- 数据模型

@dataclass
class Event:
    """一条日程事件（已校验时间后）。"""
    id: str
    resource: str
    start: datetime
    end: datetime
    priority: int
    seq: int  # 流内序号，用于同优先级先到先服务


@dataclass
class Report:
    """调度过程的全部产出：最终日程、错误清单、历史轨迹。"""
    schedule: dict[str, list[Event]] = field(default_factory=dict)   # resource -> 在册事件
    errors: list[dict[str, Any]] = field(default_factory=list)       # 错误/冲突报告
    history: list[dict[str, Any]] = field(default_factory=list)      # 可追溯历史

    def log(self, action: str, **detail: Any) -> None:
        self.history.append({"action": action, **detail})

    def error(self, kind: str, **detail: Any) -> None:
        entry = {"type": kind, **detail}
        self.errors.append(entry)
        self.log("error", **entry)


# ---------------------------------------------------------------- 时间解析

def parse_time(raw: Any, event_id: str, position: str, report: Report) -> Optional[datetime]:
    """解析时间字段；失败时报告（事件、位置）并返回 None。"""
    if not isinstance(raw, str):
        report.error("invalid_time", event=event_id, position=position,
                     value=raw, reason="时间字段必须是字符串")
        return None
    text = raw.strip()
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        report.error("invalid_time", event=event_id, position=position,
                     value=raw, reason="非法时间格式，需为 ISO 8601，如 2026-09-27 09:00")
        return None


# ---------------------------------------------------------------- 调度核心

class Scheduler:
    def __init__(self, resources: list[str]):
        self.resources = set(resources)
        # 每个资源一条在册事件列表（按开始时间有序），即该资源的"忙"区间集合
        self.timeline: dict[str, list[Event]] = {r: [] for r in resources}
        self.report = Report()
        self._seq = 0

    # ---- 重叠判定：a 与 b 时间相交（端点相接不算重叠）
    @staticmethod
    def overlaps(a_start: datetime, a_end: datetime,
                 b_start: datetime, b_end: datetime) -> bool:
        return a_start < b_end and b_start < a_end

    @staticmethod
    def overlap_segment(a: Event, b: Event) -> dict[str, str]:
        lo, hi = max(a.start, b.start), min(a.end, b.end)
        return {"from": lo.isoformat(sep=" "), "to": hi.isoformat(sep=" ")}

    # ---- 单事件处理
    def submit(self, raw: dict[str, Any]) -> None:
        self._seq += 1
        seq = self._seq
        event_id = str(raw.get("id", f"#{seq}"))
        rep = self.report
        rep.log("received", event=event_id, seq=seq)

        # 1) 资源存在性
        resource = raw.get("resource")
        if resource not in self.resources:
            rep.error("unknown_resource", event=event_id, resource=resource,
                      reason=f"资源 {resource!r} 不存在")
            return

        # 2) 时间格式
        start = parse_time(raw.get("start"), event_id, "start", rep)
        end = parse_time(raw.get("end"), event_id, "end", rep)
        if start is None or end is None:
            return

        # 3) 结束早于开始
        if end < start:
            rep.error("inverted_time", event=event_id,
                      start=start.isoformat(sep=" "), end=end.isoformat(sep=" "),
                      reason="结束时间早于开始时间")
            return
        if end == start:
            rep.error("inverted_time", event=event_id,
                      start=start.isoformat(sep=" "), end=end.isoformat(sep=" "),
                      reason="结束时间等于开始时间（零时长事件）")
            return

        # 4) 优先级
        try:
            priority = int(raw.get("priority", 0))
        except (TypeError, ValueError):
            rep.error("invalid_priority", event=event_id,
                      value=raw.get("priority"), reason="优先级必须是整数")
            return

        event = Event(event_id, resource, start, end, priority, seq)

        # 5) 冲突检测：找出该资源上所有时间重叠的在册事件
        occupants = self.timeline[resource]
        conflicts = [o for o in occupants
                     if self.overlaps(start, end, o.start, o.end)]

        if not conflicts:
            self._admit(event)
            return

        # 报告每一处重叠（事件、重叠段）
        for o in conflicts:
            rep.error("overlap", event=event_id, resource=resource,
                      conflicts_with=o.id,
                      overlap=self.overlap_segment(event, o),
                      reason="同资源时间重叠")

        # 6) 优先级裁决：必须严格高于所有冲突者才能入场
        blocking = [o for o in conflicts if o.priority >= priority]
        if blocking:
            rep.error("evicted", event=event_id, resource=resource,
                      evicted_by=[o.id for o in blocking],
                      reason="优先级不高于冲突事件，被挤掉（拒绝入场）")
            rep.log("rejected", event=event_id,
                    by=[o.id for o in blocking])
            return

        # 新事件胜出：驱逐所有低优先级冲突者（忙闲状态即时更新）
        for o in conflicts:
            occupants.remove(o)
            rep.error("evicted", event=o.id, resource=resource,
                      evicted_by=[event_id],
                      reason=f"被更高优先级事件 {event_id} 挤掉")
            rep.log("evicted", event=o.id, by=event_id)
        self._admit(event)

    # ---- 入场：插入并保持按开始时间有序
    def _admit(self, event: Event) -> None:
        occupants = self.timeline[event.resource]
        idx = 0
        while idx < len(occupants) and occupants[idx].start <= event.start:
            idx += 1
        occupants.insert(idx, event)
        self.report.log("admitted", event=event.id, resource=event.resource,
                        start=event.start.isoformat(sep=" "),
                        end=event.end.isoformat(sep=" "),
                        priority=event.priority)

    # ---- 汇总输出
    def finalize(self) -> Report:
        self.report.schedule = {r: list(evts) for r, evts in self.timeline.items()}
        return self.report


# ---------------------------------------------------------------- 输入/输出

def load_input(path: str) -> dict[str, Any]:
    text = sys.stdin.read() if path == "-" else open(path, encoding="utf-8").read()
    data = json.loads(text)
    if not isinstance(data, dict) or "resources" not in data or "events" not in data:
        raise SystemExit("输入必须是含 resources 与 events 字段的 JSON 对象")
    return data


def normalize_resources(raw: Any) -> list[str]:
    names = []
    for item in raw:
        names.append(item["name"] if isinstance(item, dict) else str(item))
    return names


def render_text(report: Report, resources: list[str]) -> str:
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("日程安排（最终生效）")
    lines.append("=" * 60)
    for res in resources:
        lines.append(f"\n[{res}]")
        evts = report.schedule.get(res, [])
        if not evts:
            lines.append("  （空闲，无日程）")
        for e in evts:
            lines.append(f"  {e.start:%Y-%m-%d %H:%M} ~ {e.end:%Y-%m-%d %H:%M}"
                         f"  优先级={e.priority}  事件={e.id}")

    lines.append("\n" + "=" * 60)
    lines.append(f"错误/冲突报告（共 {len(report.errors)} 条）")
    lines.append("=" * 60)
    if not report.errors:
        lines.append("  无")
    for i, err in enumerate(report.errors, 1):
        lines.append(f"\n#{i} [{err['type']}] 事件={err.get('event')}")
        for k, v in err.items():
            if k not in ("type", "event"):
                lines.append(f"    {k}: {v}")

    lines.append("\n" + "=" * 60)
    lines.append(f"历史轨迹（共 {len(report.history)} 条，按时间流顺序）")
    lines.append("=" * 60)
    for i, h in enumerate(report.history, 1):
        detail = ", ".join(f"{k}={v}" for k, v in h.items() if k != "action")
        lines.append(f"  {i:>3}. {h['action']:<9} {detail}")
    return "\n".join(lines)


def report_to_json(report: Report) -> str:
    def ev(e: Event) -> dict[str, Any]:
        return {"id": e.id, "start": e.start.isoformat(sep=" "),
                "end": e.end.isoformat(sep=" "), "priority": e.priority}
    out = {
        "schedule": {r: [ev(e) for e in evts] for r, evts in report.schedule.items()},
        "errors": report.errors,
        "history": report.history,
    }
    return json.dumps(out, ensure_ascii=False, indent=2)


def main(argv: list[str]) -> None:
    if len(argv) < 2:
        print(__doc__)
        raise SystemExit(1)
    as_json = "--json" in argv
    path = next(a for a in argv[1:] if not a.startswith("--"))

    data = load_input(path)
    resources = normalize_resources(data["resources"])

    scheduler = Scheduler(resources)
    for raw_event in data["events"]:
        if not isinstance(raw_event, dict):
            scheduler.report.error("invalid_event", event=str(raw_event),
                                   reason="事件必须是 JSON 对象")
            continue
        scheduler.submit(raw_event)

    report = scheduler.finalize()
    print(report_to_json(report) if as_json else render_text(report, resources))


if __name__ == "__main__":
    main(sys.argv)
