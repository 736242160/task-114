#!/usr/bin/env python3
"""
scheduler.py — 多资源日程调度工具（纯标准库，单文件）

功能：
  * 多资源定义 + 事件流式处理（跨事件状态延续，资源忙闲即时更新）
  * 同资源时间重叠检测（报告事件与重叠区间）
  * 优先级冲突裁决（高优先级挤掉低优先级，被挤掉/被拒绝均报告）
  * 错误报告：结束早于开始、引用不存在的资源、时间格式非法（含位置）
  * 全程历史可追溯（每个事件的受理/拒绝/被抢占均有流水记录）
  * 输出最终日程 + 错误清单 + 历史日志

输入格式（UTF-8 文本，# 开头为注释，空行忽略）：
  资源文件：每行一条
      resource <资源名>
  事件文件：每行一条（字段以空白分隔）
      event <事件ID> <资源名> <开始时间> <结束时间> <优先级>
  时间格式：ISO 8601，如 2026-09-27T08:00 或 2026-09-27T08:00:00
  优先级：整数，数值越大优先级越高

用法：
      python3 scheduler.py RESOURCES_FILE EVENTS_FILE
      python3 scheduler.py resources.txt -          # 事件从 stdin 读
"""

from __future__ import annotations

import bisect
import sys
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------

class Status(Enum):
    ACCEPTED = "ACCEPTED"    # 已排入日程
    REJECTED = "REJECTED"    # 冲突裁决中落败（对方优先级不低）
    PREEMPTED = "PREEMPTED"  # 曾排入，后被更高优先级事件挤掉
    INVALID = "INVALID"      # 输入非法，未进入裁决


@dataclass
class Event:
    event_id: str
    resource: str
    start: Optional[datetime]
    end: Optional[datetime]
    priority: Optional[int]
    line_no: int
    status: Status = Status.INVALID

    def label(self) -> str:
        return f"事件{self.event_id}(资源{self.resource})"


@dataclass
class ErrorReport:
    line_no: int
    event_id: str
    kind: str
    message: str

    def __str__(self) -> str:
        return f"[第{self.line_no}行][{self.kind}] {self.message}"


@dataclass
class HistoryEntry:
    seq: int
    line_no: int
    action: str
    detail: str

    def __str__(self) -> str:
        return f"#{self.seq:04d} [第{self.line_no}行] {self.action}: {self.detail}"


# ---------------------------------------------------------------------------
# 资源：按开始时间有序维护忙区间，支持任意到达顺序的重叠检测
# ---------------------------------------------------------------------------

@dataclass
class Resource:
    name: str
    _starts: list = field(default_factory=list)   # 有序的开始时间
    _events: list = field(default_factory=list)   # 与 _starts 对齐的 Event

    def overlapping(self, start: datetime, end: datetime) -> list[Event]:
        """返回与 [start, end) 重叠的所有在册事件（半开区间：首尾相接不算重叠）。"""
        result = []
        # 只需考察 start < end 且其 end > start 的事件
        idx = bisect.bisect_left(self._starts, end)
        for ev in self._events[:idx]:
            if ev.end > start:
                result.append(ev)
        return result

    def book(self, ev: Event) -> None:
        idx = bisect.bisect_left(self._starts, ev.start)
        self._starts.insert(idx, ev.start)
        self._events.insert(idx, ev)

    def unbook(self, ev: Event) -> None:
        idx = self._events.index(ev)
        del self._events[idx]
        del self._starts[idx]

    @property
    def schedule(self) -> list[Event]:
        return list(self._events)


# ---------------------------------------------------------------------------
# 调度器
# ---------------------------------------------------------------------------

class Scheduler:
    def __init__(self) -> None:
        self.resources: dict[str, Resource] = {}
        self.events: dict[str, Event] = {}
        self.errors: list[ErrorReport] = []
        self.history: list[HistoryEntry] = []
        self._seq = 0

    # ---- 基础记录 ----

    def _log(self, line_no: int, action: str, detail: str) -> None:
        self._seq += 1
        self.history.append(HistoryEntry(self._seq, line_no, action, detail))

    def _error(self, line_no: int, event_id: str, kind: str, message: str) -> None:
        self.errors.append(ErrorReport(line_no, event_id, kind, message))

    # ---- 资源定义 ----

    def add_resource(self, name: str, line_no: int) -> None:
        if name in self.resources:
            self._error(line_no, "-", "资源重复", f"资源 {name} 重复定义，忽略本次定义")
            return
        self.resources[name] = Resource(name)
        self._log(line_no, "注册资源", f"资源 {name} 已注册")

    # ---- 事件处理 ----

    def submit(self, ev: Event) -> None:
        """处理一个流入事件：校验 -> 重叠检测 -> 优先级裁决 -> 状态更新。"""
        # 1) 结束时间早于开始时间
        if ev.end < ev.start:
            ev.status = Status.INVALID
            self._error(ev.line_no, ev.event_id, "时间区间非法",
                        f"{ev.label()} 结束时间 {fmt(ev.end)} 早于开始时间 {fmt(ev.start)}，事件作废")
            self._log(ev.line_no, "拒绝", f"{ev.label()} 结束早于开始，未受理")
            return

        # 2) 引用不存在的资源
        res = self.resources.get(ev.resource)
        if res is None:
            ev.status = Status.INVALID
            self._error(ev.line_no, ev.event_id, "资源不存在",
                        f"{ev.label()} 引用了未定义的资源 {ev.resource}，事件作废")
            self._log(ev.line_no, "拒绝", f"{ev.label()} 资源不存在，未受理")
            return

        # 3) 重叠检测（基于该资源当前在册事件，状态跨事件延续）
        conflicts = res.overlapping(ev.start, ev.end)
        for other in conflicts:
            ov_start = max(ev.start, other.start)
            ov_end = min(ev.end, other.end)
            self._error(ev.line_no, ev.event_id, "时间重叠",
                        f"{ev.label()} 与 {other.label()} 在资源 {ev.resource} 上重叠，"
                        f"重叠段 [{fmt(ov_start)} ~ {fmt(ov_end)}]")

        # 4) 优先级裁决
        blockers = [o for o in conflicts if o.priority >= ev.priority]
        if blockers:
            ev.status = Status.REJECTED
            names = "、".join(o.event_id for o in blockers)
            self._error(ev.line_no, ev.event_id, "裁决落败",
                        f"{ev.label()} 优先级 {ev.priority} 不高于冲突事件 {names}，被拒绝")
            self._log(ev.line_no, "拒绝",
                      f"{ev.label()} 与事件 {names} 冲突且优先级不占优，未受理")
            return

        # 5) 受理：挤掉所有更低优先级的重叠事件，即时更新忙闲状态
        for victim in conflicts:
            res.unbook(victim)
            victim.status = Status.PREEMPTED
            self._error(ev.line_no, victim.event_id, "被抢占",
                        f"{victim.label()} 被更高优先级的 {ev.label()}"
                        f"(优先级 {ev.priority} > {victim.priority}) 挤掉，移出日程")
            self._log(ev.line_no, "抢占",
                      f"{victim.label()} 被 {ev.label()} 挤掉，资源 {ev.resource} "
                      f"时段 [{fmt(victim.start)} ~ {fmt(victim.end)}] 转为空闲")
        res.book(ev)
        ev.status = Status.ACCEPTED
        self.events[ev.event_id] = ev
        self._log(ev.line_no, "受理",
                  f"{ev.label()} 已排入日程，占用 [{fmt(ev.start)} ~ {fmt(ev.end)}]")

    # ---- 输出 ----

    def report(self, out=sys.stdout) -> None:
        print("=" * 60, file=out)
        print("最终日程", file=out)
        print("=" * 60, file=out)
        for name in sorted(self.resources):
            res = self.resources[name]
            print(f"\n资源 {name}：", file=out)
            if not res.schedule:
                print("  （空闲，无日程）", file=out)
            for ev in res.schedule:
                print(f"  [{fmt(ev.start)} ~ {fmt(ev.end)}] "
                      f"事件{ev.event_id}  优先级{ev.priority}", file=out)

        print("\n" + "=" * 60, file=out)
        print(f"错误清单（共 {len(self.errors)} 条）", file=out)
        print("=" * 60, file=out)
        for err in self.errors:
            print(str(err), file=out)
        if not self.errors:
            print("（无错误）", file=out)

        print("\n" + "=" * 60, file=out)
        print(f"历史流水（共 {len(self.history)} 条，可追溯）", file=out)
        print("=" * 60, file=out)
        for entry in self.history:
            print(str(entry), file=out)


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def fmt(dt: Optional[datetime]) -> str:
    return dt.isoformat(sep=" ") if dt else "?"


def parse_time(raw: str) -> datetime:
    """解析 ISO 8601 时间；非法时抛 ValueError。"""
    return datetime.fromisoformat(raw)


def load_resources(path: str, sched: Scheduler) -> None:
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) == 2 and parts[0] == "resource":
                sched.add_resource(parts[1], line_no)
            else:
                sched._error(line_no, "-", "资源定义非法",
                             f"无法解析的资源定义行：{line!r}（应为：resource <名称>）")


def load_events(stream, sched: Scheduler) -> None:
    FIELDS = ["事件ID", "资源", "开始时间", "结束时间", "优先级"]
    for line_no, line in enumerate(stream, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if parts and parts[0] == "event":
            parts = parts[1:]
        if len(parts) != 5:
            sched._error(line_no, parts[0] if parts else "-", "字段数量非法",
                         f"事件行应有 5 个字段（{' '.join(FIELDS)}），实际 {len(parts)} 个：{line!r}")
            continue
        event_id, resource, start_raw, end_raw, prio_raw = parts

        # 时间格式校验：逐字段解析，出错时报告事件与字段位置
        start = end = None
        bad = False
        for pos, (field_name, raw) in enumerate(
                (("开始时间", start_raw), ("结束时间", end_raw)), start=3):
            try:
                if field_name == "开始时间":
                    start = parse_time(raw)
                else:
                    end = parse_time(raw)
            except ValueError:
                sched._error(line_no, event_id, "时间格式非法",
                             f"事件{event_id} 第{pos}个字段（{field_name}）"
                             f"值 {raw!r} 不是合法的 ISO 8601 时间，事件作废")
                bad = True
        if bad:
            sched._log(line_no, "拒绝", f"事件{event_id} 时间格式非法，未受理")
            continue

        try:
            priority = int(prio_raw)
        except ValueError:
            sched._error(line_no, event_id, "优先级非法",
                         f"事件{event_id} 第5个字段（优先级）值 {prio_raw!r} 不是整数，事件作废")
            sched._log(line_no, "拒绝", f"事件{event_id} 优先级非法，未受理")
            continue

        sched.submit(Event(event_id, resource, start, end, priority, line_no))


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    sched = Scheduler()
    try:
        load_resources(argv[1], sched)
    except OSError as exc:
        print(f"无法读取资源文件：{exc}", file=sys.stderr)
        return 1
    if argv[2] == "-":
        load_events(sys.stdin, sched)
    else:
        try:
            with open(argv[2], encoding="utf-8") as fh:
                load_events(fh, sched)
        except OSError as exc:
            print(f"无法读取事件文件：{exc}", file=sys.stderr)
            return 1
    sched.report()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
