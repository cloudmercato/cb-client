# Graph Report - cb-client  (2026-10-10)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 360 nodes · 599 edges · 57 communities (10 shown, 47 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `f04b0b0c`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- wringers.py
- runners.py
- Public runner to parse and publish result.
- Geekbench4Wringer
- MetricWringer
- ._get_metadata
- .__init__
- Client
- BaseObjectStorageWringer
- BaseNetworkWringer
- BaseWringer
- DfsioWringer
- exceptions.py
- VaspTestWringer
- OpensslSpeedWringer
- SwingBenchWringer
- CpuStealWringer
- FioWringer
- LammpsWringer
- PhoronixTestSuiteWringer
- RedisBenchmarkWringer
- SudokuMlBenchWringer
- SysbenchOltpWringer
- VdbenchWringer
- BlenderBenchmarkWringer
- BwMemWringer
- CacheWringer
- CiTaskWringer
- DdWringer
- FfmpegWringer
- FinanceBenchWringer
- GpuBurnWringer
- HammerdbWringer
- InvokeAiBenchmarkWringer
- IpmiSensorsWringer
- KvazaarWringer
- LatOpsWringer
- LmbenchStreamWringer
- LmSensorsWringer
- MdtestWringer
- MtrWringer
- NpbWringer
- NvbandwidthWringer
- OllamaBenchmarkJudgeWringer
- OllamaBenchmarkSpeedWringer
- PgbenchWringer
- PyPerformanceWringer
- PythonBenchmarkWringer
- PythonFpbWringer
- PythonReadWringer
- StreamWringer
- TcptracerouteWringer
- TlbWringer
- VRayWringer
- YcsbWringer

## God Nodes (most connected - your core abstractions)
1. `BaseWringer` - 74 edges
2. `MetricWringer` - 14 edges
3. `BaseRunner` - 13 edges
4. `Client` - 11 edges
5. `BaseOsBenchmarkWringer` - 10 edges
6. `BaseHiBenchWringer` - 8 edges
7. `Geekbench4Runner` - 8 edges
8. `DfsioWringer` - 7 edges
9. `BaseObjectStorageWringer` - 7 edges
10. `FioRunner` - 6 edges

## Surprising Connections (you probably didn't know these)
- `get_wringer()` --calls--> `WringerNotFound`  [EXTRACTED]
  cb_client/wringers.py → cb_client/exceptions.py

## Import Cycles
- None detected.

## Communities (57 total, 47 thin omitted)

### Community 0 - "wringers.py"
Cohesion: 0.06
Nodes (14): get_config(), BaseHiBenchWringer, BaseOsBenchmarkWringer, BonnieWringer, convert_second(), CoreMarkWringer, OsBenchmarkAbWringer, OsBenchmarkCurlWringer (+6 more)

### Community 1 - "runners.py"
Cohesion: 0.06
Nodes (6): ClientArgumentParser, BaseRunner, FioRunner, Geekbench4Runner, SysbenchCpuRunner, SysbenchRamRunner

### Community 2 - "Public runner to parse and publish result."
Cohesion: 0.16
Nodes (4): PrometheusMetricWringer, SpecCpu2006Wringer, SpecCpu2017Wringer, SpecCpu2026Wringer

### Community 3 - "Geekbench4Wringer"
Cohesion: 0.18
Nodes (5): Geekbench4Wringer, Geekbench5Wringer, Geekbench6Wringer, geo_mean(), weighted_mean()

### Community 5 - "._get_metadata"
Cohesion: 0.18
Nodes (4): AiBenchmarkWringer, DeepsparseBenchmarkWringer, LatMemRdWringer, MhzWringer

### Community 6 - ".__init__"
Cohesion: 0.18
Nodes (3): FfmpegBenchmarkTranscodeWringer, OllamaWringer, WhisperBenchmarkWringer

### Community 8 - "BaseObjectStorageWringer"
Cohesion: 0.24
Nodes (3): AbWringer, BaseObjectStorageWringer, WrkWringer

### Community 9 - "BaseNetworkWringer"
Cohesion: 0.24
Nodes (4): BaseNetworkWringer, GoToWafWringer, IperfWringer, TracepathWringer

### Community 10 - "BaseWringer"
Cohesion: 0.20
Nodes (5): BaseWringer, Geekbench3Wringer, SysbenchCpuWringer, SysbenchRamWringer, YoloBenchmarkPredictWringer

### Community 12 - "exceptions.py"
Cohesion: 0.43
Nodes (5): CbClientError, ParseError, ServerError, WringerNotFound, get_wringer()

## Knowledge Gaps
- **47 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `BaseWringer` connect `BaseWringer` to `wringers.py`, `Public runner to parse and publish result.`, `Geekbench4Wringer`, `MetricWringer`, `._get_metadata`, `.__init__`, `BaseObjectStorageWringer`, `BaseNetworkWringer`, `DfsioWringer`, `VaspTestWringer`, `OpensslSpeedWringer`, `SwingBenchWringer`, `CpuStealWringer`, `FioWringer`, `LammpsWringer`, `PhoronixTestSuiteWringer`, `RedisBenchmarkWringer`, `SudokuMlBenchWringer`, `SysbenchOltpWringer`, `VdbenchWringer`, `BlenderBenchmarkWringer`, `BwMemWringer`, `CacheWringer`, `CiTaskWringer`, `DdWringer`, `FfmpegWringer`, `FinanceBenchWringer`, `GpuBurnWringer`, `HammerdbWringer`, `InvokeAiBenchmarkWringer`, `IpmiSensorsWringer`, `KvazaarWringer`, `LatOpsWringer`, `LmbenchStreamWringer`, `LmSensorsWringer`, `MdtestWringer`, `MtrWringer`, `NpbWringer`, `NvbandwidthWringer`, `OllamaBenchmarkJudgeWringer`, `OllamaBenchmarkSpeedWringer`, `PgbenchWringer`, `PyPerformanceWringer`, `PythonBenchmarkWringer`, `PythonFpbWringer`, `PythonReadWringer`, `StreamWringer`, `TcptracerouteWringer`, `TlbWringer`, `VRayWringer`, `YcsbWringer`?**
  _High betweenness centrality (0.252) - this node is a cross-community bridge._
- **Should `wringers.py` be split into smaller, more focused modules?**
  _Cohesion score 0.05990338164251208 - nodes in this community are weakly interconnected._
- **Why does `MetricWringer` connect `MetricWringer` to `wringers.py`, `BaseWringer`, `Public runner to parse and publish result.`?**
  _High betweenness centrality (0.086) - this node is a cross-community bridge._
- **Should `runners.py` be split into smaller, more focused modules?**
  _Cohesion score 0.06423034330011074 - nodes in this community are weakly interconnected._
- **Why does `Client` connect `Client` to `wringers.py`?**
  _High betweenness centrality (0.049) - this node is a cross-community bridge._