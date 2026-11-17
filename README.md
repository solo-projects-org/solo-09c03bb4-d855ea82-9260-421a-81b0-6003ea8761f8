# 集合降雨超阈值概率 API

防汛预报员把**同一预报时次**的 2～8 个集合成员降雨网格，合成为逐格的**超阈值概率**产品，
供值班员圈定高风险区域。技术栈：**FastAPI + SQLite（WAL）+ NumPy**，可双实例部署、共享同一数据库文件。

## 1. 业务规则与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 登记 UTC 时次、统一网格尺寸、2～8 个成员、雨量阈值 | `POST /groups`，校验时区（必须带时区，统一转 UTC）、成员数 2–8、阈值非负且唯一、网格上限 |
| 逐成员提交非负雨量矩阵、缺测格、递增修订号 | `PUT /groups/{gid}/members/{mid}`，body 含 `rain`、可选 `missing`（0/1 网格，1=缺测）、`revision`（≥1 且严格递增） |
| 同版同文重送不重复 | 同 `revision` 且内容 SHA-256 相同 → 返回 `duplicate=true`，不写新行、不影响产品 |
| 异文拒收 | 同 `revision` 但内容不同 → `409 REVISION_CONTENT_CONFLICT` |
| 成员齐全才可发布 | 任一成员无修订 → `409 MEMBERS_INCOMPLETE` |
| 逐格概率 | `p = 有效成员中 rain ≥ 阈值 的数量 / 该格有效成员数`（NumPy 向量化，按阈值分别计算） |
| 返回概率、分母与所用修订 | 产品含每阈值的 `probability`、`denominator` 矩阵及 `members_used`（成员→修订号） |
| 全缺测格返回空值 | 分母 `0`，概率序列化为 JSON `null` |
| 更正成员后旧产品立即过期 | 新修订入库的同一事务里把该组所有产品置 `is_current=0`；历史行保留 |
| 历史仍可查 | `GET /groups/{gid}/products` 与 `.../products/{id}` 可查已过期产品，响应中 `is_current=false` |
| 当前查询不能把旧概率作为预警依据 | `GET .../products/current` 无现行产品时返回 `404 NO_CURRENT_PRODUCT`，绝不返回过期数据 |
| 两实例交错上传/发布只能产生同组修订的完整产品 | 发布在写事务内重新核对“每个成员的最新修订”，若计算期间发生更正则整体重试；产品只以一致修订集落库（见下节） |
| 计算失败不得留半份当前结果 | 产品主行与全部阈值行在同一 `BEGIN IMMEDIATE` 事务插入，任何异常整体回滚（自测含故障注入验证） |
| 慢计算不阻塞其他时次入仓 | NumPy 计算在写事务**之外**完成，计算期间不持写锁；仅在最后落库阶段短时加锁 |
| 可启动的双实例 Docker Compose 和 Swagger | `docker compose up` 起 `api-a`(:8081)/`api-b`(:8082)，共享命名卷；Swagger 在 `/docs` |

### 跨时次持续高风险分析（`POST /analysis/persistent-high-risk`）

值班员需要从连续几个时次的预报里找出**持续高风险网格**，安排现场复核。

| 需求 | 实现 |
| --- | --- |
| 传入 2～6 个时次 ID，按 UTC 升序、相邻间隔 1 小时 | 请求体 `group_ids`；服务端按 `forecast_time` 校验严格递增且相邻差恰为 1 小时，否则 `422 TIME_SEQUENCE_INVALID`；重复 ID 在请求模型层拒绝 |
| 传入雨量阈值与概率警戒线 | `threshold`（须存在于每个时次的阈值列表，容差 1e-9）、`probability_threshold`（`[0,1]`） |
| 读取各时次**现行产品** | 只读 `products.is_current=1`；任一时次无现行产品 → `409 ANALYSIS_SOURCE_MISSING` |
| 同形网格才分析 | 各时次 `rows/cols` 必须一致，否则 `422 GRID_MISMATCH` |
| 唯一命中阈值才对时序 | 阈值不在某时次产品中 → `422 THRESHOLD_MISMATCH` |
| 连续两轮概率非空且均 ≥ 警戒线 | 逐格判定 `p[t] >= line`（边界相等算达标，`null` 视为不达标）；`null` 会**中断**连续计数；命中后返回该格首次确认的时次 |
| 返回首次确认时次 | 响应含 `first_confirmed_step`（1 起的时次序号）以及同一位置的 `first_confirmed_forecast_time` / `_group_id` / `_product_id`；未命中的格为 `null` |
| 列出所用产品 ID | `product_ids`（按时次顺序）与 `products_used`（含产品 ID、预报时间、成员修订集） |
| 计算期间来源被更正/撤回/重发则不交付旧组合 | 计算前后各取一次只读快照，提交前复核每个时次的**现行产品 ID** 与**各成员最新修订**；不一致 → `409 ANALYSIS_SOURCE_CHANGED`，提示重试，绝不返回混合旧数据 |
| 计算不长期占用写锁 | 分析全程只用只读事务（WAL 下不阻塞写），NumPy 计算在事务之外；不需要写锁，其他时次可继续入仓 |

响应示例（`1x5` 网格、4 个时次，第 2 个时次确认前两格）：

```json
{
  "group_ids": ["g0", "g1", "g2", "g3"],
  "forecast_times": ["2026-11-16T00:00:00+00:00", "2026-11-16T01:00:00+00:00",
                     "2026-11-16T02:00:00+00:00", "2026-11-16T03:00:00+00:00"],
  "rows": 1, "cols": 5,
  "rain_threshold": 5.0, "probability_threshold": 0.5,
  "consecutive_rounds_required": 2,
  "product_ids": [11, 12, 13, 14],
  "products_used": [
    {"step": 1, "group_id": "g0", "forecast_time": "2026-11-16T00:00:00+00:00",
     "product_id": 11, "revision_map": {"0": 1, "1": 1}},
    {"step": 2, "group_id": "g1", "forecast_time": "2026-11-16T01:00:00+00:00",
     "product_id": 12, "revision_map": {"0": 1, "1": 1}}
  ],
  "first_confirmed_step": [[2, 4, 4, 2, null]],
  "first_confirmed_forecast_time": [[
    "2026-11-16T01:00:00+00:00", "2026-11-16T03:00:00+00:00",
    "2026-11-16T03:00:00+00:00", "2026-11-16T01:00:00+00:00", null]],
  "first_confirmed_group_id": [[ "g1", "g3", "g3", "g1", null ]],
  "first_confirmed_product_id": [[ 12, 14, 14, 12, null ]],
  "confirmed_cell_count": 4
}
```

新增错误码：`TIME_SEQUENCE_INVALID(422)`、`THRESHOLD_MISMATCH(422)`、
`ANALYSIS_SOURCE_MISSING(409)`、`ANALYSIS_SOURCE_CHANGED(409)`。

### 交错发布的一致性（核心并发设计）

1. 先在一个只读事务里取快照：每个成员的**最新修订号**及对应网格；
2. 释放锁，在事务外做 NumPy 概率计算（慢计算不阻塞任何写入）；
3. 重新进入 `BEGIN IMMEDIATE` 写事务，再次读取每个成员最新修订号：
   - 与快照一致 → 旧产品批量过期、新产品与全部阈值行原子写入；
   - 不一致（计算期间有更正）→ 回滚并按新修订集整体重试（最多 3 次）；
4. 同修订集重复发布走唯一约束/幂等路径，两个实例并发发布同一版本只返回同一个产品。

SQLite 启用 WAL + `busy_timeout=10s`；所有写事务为 `BEGIN IMMEDIATE`，进程间靠单写者锁串行化，
锁竞争失败返回 `409/503` 提示重试，不会产生脏写或半份产品。

## 2. API 一览（Swagger 见 `/docs`，OpenAPI 见 `/openapi.json`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/groups` | 登记时次/网格/成员/阈值 |
| GET | `/groups` | 列出时次 |
| GET | `/groups/{gid}` | 时次状态（各成员最新修订、是否齐全、当前产品 ID） |
| PUT | `/groups/{gid}/members/{mid}` | 提交/更正成员网格修订 |
| POST | `/groups/{gid}/publish` | 发布概率产品（成员齐全） |
| GET | `/groups/{gid}/products/current` | 当前可用于预警的产品（无则 404） |
| GET | `/groups/{gid}/products` | 全部产品（含过期历史） |
| GET | `/groups/{gid}/products/{pid}` | 按 ID 查历史产品 |
| POST | `/analysis/persistent-high-risk` | 跨时次识别持续高风险网格（连续两轮达标） |

### 示例

```bash
# 1) 登记：2x2 网格、3 个成员、两个阈值
curl -s localhost:8081/groups -H 'Content-Type: application/json' -d '{
  "forecast_time": "2026-11-16T00:00:00Z",
  "rows": 2, "cols": 2,
  "member_names": ["m1", "m2", "m3"],
  "thresholds": [5.0, 10.0]
}'

# 2) 逐成员提交（m3 第二行两个格缺测）
curl -s -X PUT localhost:8081/groups/$GID/members/m1 -H 'Content-Type: application/json' \
  -d '{"rain":[[0,10],[20,0]],"revision":1}'
curl -s -X PUT localhost:8082/groups/$GID/members/m2 -H 'Content-Type: application/json' \
  -d '{"rain":[[10,10],[0,0]],"revision":1}'
curl -s -X PUT localhost:8081/groups/$GID/members/m3 -H 'Content-Type: application/json' \
  -d '{"rain":[[5,0],[99,0]],"missing":[[0,0],[1,1]],"revision":1}'

# 3) 发布并查询
curl -s -X POST localhost:8081/groups/$GID/publish
curl -s localhost:8082/groups/$GID/products/current

# 4) 跨时次持续高风险分析（$G00 … $G03 为 4 个连续整点、同网格时次）
curl -s -X POST localhost:8081/analysis/persistent-high-risk \
  -H 'Content-Type: application/json' -d '{
    "group_ids": ["'$G00'", "'$G01'", "'$G02'", "'$G03'"],
    "threshold": 5.0,
    "probability_threshold": 0.5
  }'
```

产品响应片段（全缺测格 `null` + 分母 0）：

```json
{
  "product_id": 1,
  "is_current": true,
  "members_used": [
    {"member_idx": 0, "member_id": "m1", "revision": 1},
    {"member_idx": 1, "member_id": "m2", "revision": 1},
    {"member_idx": 2, "member_id": "m3", "revision": 1}
  ],
  "thresholds": [
    {
      "threshold": 5.0,
      "probability": [[0.6666666666666666, 0.6666666666666666], [0.5, 0.0]],
      "denominator": [[3, 3], [2, 2]]
    }
  ]
}
```

错误统一为 `{"error": {"code": "...", "message": "..."}}`：
`VALIDATION_ERROR(422)`、`GRID_MISMATCH(422)`、`NEGATIVE_RAIN(422)`、
`REVISION_CONFLICT(409)`、`REVISION_CONTENT_CONFLICT(409)`、`MEMBERS_INCOMPLETE(409)`、
`NO_CURRENT_PRODUCT(404)`、`GROUP_NOT_FOUND/MEMBER_NOT_FOUND/PRODUCT_NOT_FOUND(404)`、
`COMPUTE_FAILED(500)`、`DATABASE_BUSY(503)`，以及跨时次分析的
`TIME_SEQUENCE_INVALID(422)`、`THRESHOLD_MISMATCH(422)`、
`ANALYSIS_SOURCE_MISSING(409)`、`ANALYSIS_SOURCE_CHANGED(409)`。

## 3. 运行

### Docker Compose（双实例）

```bash
docker compose up --build
# api-a: http://localhost:8081   api-b: http://localhost:8082
# Swagger: http://localhost:8081/docs 或 http://localhost:8082/docs
# SQLite 文件在命名卷 rain-data 中，两实例共享（WAL 模式）
```

### 本地 Python（无 Docker）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./run_local.sh                 # 起两个 uvicorn：8081 / 8082，共享 ./data/rainfall.db
```

环境变量：`RAIN_DB_PATH`（默认 `/data/rainfall.db`）、`RAIN_BUSY_TIMEOUT_MS`（默认 10000）。
另有两个**默认关闭**的测试钩子：`RAIN_TEST_PUBLISH_DELAY_S`（发布计算延时）、
`RAIN_TEST_FAIL_MARKER`（该文件存在时发布计算确定性失败），仅用于自测，生产不要设置。
跨时次分析另有 `RAIN_TEST_ANALYSIS_DELAY_S`（分析计算延时，默认关闭），用于自测交错一致性。

## 4. 自测

`tests/selftest.py` 只用标准库，默认自动拉起两个共享同一 SQLite 的 uvicorn 实例并跑 105 项断言：

```bash
python tests/selftest.py
# 也可对已运行实例执行：
RAIN_BASE_A=http://127.0.0.1:8081 RAIN_BASE_B=http://127.0.0.1:8082 python tests/selftest.py
```

覆盖：参数校验、幂等/异文拒收、修订号顺序、成员齐全门禁、概率与分母数学、全缺测空值、
更正即过期与历史可查、双实例交错发布一致性、并行发布幂等、故障注入下无半份产品、
慢计算不阻塞其他时次入仓、Swagger/OpenAPI 可用性；跨时次分析还覆盖连续两轮语义、
概率空值中断连续、阈值/警戒线边界、2 与 6 上限、时间顺序与 1 小时间隔校验、
来源产品缺失/网格不匹配/阈值不匹配拒绝、分析期间成员更正或更正后重新发布则整体作废并提示重试、
分析计算不持写锁且不阻塞其他时次入仓。

## 5. 目录结构

```
app/
  main.py       FastAPI 路由、统一错误处理、启动建表
  schemas.py    Pydantic 请求模型与结构校验
  service.py    登记/提交/发布/查询事务逻辑、跨时次持续高风险分析与并发控制
  storage.py    网格序列化、内容哈希、NumPy 概率计算
  db.py         SQLite 连接、WAL、schema
  errors.py     统一 API 异常
tests/selftest.py   端到端自测（双实例）
Dockerfile / docker-compose.yml / requirements.txt / run_local.sh
```
