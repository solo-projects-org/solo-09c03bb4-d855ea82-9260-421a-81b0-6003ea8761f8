"""FastAPI application: ensemble rainfall exceedance-probability API.

Interactive docs:  GET /docs   (Swagger UI)
OpenAPI schema:    GET /openapi.json
Replacement docs: GET /redoc
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import __version__
from .db import init_db
from .errors import ApiError
from .schemas import CrossTimeAnalysisRequest, RegisterRequest, SubmitRequest
from . import service

app = FastAPI(
    title="集合降雨超阈值概率 API",
    description=(
        "防汛值班用：登记 UTC 预报时次与集合成员，逐成员提交雨量网格修订，"
        "成员齐全后发布逐格超阈值概率产品。支持双实例共享 SQLite 并发入仓与发布。"
    ),
    version=__version__,
)


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.exception_handler(ApiError)
async def _api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


@app.exception_handler(RequestValidationError)
async def _validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Uniform 422 envelope for every request-body/query validation failure.
    details = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", []) if p != "body") or "body"
        details.append({"field": loc, "message": err.get("msg", "invalid request")})
    first = details[0]["message"] if details else "request validation failed"
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "VALIDATION_ERROR", "message": first, "details": details}},
    )


@app.get("/health", tags=["meta"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "version": __version__}


@app.post("/groups", tags=["groups"], status_code=201, summary="登记预报时次与集合成员")
def register_group(payload: RegisterRequest) -> dict:
    return service.register_group(payload)


@app.get("/groups", tags=["groups"], summary="列出已登记时次")
def list_groups() -> dict:
    return service.list_groups()


@app.get("/groups/{group_id}", tags=["groups"], summary="查询时次状态与成员修订")
def get_group(group_id: str) -> dict:
    return service.get_group_status(group_id)


@app.put(
    "/groups/{group_id}/members/{member_id}",
    tags=["members"],
    summary="提交/更正成员雨量网格（递增修订号）",
)
def submit_member(group_id: str, member_id: str, payload: SubmitRequest) -> dict:
    return service.submit_member(group_id, member_id, payload)


@app.post(
    "/groups/{group_id}/publish",
    tags=["products"],
    summary="成员齐全后发布超阈值概率产品",
)
def publish(group_id: str) -> dict:
    return service.publish_product(group_id)


@app.get(
    "/groups/{group_id}/products/current",
    tags=["products"],
    summary="查询当前可用于预警的概率产品",
)
def current_product(group_id: str) -> dict:
    return service.get_current_product(group_id)


@app.get(
    "/groups/{group_id}/products",
    tags=["products"],
    summary="列出该时次全部历史产品（含已过期）",
)
def list_products(group_id: str) -> dict:
    return service.list_products(group_id)


@app.get(
    "/groups/{group_id}/products/{product_id}",
    tags=["products"],
    summary="按 ID 查询历史产品",
)
def get_product(group_id: str, product_id: int) -> dict:
    return service.get_product(group_id, product_id)


@app.post(
    "/analysis/persistent-high-risk",
    tags=["analysis"],
    summary="跨时次识别持续高风险网格（连续两轮达标）",
    description=(
        "按 UTC 时间升序传入 2～6 个相邻间隔 1 小时的时次 ID，读取各时次现行产品，"
        "在同一网格上找出连续两轮概率均非空且 ≥ 概率警戒线的格，"
        "返回该格首次确认的时次（1 起）及所用产品 ID。"
        "概率为空会中断连续判定；产品缺失、网格或阈值不匹配则拒绝分析；"
        "计算期间来源产品被更正/撤回/重发时返回 409 ANALYSIS_SOURCE_CHANGED，提示重试。"
    ),
    responses={
        200: {"description": "分析完成；first_confirmed_* 矩阵中未命中的格为 null"},
        409: {
            "description": (
                "来源产品不可用或在计算期间变化：产品缺失/被撤回 -> ANALYSIS_SOURCE_MISSING；"
                "成员更正或重新发布 -> ANALYSIS_SOURCE_CHANGED（重试即可）"
            )
        },
        422: {
            "description": (
                "请求或数据不匹配：时间顺序/间隔 -> TIME_SEQUENCE_INVALID；"
                "网格 -> GRID_MISMATCH；雨量阈值 -> THRESHOLD_MISMATCH；字段 -> VALIDATION_ERROR"
            )
        },
    },
)
def analyze(payload: CrossTimeAnalysisRequest) -> dict:
    return service.analyze_across_times(payload)
