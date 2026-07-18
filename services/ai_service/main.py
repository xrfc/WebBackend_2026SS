"""AI智能导购服务 - LLM驱动的商品咨询与购买建议"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import httpx
from fastapi import FastAPI, Depends
from pydantic import BaseModel, Field

from openai import AsyncOpenAI

from common.config import settings
from common.auth import get_current_user
from common.response import ApiResponse


# ==================== OpenAI客户端 ====================

ai_client = AsyncOpenAI(
    api_key=settings.LLM_API_KEY,
    base_url=settings.LLM_BASE_URL,
)


# ==================== 请求模型 ====================

class ConsultRequest(BaseModel):
    product_id: int = Field(..., gt=0, description="咨询的商品ID")
    question: str = Field(..., min_length=1, max_length=500, description="用户问题")


# ==================== FastAPI 应用 ====================

app = FastAPI(title="AI Service", version="1.0.0",
              description="AI智能导购服务 - LLM商品咨询")


# ==================== 系统提示词 ====================

SYSTEM_PROMPT = """你是一个专业的电商导购助手。你的任务是根据用户提供的商品信息，
回答用户关于该商品的问题，并给出专业的购买建议。

在回答时请注意：
1. 结合商品的具体参数（名称、价格、描述等）进行分析
2. 回答要简洁专业，重点突出
3. 如果有明显的优缺点，请客观指出
4. 最后给出一个明确的购买建议（推荐/谨慎/不推荐）
5. 回答控制在200字以内"""


# ==================== API接口 ====================

@app.post("/ai/consult", summary="AI智能导购咨询")
async def ai_consult(
    req: ConsultRequest,
    user: dict = Depends(get_current_user)
):
    """获取商品信息，结合用户问题调用LLM生成购买建议"""

    # 1. 从商品服务获取商品信息
    product_info = None
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{settings.PRODUCT_SERVICE_URL}/products/{req.product_id}"
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 200:
                    product_info = data["data"]
    except Exception:
        pass

    if not product_info:
        return ApiResponse.fail(404, "商品信息获取失败，请确认商品ID是否正确")

    # 2. 构建提示词
    product_desc = (
        f"商品名称：{product_info['name']}\n"
        f"商品描述：{product_info['description'] or '暂无描述'}\n"
        f"商品价格：¥{product_info['price']}\n"
        f"当前库存：{product_info['stock']}件\n"
        f"商品状态：{'在售' if product_info['status'] == 'on_sale' else '已下架'}"
    )

    user_message = f"以下是用户正在咨询的商品信息：\n\n{product_desc}\n\n用户的提问是：{req.question}\n\n请根据以上信息回答用户的问题并给出购买建议。"

    # 3. 调用LLM
    try:
        response = await ai_client.chat.completions.create(
            model=settings.LLM_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.7,
            max_tokens=500,
        )
        ai_answer = response.choices[0].message.content
    except Exception as e:
        # LLM调用失败时的降级回复
        ai_answer = (
            f"关于「{product_info['name']}」的回答：\n\n"
            f"该商品当前售价 ¥{product_info['price']}，{product_info['description'] or '暂无详细描述。'}"
            f"\n\n建议您根据自身需求决定是否购买。"
            f"\n（AI助手暂时繁忙，以上为基础信息回复）"
        )

    return ApiResponse.ok({
        "product_id": req.product_id,
        "product_name": product_info["name"],
        "question": req.question,
        "answer": ai_answer,
    })


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
