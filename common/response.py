from typing import Any, Optional


class ApiResponse:
    """统一API响应格式"""

    @staticmethod
    def ok(data: Any = None, message: str = "success") -> dict:
        return {"code": 200, "message": message, "data": data}

    @staticmethod
    def fail(code: int = 400, message: str = "error", data: Any = None) -> dict:
        return {"code": code, "message": message, "data": data}
