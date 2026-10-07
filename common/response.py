from fastapi.responses import JSONResponse


class ApiResponse:
    @staticmethod
    def ok(data=None, message='success'):
        return {'code': 200, 'message': message, 'data': data}

    @staticmethod
    def fail(code=400, message='error', data=None):
        return JSONResponse(status_code=code, content={'code': code, 'message': message, 'data': data})
