import uvicorn
from app.config import set_test_mode, TestMode


if __name__ == "__main__":
    
    uvicorn.run("app.web_api:app", host="0.0.0.0", port=5173, reload=True)
