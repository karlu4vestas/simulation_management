import uvicorn
from app.config import set_test_mode, TestMode


if __name__ == "__main__":
    # Set test mode for main.py execution
    # Change this to TestMode.CLIENT_TEST or TestMode.PRODUCTION as needed
    set_test_mode(TestMode.CLIENT_TEST)
    
    uvicorn.run("app.web_api:app", host="0.0.0.0", port=5173, reload=True)
