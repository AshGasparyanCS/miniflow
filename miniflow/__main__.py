"""Run the API server: python -m miniflow"""
import os
import uvicorn
from .api import create_app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
