import os
import sys
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("PORTAL_API_URL", "https://portal.example.org")
os.environ.setdefault("PORTAL_SECRET_ARN", "arn:aws:secretsmanager:us-west-2:123456789012:secret:test")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lambda" / "tagging_handler"))
