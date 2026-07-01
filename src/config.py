import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Polymarket API Configuration
GAMMA_API_URL = os.getenv("GAMMA_API_URL", "https://gamma-api.polymarket.com")
DATA_API_URL = os.getenv("DATA_API_URL", "https://data-api.polymarket.com")
TRADE_API_URL = os.getenv("TRADE_API_URL", "https://api.polymarket.com")

# Relayer API (for submitting trades without gas)
RELAYER_API_KEY = os.getenv("RELAYER_API_KEY", "")
RELAYER_WALLET_ADDRESS = os.getenv("RELAYER_WALLET_ADDRESS", "")

# Trading Configuration
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "10000"))
MAX_POSITION_SIZE = float(os.getenv("MAX_POSITION_SIZE", "1000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.02"))  # 2% risk per trade

# Data Configuration
DATA_DB_PATH = os.getenv("DATA_DB_PATH", "data/trading.db")
HISTORICAL_DATA_DAYS = int(os.getenv("HISTORICAL_DATA_DAYS", "365"))

# ML Configuration
ML_MODEL_PATH = os.getenv("ML_MODEL_PATH", "models/predictor.pkl")
PREDICTION_THRESHOLD = float(os.getenv("PREDICTION_THRESHOLD", "0.55"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/trading.log")

# Strategy Configuration
STRATEGY_NAME = os.getenv("STRATEGY_NAME", "momentum")
PAPER_TRADING = os.getenv("PAPER_TRADING", "true").lower() == "true"
