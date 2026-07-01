"""Machine learning models for price prediction."""

import pickle
import numpy as np
import pandas as pd
from typing import Tuple, Optional
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from loguru import logger

from src.config import ML_MODEL_PATH, PREDICTION_THRESHOLD


class PricePredictor:
    """ML model for predicting price movements (up/down)."""

    def __init__(self, model_path: str = ML_MODEL_PATH):
        """Initialize predictor."""
        self.model_path = model_path
        self.model = None
        self.scaler = StandardScaler()
        self.feature_names = [
            "sma_5", "sma_20", "rsi", "macd", "bb_position",
            "volatility", "volume_ratio", "momentum"
        ]

    def _create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features from OHLCV data."""
        df = df.copy()
        
        # Moving averages
        df["sma_5"] = df["close"].rolling(5).mean()
        df["sma_20"] = df["close"].rolling(20).mean()
        
        # RSI
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # MACD
        ema_12 = df["close"].ewm(span=12).mean()
        ema_26 = df["close"].ewm(span=26).mean()
        df["macd"] = ema_12 - ema_26
        
        # Bollinger Bands
        sma = df["close"].rolling(20).mean()
        std = df["close"].rolling(20).std()
        df["bb_upper"] = sma + (std * 2)
        df["bb_lower"] = sma - (std * 2)
        df["bb_position"] = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])
        
        # Volatility
        df["volatility"] = df["close"].rolling(20).std()
        
        # Volume ratio
        df["volume_ratio"] = df["volume"] / df["volume"].rolling(20).mean()
        
        # Momentum
        df["momentum"] = df["close"].diff(10)
        
        return df

    def create_training_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """Create training features and labels from OHLCV data."""
        df = self._create_features(df)
        
        # Create labels: 1 if price goes up tomorrow, 0 otherwise
        df["next_close"] = df["close"].shift(-1)
        df["label"] = (df["next_close"] > df["close"]).astype(int)
        
        # Drop NaN rows
        df = df.dropna()
        
        X = df[self.feature_names].values
        y = df["label"].values
        
        return X, y

    def train(self, X: np.ndarray, y: np.ndarray):
        """Train the prediction model."""
        try:
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Train Random Forest
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            self.model.fit(X_scaled, y)
            
            # Save model
            self._save_model()
            logger.info("Model trained and saved")
        except Exception as e:
            logger.error(f"Error training model: {e}")

    def predict(self, df: pd.DataFrame) -> float:
        """Predict probability of price going up."""
        if self.model is None:
            self._load_model()
        
        if self.model is None:
            logger.warning("No model available")
            return 0.5
        
        try:
            df = self._create_features(df)
            df = df.dropna()
            
            if len(df) == 0:
                return 0.5
            
            # Use latest row
            X = df[self.feature_names].iloc[-1:].values
            X_scaled = self.scaler.transform(X)
            
            # Get probability of up movement
            proba = self.model.predict_proba(X_scaled)[0]
            return float(proba[1])  # Probability of class 1 (up)
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            return 0.5

    def should_buy(self, df: pd.DataFrame) -> bool:
        """Determine if we should buy (prediction > threshold)."""
        proba = self.predict(df)
        return proba > PREDICTION_THRESHOLD

    def should_sell(self, df: pd.DataFrame) -> bool:
        """Determine if we should sell (prediction < threshold)."""
        proba = self.predict(df)
        return proba < (1 - PREDICTION_THRESHOLD)

    def _save_model(self):
        """Save model to disk."""
        try:
            with open(self.model_path, "wb") as f:
                pickle.dump({"model": self.model, "scaler": self.scaler}, f)
            logger.info(f"Model saved to {self.model_path}")
        except Exception as e:
            logger.error(f"Error saving model: {e}")

    def _load_model(self):
        """Load model from disk."""
        try:
            with open(self.model_path, "rb") as f:
                data = pickle.load(f)
                self.model = data["model"]
                self.scaler = data["scaler"]
            logger.info(f"Model loaded from {self.model_path}")
        except Exception as e:
            logger.warning(f"Could not load model: {e}")
