"""
Wallet & API Key Management - Secure credential handling for Polymarket

Handles:
- Encrypted storage of API keys
- Wallet address management
- Private key handling
- Balance tracking
- Multi-wallet support
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, List
from datetime import datetime
import json
import os
from pathlib import Path
from cryptography.fernet import Fernet
from loguru import logger

from src.polymarket.client import PolymarketClient


class WalletType(str, Enum):
    """Supported wallet types"""
    METAMASK = "metamask"
    WALLET_CONNECT = "wallet_connect"
    PRIVATE_KEY = "private_key"
    API_KEY = "api_key"


class CredentialType(str, Enum):
    """Types of credentials"""
    API_KEY = "api_key"
    API_SECRET = "api_secret"
    PRIVATE_KEY = "private_key"
    MNEMONIC = "mnemonic"


@dataclass
class Credential:
    """Encrypted credential storage"""
    
    credential_type: CredentialType
    name: str  # e.g., 'polymarket_api_key'
    encrypted_value: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    is_active: bool = True
    
    def marshal(self) -> Dict:
        """Convert to dict for storage"""
        return {
            'credential_type': self.credential_type.value,
            'name': self.name,
            'encrypted_value': self.encrypted_value,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'is_active': self.is_active
        }


@dataclass
class Wallet:
    """Wallet configuration"""
    
    wallet_id: str
    wallet_type: WalletType
    address: str
    network: str  # e.g., 'ethereum', 'polygon', 'mainnet'
    
    # Balances
    usdc_balance: float = 0.0  # Polymarket uses USDC
    eth_balance: float = 0.0
    
    # Tracking
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_sync: Optional[datetime] = None
    total_trades: int = 0
    total_deployed: float = 0.0
    realized_pnl: float = 0.0
    
    # Settings
    max_trade_size: float = 5000.0  # Max per trade
    daily_limit: float = 50000.0    # Max per day
    is_active: bool = True
    
    def marshal(self) -> Dict:
        """Convert to dict for storage"""
        return {
            'wallet_id': self.wallet_id,
            'wallet_type': self.wallet_type.value,
            'address': self.address,
            'network': self.network,
            'usdc_balance': self.usdc_balance,
            'eth_balance': self.eth_balance,
            'created_at': self.created_at.isoformat(),
            'last_sync': self.last_sync.isoformat() if self.last_sync else None,
            'total_trades': self.total_trades,
            'total_deployed': self.total_deployed,
            'realized_pnl': self.realized_pnl,
            'max_trade_size': self.max_trade_size,
            'daily_limit': self.daily_limit,
            'is_active': self.is_active
        }


@dataclass
class WalletValidation:
    """Wallet validation result"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class CredentialManager:
    """Manages encrypted credentials"""
    
    def __init__(self, key_file: Optional[str] = None):
        """
        Initialize credential manager
        
        Args:
            key_file: Path to encryption key file (will generate if not found)
        """
        self.key_file = key_file or ".credentials_key"
        self.key = self._load_or_create_key()
        self.cipher = Fernet(self.key)
        self.credentials: Dict[str, Credential] = {}
        
        logger.info("✅ CredentialManager initialized")
    
    def _load_or_create_key(self) -> bytes:
        """Load or create encryption key"""
        if os.path.exists(self.key_file):
            with open(self.key_file, 'rb') as f:
                return f.read()
        
        # Generate new key
        key = Fernet.generate_key()
        with open(self.key_file, 'wb') as f:
            f.write(key)
        
        # Secure file permissions
        os.chmod(self.key_file, 0o600)
        logger.info(f"✅ Generated encryption key at {self.key_file}")
        
        return key
    
    def encrypt(self, value: str) -> str:
        """Encrypt a value"""
        return self.cipher.encrypt(value.encode()).decode()
    
    def decrypt(self, encrypted: str) -> str:
        """Decrypt a value"""
        return self.cipher.decrypt(encrypted.encode()).decode()
    
    def store_credential(
        self,
        name: str,
        value: str,
        credential_type: CredentialType,
        expires_at: Optional[datetime] = None
    ) -> Credential:
        """
        Store an encrypted credential
        
        Args:
            name: Credential name (e.g., 'polymarket_api_key')
            value: Credential value
            credential_type: Type of credential
            expires_at: Optional expiration datetime
            
        Returns:
            Credential object
        """
        encrypted = self.encrypt(value)
        credential = Credential(
            credential_type=credential_type,
            name=name,
            encrypted_value=encrypted,
            expires_at=expires_at
        )
        
        self.credentials[name] = credential
        logger.info(f"✅ Credential stored: {name}")
        return credential
    
    def get_credential(self, name: str) -> Optional[str]:
        """
        Retrieve and decrypt a credential
        
        Args:
            name: Credential name
            
        Returns:
            Decrypted value or None if not found
        """
        if name not in self.credentials:
            logger.warning(f"Credential not found: {name}")
            return None
        
        credential = self.credentials[name]
        
        if credential.expires_at and datetime.utcnow() > credential.expires_at:
            logger.warning(f"Credential expired: {name}")
            return None
        
        if not credential.is_active:
            logger.warning(f"Credential inactive: {name}")
            return None
        
        return self.decrypt(credential.encrypted_value)
    
    def list_credentials(self) -> List[str]:
        """List all credential names (safe, no values)"""
        return list(self.credentials.keys())
    
    def revoke_credential(self, name: str) -> bool:
        """Revoke a credential"""
        if name in self.credentials:
            self.credentials[name].is_active = False
            logger.info(f"✅ Credential revoked: {name}")
            return True
        return False
    
    def delete_credential(self, name: str) -> bool:
        """Delete a credential"""
        if name in self.credentials:
            del self.credentials[name]
            logger.info(f"✅ Credential deleted: {name}")
            return True
        return False
    
    def save_to_file(self, filepath: str):
        """Save credentials to encrypted file"""
        credentials_dict = {
            name: cred.marshal()
            for name, cred in self.credentials.items()
        }
        
        with open(filepath, 'w') as f:
            json.dump(credentials_dict, f, indent=2)
        
        # Secure file permissions
        os.chmod(filepath, 0o600)
        logger.info(f"✅ Credentials saved to {filepath}")
    
    def load_from_file(self, filepath: str):
        """Load credentials from encrypted file"""
        if not os.path.exists(filepath):
            logger.warning(f"Credentials file not found: {filepath}")
            return
        
        with open(filepath, 'r') as f:
            credentials_dict = json.load(f)
        
        for name, cred_data in credentials_dict.items():
            credential = Credential(
                credential_type=CredentialType(cred_data['credential_type']),
                name=cred_data['name'],
                encrypted_value=cred_data['encrypted_value'],
                created_at=datetime.fromisoformat(cred_data['created_at']),
                updated_at=datetime.fromisoformat(cred_data['updated_at']),
                expires_at=datetime.fromisoformat(cred_data['expires_at']) if cred_data['expires_at'] else None,
                is_active=cred_data['is_active']
            )
            self.credentials[name] = credential
        
        logger.info(f"✅ Loaded {len(self.credentials)} credentials from {filepath}")


class WalletManager:
    """Manages wallets and balances"""
    
    def __init__(self, client: PolymarketClient, credential_manager: CredentialManager):
        """
        Initialize wallet manager
        
        Args:
            client: PolymarketClient instance
            credential_manager: CredentialManager instance
        """
        self.client = client
        self.cred_manager = credential_manager
        self.wallets: Dict[str, Wallet] = {}
        self.primary_wallet: Optional[str] = None
        
        logger.info("✅ WalletManager initialized")
    
    def create_wallet(
        self,
        wallet_id: str,
        wallet_type: WalletType,
        address: str,
        network: str = 'ethereum',
        max_trade_size: float = 5000.0,
        daily_limit: float = 50000.0
    ) -> Wallet:
        """
        Create a new wallet configuration
        
        Args:
            wallet_id: Unique wallet identifier
            wallet_type: Type of wallet
            address: Wallet address
            network: Network name
            max_trade_size: Max per trade
            daily_limit: Max per day
            
        Returns:
            Wallet object
        """
        wallet = Wallet(
            wallet_id=wallet_id,
            wallet_type=wallet_type,
            address=address,
            network=network,
            max_trade_size=max_trade_size,
            daily_limit=daily_limit
        )
        
        self.wallets[wallet_id] = wallet
        
        if not self.primary_wallet:
            self.primary_wallet = wallet_id
        
        logger.info(f"✅ Wallet created: {wallet_id} ({address})")
        return wallet
    
    def validate_wallet(self, wallet_id: str) -> WalletValidation:
        """
        Validate wallet configuration
        
        Args:
            wallet_id: Wallet ID to validate
            
        Returns:
            WalletValidation result
        """
        errors = []
        warnings = []
        
        if wallet_id not in self.wallets:
            errors.append(f"Wallet not found: {wallet_id}")
            return WalletValidation(is_valid=False, errors=errors)
        
        wallet = self.wallets[wallet_id]
        
        # Check address format
        if not wallet.address or len(wallet.address) != 42:  # Ethereum address length
            errors.append(f"Invalid address format: {wallet.address}")
        
        # Check balance
        if wallet.usdc_balance <= 0:
            warnings.append(f"USDC balance is zero or negative")
        
        # Check if balance is sufficient for daily limit
        if wallet.usdc_balance < wallet.daily_limit * 0.1:
            warnings.append(f"USDC balance low for daily operations")
        
        return WalletValidation(is_valid=len(errors) == 0, errors=errors, warnings=warnings)
    
    def sync_wallet_balance(self, wallet_id: str) -> bool:
        """
        Sync wallet balance from blockchain
        
        Args:
            wallet_id: Wallet ID to sync
            
        Returns:
            True if successful
        """
        if wallet_id not in self.wallets:
            logger.error(f"Wallet not found: {wallet_id}")
            return False
        
        try:
            wallet = self.wallets[wallet_id]
            
            # Query blockchain for balances
            balances = self.client.get_wallet_balances(wallet.address)
            
            # Update wallet
            wallet.usdc_balance = balances.get('usdc', 0.0)
            wallet.eth_balance = balances.get('eth', 0.0)
            wallet.last_sync = datetime.utcnow()
            
            logger.info(
                f"✅ Wallet synced: {wallet_id} "
                f"(USDC: ${wallet.usdc_balance:.2f}, ETH: {wallet.eth_balance:.4f})"
            )
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to sync wallet {wallet_id}: {e}")
            return False
    
    def get_primary_wallet(self) -> Optional[Wallet]:
        """Get the primary wallet"""
        if self.primary_wallet and self.primary_wallet in self.wallets:
            return self.wallets[self.primary_wallet]
        return None
    
    def set_primary_wallet(self, wallet_id: str) -> bool:
        """Set the primary wallet"""
        if wallet_id in self.wallets:
            self.primary_wallet = wallet_id
            logger.info(f"✅ Primary wallet set to: {wallet_id}")
            return True
        return False
    
    def list_wallets(self) -> List[Wallet]:
        """Get all wallets"""
        return list(self.wallets.values())
    
    def can_trade(self, wallet_id: str, trade_size: float) -> tuple[bool, str]:
        """
        Check if wallet can execute a trade
        
        Args:
            wallet_id: Wallet ID
            trade_size: Trade size in USD
            
        Returns:
            (can_trade: bool, reason: str)
        """
        if wallet_id not in self.wallets:
            return False, f"Wallet not found: {wallet_id}"
        
        wallet = self.wallets[wallet_id]
        
        if not wallet.is_active:
            return False, "Wallet is inactive"
        
        if trade_size > wallet.max_trade_size:
            return False, f"Trade size ${trade_size} exceeds max ${wallet.max_trade_size}"
        
        if wallet.total_deployed + trade_size > wallet.daily_limit:
            return False, f"Daily limit would be exceeded (${wallet.total_deployed} + ${trade_size} > ${wallet.daily_limit})"
        
        if wallet.usdc_balance < trade_size:
            return False, f"Insufficient USDC balance (${wallet.usdc_balance} < ${trade_size})"
        
        return True, "OK"
    
    def record_trade(self, wallet_id: str, trade_size: float, realized_pnl: float = 0.0):
        """
        Record a trade for a wallet
        
        Args:
            wallet_id: Wallet ID
            trade_size: Amount deployed
            realized_pnl: Realized profit/loss
        """
        if wallet_id in self.wallets:
            wallet = self.wallets[wallet_id]
            wallet.total_trades += 1
            wallet.total_deployed += trade_size
            wallet.realized_pnl += realized_pnl
            wallet.usdc_balance -= trade_size
            
            logger.info(
                f"✅ Trade recorded for {wallet_id}: "
                f"${trade_size} deployed, PnL: ${realized_pnl} "
                f"(Balance: ${wallet.usdc_balance})"
            )
    
    def get_total_balance(self) -> Dict[str, float]:
        """Get total balances across all wallets"""
        total_usdc = sum(w.usdc_balance for w in self.wallets.values())
        total_eth = sum(w.eth_balance for w in self.wallets.values())
        total_deployed = sum(w.total_deployed for w in self.wallets.values())
        total_pnl = sum(w.realized_pnl for w in self.wallets.values())
        
        return {
            'total_usdc': total_usdc,
            'total_eth': total_eth,
            'total_deployed': total_deployed,
            'total_realized_pnl': total_pnl,
            'num_wallets': len(self.wallets)
        }
    
    def export_wallets(self, filepath: str):
        """Export wallet configurations to JSON"""
        wallets_dict = {
            wid: wallet.marshal()
            for wid, wallet in self.wallets.items()
        }
        
        with open(filepath, 'w') as f:
            json.dump(wallets_dict, f, indent=2)
        
        logger.info(f"✅ Wallets exported to {filepath}")
    
    def import_wallets(self, filepath: str):
        """Import wallet configurations from JSON"""
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            return
        
        with open(filepath, 'r') as f:
            wallets_dict = json.load(f)
        
        for wid, wallet_data in wallets_dict.items():
            wallet = Wallet(
                wallet_id=wallet_data['wallet_id'],
                wallet_type=WalletType(wallet_data['wallet_type']),
                address=wallet_data['address'],
                network=wallet_data['network'],
                usdc_balance=wallet_data['usdc_balance'],
                eth_balance=wallet_data['eth_balance'],
                created_at=datetime.fromisoformat(wallet_data['created_at']),
                last_sync=datetime.fromisoformat(wallet_data['last_sync']) if wallet_data['last_sync'] else None,
                total_trades=wallet_data['total_trades'],
                total_deployed=wallet_data['total_deployed'],
                realized_pnl=wallet_data['realized_pnl'],
                max_trade_size=wallet_data['max_trade_size'],
                daily_limit=wallet_data['daily_limit'],
                is_active=wallet_data['is_active']
            )
            self.wallets[wid] = wallet
        
        logger.info(f"✅ Imported {len(self.wallets)} wallets from {filepath}")
