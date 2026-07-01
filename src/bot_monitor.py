"""Monitor trading bots for crashes and send alerts."""

import asyncio
import subprocess
import time
import os
from datetime import datetime
from loguru import logger

from src.config import LOG_LEVEL

# Note: discord_webhook import moved to send_crash_alert() method
# to avoid module-level hang with aiohttp initialization

# Configure logging with error handling
try:
    LOG_FILE = os.getenv("MONITOR_LOG_FILE", "logs/bot_monitor.log")
    os.makedirs(os.path.dirname(LOG_FILE) or "logs", exist_ok=True)
    
    logger.remove()
    
    logger.add(
        LOG_FILE,
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )
    
    logger.add(
        lambda msg: print(msg.rstrip(), flush=True),
        level=LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    )
except Exception as logger_err:
    logger.error(f"❌ Logger setup failed: {logger_err}")


class BotMonitor:
    """Monitor trading bots and alert on crashes."""
    
    def __init__(self):
        """Initialize bot monitor."""
        self.bots = {
            "BTC": {"cmd": ["python3", "-m", "src.main"], "name": "BTC Bot"},
            "POLYMARKET": {"cmd": ["python3", "-m", "src.main_polymarket"], "name": "Polymarket Bot"},
            "KAKASHI": {"cmd": ["python3", "-m", "src.main_kakashi"], "name": "Kakashi Bot"},
            "WEBHOOK": {"cmd": ["python3", "-m", "src.main_webhook"], "name": "TradingView Webhook"},
            # ETH Market Maker removed — wrong strategy for prediction markets
            # Weather Bot removed — 7.6% win rate, no edge
            # KrakenPolyArb removed — replaced by Top Trader Follower (main_top_follower)
        }
        self.bot_pids = {}  # Track current PIDs
        self.crash_count = {}  # Track crashes per bot
        # Per-bot restart backoff: seconds to wait before the next restart attempt.
        # Starts at 5s, doubles on each consecutive crash, caps at 5 minutes.
        # Resets to 5s when the bot runs cleanly for a full check cycle.
        self._restart_backoff: dict = {}
        self.cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Initialize crash counts
        for bot_key in self.bots:
            self.crash_count[bot_key] = 0
            self._restart_backoff[bot_key] = 5

    def is_bot_running(self, bot_key: str) -> bool:
        """Check if bot process is still running."""
        if bot_key not in self.bot_pids or self.bot_pids[bot_key] is None:
            logger.debug(f"is_bot_running({bot_key}) - no PID stored")
            return False
        
        pid = self.bot_pids[bot_key]
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "pid="],
                capture_output=True,
                text=True,
                timeout=2
            )
            is_running = bool(result.stdout.strip())
            logger.debug(f"is_bot_running({bot_key}, PID={pid}) = {is_running}")
            return is_running
        except Exception as e:
            logger.debug(f"is_bot_running({bot_key}) check failed: {type(e).__name__}")
            return False

    def get_bot_pid(self, bot_key: str) -> int:
        """Get PID of running bot from ps."""
        try:
            logger.debug(f"get_bot_pid({bot_key}) - searching ps aux")
            # Map bot keys to their unique module search strings and optional excludes
            search_map = {
                "BTC":        ("src.main",            ["main_eth", "main_poly", "main_kak", "main_web"]),
                "ETH":        ("src.main_eth",         []),
                "POLYMARKET": ("src.main_polymarket",  []),
                "KAKASHI":    ("src.main_kakashi",     []),
                "WEBHOOK":    ("src.main_webhook",     []),
            }
            if bot_key not in search_map:
                logger.debug(f"Unknown bot_key: {bot_key}")
                return None
            search, excludes = search_map[bot_key]
            
            logger.debug(f"Searching for: {search}")
            result = subprocess.run(
                ["ps", "aux"],
                capture_output=True,
                text=True,
                timeout=2
            )

            logger.debug(f"ps aux returned {len(result.stdout.split(chr(10)))} lines")
            for line in result.stdout.split('\n'):
                if search in line and 'grep' not in line:
                    if any(ex in line for ex in excludes):
                        continue
                    parts = line.split()
                    if len(parts) > 1:
                        try:
                            pid = int(parts[1])
                            logger.debug(f"Found PID {pid} for {bot_key}")
                            return pid
                        except ValueError:
                            pass
            logger.debug(f"No PID found for {bot_key}")
            return None
        except Exception as e:
            logger.debug(f"get_bot_pid({bot_key}) failed: {type(e).__name__}: {e}")
            return None

    async def send_crash_alert(self, bot_key: str, restart_attempt: bool = False):
        """Send Discord alert for bot crash."""
        try:
            # Import here to avoid module-level hang with aiohttp
            from src.notifications.discord_webhook import get_discord_client
            
            discord = get_discord_client()
            if not discord.enabled:
                logger.warning(f"Discord not configured, skipping alert for {bot_key}")
                return
            
            bot_name = self.bots[bot_key]["name"]
            status = "🔄 RESTARTING..." if restart_attempt else "💀 CRASHED"
            
            # Create embed for rich formatting
            embed = {
                "title": f"{status} {bot_name}",
                "description": f"Bot process died at {datetime.now().strftime('%H:%M:%S')}",
                "color": 0xFF0000 if not restart_attempt else 0xFFAA00,  # Red or Orange
                "fields": [
                    {"name": "Crash Count", "value": str(self.crash_count[bot_key]), "inline": True},
                    {"name": "Strategy", "value": "4-Hour Bollinger Bands + RSI", "inline": True},
                    {"name": "Action", "value": "🔄 Auto-restarting..." if restart_attempt else "⚠️ Manual restart needed", "inline": False},
                ],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            await discord.send_message(embed=embed)
            logger.warning(f"Sent Discord alert for {bot_name}")
        except Exception as e:
            logger.error(f"Failed to send crash alert: {e}")

    def restart_bot(self, bot_key: str) -> bool:
        """Attempt to restart crashed bot with proper resource management."""
        logger.debug(f"restart_bot({bot_key}) - starting")
        try:
            cmd = self.bots[bot_key]["cmd"]
            logger.debug(f"restart_bot({bot_key}) - cmd: {' '.join(cmd)}")
            
            # Determine log file
            if bot_key == "BTC":
                log_file = "logs/trading.log"
            elif bot_key == "ETH":
                log_file = "logs/trading_eth.log"
            elif bot_key == "POLYMARKET":
                log_file = "logs/trading_polymarket.log"
            elif bot_key == "WEATHER":
                log_file = "logs/trading_weather.log"
            else:
                log_file = f"logs/trading_{bot_key.lower()}.log"
            
            log_path = os.path.join(self.cwd, log_file)
            logger.debug(f"restart_bot({bot_key}) - log path: {log_path}")
            
            # Ensure log directory exists
            os.makedirs(os.path.dirname(log_path) or "logs", exist_ok=True)
            logger.debug(f"restart_bot({bot_key}) - log directory created")
            
            # Start bot process with proper file handle management
            try:
                logger.debug(f"restart_bot({bot_key}) - opening log file {log_path}")
                log_handle = open(log_path, "a")
            except OSError as io_err:
                logger.error(f"❌ Cannot open log file {log_path}: {io_err}")
                return False

            try:
                logger.debug(f"restart_bot({bot_key}) - calling Popen with cmd: {' '.join(cmd)}")
                process = subprocess.Popen(
                    cmd,
                    cwd=self.cwd,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid,
                )
                log_handle.close()
                logger.debug(f"restart_bot({bot_key}) - Popen returned, PID: {process.pid}")
            except OSError as popen_err:
                log_handle.close()
                logger.error(f"❌ Cannot launch {' '.join(cmd)}: {popen_err}")
                return False
            
            self.bot_pids[bot_key] = process.pid
            logger.debug(f"restart_bot({bot_key}) - stored PID {process.pid}")
            logger.warning(f"🔄 Restarted {bot_key} bot (PID: {process.pid})")
            return True
        except OSError as os_err:
            logger.debug(f"restart_bot({bot_key}) - OSError: {type(os_err).__name__}: {os_err}")
            logger.error(f"❌ OS error restarting {bot_key}: {type(os_err).__name__}: {os_err}")
            return False
        except Exception as e:
            logger.debug(f"restart_bot({bot_key}) - Exception: {type(e).__name__}: {e}")
            logger.error(f"❌ Failed to restart {bot_key} bot: {type(e).__name__}: {e}")
            return False

    async def check_bots(self):
        """Check all bots and handle crashes."""
        logger.debug(f"check_bots() - starting check of {len(self.bots)} bots")
        for bot_key, bot_info in self.bots.items():
            bot_name = bot_info["name"]
            logger.debug(f"Checking {bot_key}...")
            
            # Get current PID if we don't have one
            if bot_key not in self.bot_pids or self.bot_pids[bot_key] is None:
                logger.debug(f"No PID for {bot_key}, searching...")
                pid = self.get_bot_pid(bot_key)
                self.bot_pids[bot_key] = pid
                logger.debug(f"Found PID for {bot_key}: {pid}")
                if pid:
                    logger.info(f"Found {bot_name} running with PID {pid}")
            
            # Check if bot is still running
            logger.debug(f"Checking if {bot_key} is still running...")
            if not self.is_bot_running(bot_key):
                self.crash_count[bot_key] += 1
                current_count = self.crash_count[bot_key]
                backoff = self._restart_backoff[bot_key]
                logger.debug(f"{bot_key} NOT running! Crash count: {current_count}")

                logger.error(f"❌ {bot_name} crashed! (Crash #{current_count}) — waiting {backoff}s before restart")

                # Send alert
                logger.debug(f"Sending crash alert for {bot_key}...")
                await self.send_crash_alert(bot_key, restart_attempt=True)

                # Backoff before restarting to avoid hammering resources on a boot-loop
                await asyncio.sleep(backoff)
                self._restart_backoff[bot_key] = min(backoff * 2, 300)  # cap at 5 min

                # Try to restart
                logger.debug(f"Attempting restart of {bot_key}...")
                restarted = self.restart_bot(bot_key)
                logger.debug(f"Restart result for {bot_key}: {restarted}")

                if restarted:
                    logger.warning(f"✅ Restart attempt #{current_count} successful")
                else:
                    logger.error(f"❌ Failed to restart {bot_name}")
            else:
                logger.debug(f"{bot_key} is running OK")
                # Bot is running — reset crash count and backoff
                if self.crash_count[bot_key] > 0:
                    logger.info(f"✅ {bot_name} recovered after {self.crash_count[bot_key]} crash(es)")
                    self.crash_count[bot_key] = 0
                self._restart_backoff[bot_key] = 5  # reset backoff on healthy check
        logger.debug(f"check_bots() - complete")

    async def run_forever(self):
        """Run bot monitor continuously with resilience."""
        logger.info("=" * 60)
        logger.debug("run_forever() - STARTING BOT MONITOR")
        logger.info("=" * 60)
        logger.info("🚀 Bot Monitor Started")
        logger.info("Checking bots every 10 seconds for crashes...")
        logger.debug("Starting initial bot detection...")
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        try:
            while True:
                try:
                    await self.check_bots()
                    await asyncio.sleep(10)  # Check every 10 seconds
                    consecutive_errors = 0  # Reset on successful check
                except Exception as check_err:
                    consecutive_errors += 1
                    logger.error(
                        f"❌ Error checking bots ({consecutive_errors}/{max_consecutive_errors}): "
                        f"{type(check_err).__name__}: {str(check_err)[:100]}",
                        exc_info=False
                    )
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"💥 Too many consecutive errors. Monitor shutting down.")
                        break
                    
                    # Backoff before retry
                    await asyncio.sleep(min(2 ** (consecutive_errors - 1), 60))
        
        except KeyboardInterrupt:
            logger.info("⏹️  Bot Monitor stopped by user")
        except asyncio.CancelledError:
            logger.info("⏹️  Bot Monitor cancelled")
        except Exception as e:
            logger.error(f"💥 Unexpected error in monitor: {type(e).__name__}: {e}", exc_info=True)
        finally:
            logger.info("Bot Monitor clean shutdown")


PIDFILE = "/tmp/bot_monitor.pid"


def _acquire_lock() -> bool:
    """Return True if we are the only running bot_monitor, False otherwise."""
    # Check if an existing PID file points to a live process
    if os.path.exists(PIDFILE):
        try:
            with open(PIDFILE) as f:
                old_pid = int(f.read().strip())
            result = subprocess.run(
                ["ps", "-p", str(old_pid), "-o", "pid="],
                capture_output=True, text=True, timeout=2
            )
            if result.stdout.strip():
                logger.error(
                    f"💥 Another bot_monitor is already running (PID {old_pid}). "
                    f"Kill it first: kill {old_pid}"
                )
                return False
        except Exception as exc:
            logger.debug(f"Stale PID file check skipped ({exc}) — safe to overwrite")

    # Write our own PID
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))
    return True


def _release_lock():
    try:
        os.remove(PIDFILE)
    except Exception as exc:
        logger.debug(f"PID file cleanup skipped: {exc}")


async def main():
    """Main entry point."""
    if not _acquire_lock():
        logger.error("Exiting — another bot_monitor instance is already running.")
        return

    try:
        monitor = BotMonitor()
        await monitor.run_forever()
    finally:
        _release_lock()


if __name__ == "__main__":
    asyncio.run(main())
