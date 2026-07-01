"""Notifications module for Discord and other alert systems."""

from .discord_webhook import DiscordWebhook, get_discord_client, send_discord_alert

__all__ = ["DiscordWebhook", "get_discord_client", "send_discord_alert"]
