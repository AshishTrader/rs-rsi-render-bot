# RS+RSI Telegram Bot Deployment Guide

## 1. Telegram Bot Setup
1. Message [@BotFather](https://t.me/botfather) to create a new bot.
2. Save the **Bot Token**.
3. Create a Telegram Group or Channel and add your bot.
4. Get your **Chat ID** (use @userinfobot or similar).

## 2. Environment Variables
Set these in your Render Dashboard:
- `TELEGRAM_BOT_TOKEN`: Your bot token.
- `TELEGRAM_CHAT_ID`: Your Telegram Chat ID.
- `RENDER_URL`: Your app's URL (e.g., `https://your-bot.onrender.com`).
- `UNIVERSE`: `BOTH`, `N200`, or `N500` (default: `BOTH`).

## 3. Render Deployment
1. Create a new **Web Service** on [Render](https://render.com).
2. Connect this GitHub repository.
3. Render will automatically detect the `render.yaml` and configure the service.
4. Once deployed, the bot will auto-run at 3:15 PM IST every weekday.

## 4. Bot Commands
- `/run`: Start a full scan immediately.
- `/status`: View results of the last scan.
- `/help`: Show command list.
