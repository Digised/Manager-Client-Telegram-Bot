import json
import logging
import hashlib
import asyncio
import random
from typing import Dict, Optional
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler
)

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Conversation state
WAITING_PASSWORD = 1

class ConfigManager:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.exception(f"Failed to load config: {e}")
            raise

    def get(self, key: str, default=None):
        return self.config.get(key, default)

class GoogleSheetsClient:

    HEADERS = {
        'Submission_ID': 1,
        'Respondent_ID': 2,
        'Submitted_at': 3,
        'Name': 4,
        'Student_Number': 5,
        'Major': 6,
        'Email': 7,
        'Info': 8,
        'Committee': 9,
        'Group_Link': 10,
        'Username': 11,
        'Telegram_ID': 12,
        'Password': 13,
        'Signature': 14,
        'Status': 15,
        'Logged_In': 16
    }

    def __init__(self, credentials_file: str, sheet_url: str, worksheet_name: str = "Sheet1", config: ConfigManager = None):
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_url(sheet_url)
            self.worksheet = spreadsheet.worksheet(worksheet_name)
            logger.info("Connected to Google Sheets successfully")
        except Exception as e:
            logger.exception(f"Failed to initialize Google Sheets: {e}")
            raise

        if config:
            cache_settings = config.get('cache_settings', {})
            self._max_cache_size = cache_settings.get('max_cache_size', 1000)
            self._cache_enabled = cache_settings.get('enable_cache', True)
            sheets_settings = config.get('sheets_settings', {})
            self._request_delay = sheets_settings.get('request_delay_seconds', 0.1)
            self._max_retries = sheets_settings.get('max_retries', 3)
        else:
            self._max_cache_size = 1000
            self._cache_enabled = True
            self._request_delay = 0.1
            self._max_retries = 3

        self._user_cache = {} if self._cache_enabled else None

    # -------------------- Utility -------------------- #
    def _get_column_letter(self, col_num: int) -> str:
        return chr(64 + col_num)

    def _get_from_cache(self, telegram_id: str) -> Optional[Dict]:
        """Get from cache using telegram_id"""
        if not self._cache_enabled or not telegram_id:
            return None
        return self._user_cache.get(f"tg_{telegram_id}")

    def _set_cache(self, telegram_id: str, record: Dict):
        if self._user_cache is None or not telegram_id:
            return None
        key = f"tg_{telegram_id}"
        if key not in self._user_cache and len(self._user_cache) >= self._max_cache_size:
            oldest_key = next(iter(self._user_cache))
            self._user_cache.pop(oldest_key, None)
        self._user_cache[key] = record

    """ 
    async def _api_call_with_delay(self, func, *args, **kwargs):
        for attempt in range(self._max_retries):
            try:
                await asyncio.sleep(self._request_delay + random.uniform(0, 0.05))
                return func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) and attempt < self._max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                elif attempt < self._max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                else:
                    raise
    """

    def find_user_by_telegram_id(self, telegram_id: str) -> Optional[Dict]:
        telegram_id = str(telegram_id).strip()
        
        cached = self._get_from_cache(telegram_id)
        if cached:
            return cached
        
        try:
            col_index = self.HEADERS['Telegram_ID']   
            col_values = self.worksheet.col_values(col_index)  
            if telegram_id in col_values:
                row_number = col_values.index(telegram_id) + 1 
                record = self.get_record_by_row(row_number)
                self._set_cache(telegram_id, record)
                return record
        except gspread.exceptions.CellNotFound:
            return None
        except Exception as e:
            logger.exception(f"Error finding Telegram_ID {telegram_id}: {e}")
            return None

    def find_user_by_username(self, username: str, telegram_id: str) -> Optional[Dict]:
        if not username:
            return None
        search = username.strip().lstrip('@').lower()
        
        cached = self._get_from_cache(telegram_id)
        if cached:
            return cached

        try:
            usernames = self.worksheet.col_values(self.HEADERS['Username'])
            for i, stored_username in enumerate(usernames[1:], start=2):
                clean_stored = stored_username.strip().lstrip('@').lower()
                if clean_stored == search:
                    record = self.get_record_by_row(i)
                    self._set_cache(telegram_id, record)
                    return record
            return None
        except Exception as e:
            logger.exception(f"Error finding Username {username}: {e}")
            return None
        
    def get_record_by_row(self, row: int) -> Dict:
        try:
            values = self.worksheet.row_values(row)
            record = {h: values[c-1] if c-1 < len(values) else "" for h, c in self.HEADERS.items()}
            record['row_number'] = row
            return record
        except Exception as e:
            logger.exception(f"Error fetching row {row}: {e}")
            return {}

    def update_user_fields(self, record: Dict, fields: Dict, telegram_id: str) -> Dict:
        """Update fields in sheet and return locally updated record"""
        row = record['row_number']
        updates = []
        
        for field, value in fields.items():
            col_letter = self._get_column_letter(self.HEADERS[field])
            updates.append({
                'range': f'{col_letter}{row}',
                'values': [[str(value)]]
            })
        
        try:
            # Update sheet
            self.worksheet.batch_update(updates, value_input_option='RAW')
            
            # Update record locally
            updated_record = record.copy()
            for field, value in fields.items():
                updated_record[field] = str(value)

            # Update cache
            self._set_cache(telegram_id, updated_record)
            
            return updated_record
            
        except Exception as e:
            logger.exception(f"Error updating fields for row {row}: {e}")
            return record  # Return original record if update failed

    def hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

class ClientBot:
    def __init__(self, config: ConfigManager):
        self.token = config.get('client_bot_token')
        self.config = config
        self.sheets_client = GoogleSheetsClient(
            config.get('google_credentials_file'),
            config.get('google_sheet_url'),
            config.get('worksheet_name', 'Sheet1'),
            config
        )
        
        self.application = None
        self.user_attempts = {}
        self.logged_user_requests = {} 
        
        rate_limits = config.get('rate_limits', {})
        self.max_unknown_attempts = rate_limits.get('max_unknown_attempts', 3)
        self.unknown_ban_minutes = rate_limits.get('unknown_ban_minutes', 5)
        self.max_logged_requests = rate_limits.get('max_logged_requests', 10)
        self.logged_limit_minutes = rate_limits.get('logged_limit_minutes', 5)
        
        sheets_settings = config.get('sheets_settings', {})
        self.request_delay = sheets_settings.get('request_delay_seconds', 0.1)

    def _is_user_banned(self, telegram_id: str) -> tuple[bool, Optional[datetime]]:
        if telegram_id not in self.user_attempts:
            return False, None
        attempt_info = self.user_attempts[telegram_id]
        banned_until = attempt_info.get('banned_until')
        if banned_until and datetime.now() < banned_until:
            return True, banned_until
        elif banned_until and datetime.now() >= banned_until:
            self.user_attempts.pop(telegram_id, None)
            return False, None
        return False, None

    def _record_failed_attempt(self, telegram_id: str):
        now = datetime.now()
        if telegram_id not in self.user_attempts:
            self.user_attempts[telegram_id] = {
                'count': 1,
                'first_attempt': now,
                'banned_until': None
            }
        else:
            self.user_attempts[telegram_id]['count'] += 1
        attempt_info = self.user_attempts[telegram_id]
        if attempt_info['count'] >= self.max_unknown_attempts:
            self.user_attempts[telegram_id]['banned_until'] = now + timedelta(minutes=self.unknown_ban_minutes)

    def _check_logged_user_rate_limit(self, telegram_id: str) -> bool:
        """Rate limiting for logged-in users"""
        now = datetime.now()
        
        if telegram_id not in self.logged_user_requests:
            self.logged_user_requests[telegram_id] = []
        
        requests = self.logged_user_requests[telegram_id]
        recent_requests = [req for req in requests if now - req < timedelta(minutes=self.logged_limit_minutes)]
        self.logged_user_requests[telegram_id] = recent_requests
        
        if len(recent_requests) >= self.max_logged_requests:
            return True
        
        self.logged_user_requests[telegram_id].append(now)
        return False

    def _is_user_logged_in(self, record: Dict) -> bool:
        """Check if the user is logged in"""
        logged_in = (record.get('Logged_In') or "").strip().lower()
        return logged_in in ("yes", "y", "true", "1")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        await asyncio.sleep(random.uniform(self.request_delay, self.request_delay + 0.2))
        
        user = update.effective_user
        telegram_id = str(user.id)
        username = user.username or ""

        is_banned, banned_until = self._is_user_banned(telegram_id)
        if is_banned:
            remaining = banned_until - datetime.now()
            minutes = int(remaining.total_seconds() / 60) + 1
            await update.message.reply_text(
                f"⌚ محدود شده‌اید. {minutes} دقیقه دیگر دوباره تلاش کنید.",
                parse_mode='Markdown'
            )
            return ConversationHandler.END

        record = self.sheets_client.find_user_by_telegram_id(telegram_id)
        if not record and username:
            record = self.sheets_client.find_user_by_username(username, telegram_id)
            if record:
                # Update telegram_id in sheet and record
                record = self.sheets_client.update_user_fields(
                    record, 
                    {'Telegram_ID': telegram_id},
                    telegram_id
                )

        if not record:
            self._record_failed_attempt(telegram_id)
            user_not_found_msg = self.config.get('messages', {}).get('user_not_found', 
                "⌚ کاربر یافت نشد. لطفاً ابتدا فرم ثبت‌نام را تکمیل کنید.")
            await update.message.reply_text(user_not_found_msg, parse_mode='Markdown')
            return ConversationHandler.END

        self.user_attempts.pop(telegram_id, None)
        context.user_data['user_record'] = record
        context.user_data['telegram_id'] = telegram_id

        if self._is_user_logged_in(record):
            if self._check_logged_user_rate_limit(telegram_id):
                await update.message.reply_text(
                    f"⏳ درخواست‌های شما زیاد است. لطفاً {self.logged_limit_minutes} دقیقه صبر کنید.",
                    parse_mode='Markdown'
                )
                return ConversationHandler.END
            
            return await self.show_status(update, context, record)
        else:
            welcome_msg = self.config.get('messages', {}).get('welcome_user', 
                "👋 سلام {name}!\nلطفاً رمز عبور خود را وارد کنید:")
            formatted_msg = welcome_msg.format(name=record.get('Name', 'کاربر گرامی'))
            await update.message.reply_text(formatted_msg, parse_mode='Markdown')
            return WAITING_PASSWORD

    async def handle_password(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        password = update.message.text.strip()
        record = context.user_data.get('user_record')
        telegram_id = context.user_data.get('telegram_id')

        if not record:
            await update.message.reply_text("⌚ جلسه منقضی شده است. دوباره /start را بزنید.", parse_mode='Markdown')
            return ConversationHandler.END

        hashed_input = self.sheets_client.hash_password(password)
        stored_password_plain = record.get('Password', '').strip()
        stored_password_hashed = self.sheets_client.hash_password(stored_password_plain)

        if stored_password_hashed != hashed_input:
            wrong_password_msg = self.config.get('messages', {}).get('wrong_password', 
                "⌚ رمز عبور اشتباه است. لطفاً دوباره تلاش کنید:")
            await update.message.reply_text(wrong_password_msg, parse_mode='Markdown')
            return WAITING_PASSWORD

        record = self.sheets_client.update_user_fields(
            record, 
            {'Logged_In': 'Yes'},
            telegram_id
        )
        context.user_data['user_record'] = record

        return await self.show_status(update, context, record)

    async def show_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE, record: Dict):
        telegram_id = context.user_data.get('telegram_id')
        current_status = (record.get('Status') or "").strip().lower()
        
        if current_status in ["accepted", "rejected"]:
            if current_status == "accepted":
                return await self.send_accepted(update, record)
            else:
                return await self.send_rejected(update, record)
        
        try:
            row = record['row_number']
            status_col = self.sheets_client._get_column_letter(self.sheets_client.HEADERS['Status'])
            group_link_col = self.sheets_client._get_column_letter(self.sheets_client.HEADERS['Group_Link'])
            
            current_status_value = self.sheets_client.worksheet.acell(f'{status_col}{row}').value or ""
            
            # Update record locally if status changed
            if current_status_value.strip().lower() != current_status:
                current_group_link = self.sheets_client.worksheet.acell(f'{group_link_col}{row}').value or ""
                record['Status'] = current_status_value
                record['Group_Link'] = current_group_link
                context.user_data['user_record'] = record
                self.sheets_client._set_cache(telegram_id, record)
        except Exception as e:
            logger.exception(f"Error checking fresh status: {e}")
        
        status = (record.get('Status') or "").strip().lower()
        
        if status == "accepted":
            return await self.send_accepted(update, record)
        elif status == "rejected":
            return await self.send_rejected(update, record)
        else:
            return await self.send_pending(update, record)

    async def send_accepted(self, update: Update, record: Dict):
        name = record.get('Name', 'کاربر گرامی')
        committee = record.get('Committee', 'نامشخص')
        group_link = (record.get("Group_Link") or "").strip()
        team_link = config.get("executive_team", {}).get("Link", "")

        accepted_msg = self.config.get('messages', {}).get('status_accepted', 
            "🎉 {name} عزیز، درخواست شما برای مشارکت در کمیته «{committee}» پذیرفته شد.")
        
        message = accepted_msg.format(name=name, committee=committee)
        
        if group_link:
            group_link_msg = self.config.get('messages', {}).get('group_link_text', 
                "\n🔗 لینک گروه: <a href='{group_link}'>اینجا کلیک کنید</a>")
            message += group_link_msg.format(group_link=group_link)
        
        if team_link:
            message += "\n<a href='{0}'>گروه کادر اجرایی</a>".format(team_link)
            
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        return ConversationHandler.END

    async def send_rejected(self, update: Update, record: Dict):
        name = record.get('Name', 'کاربر گرامی')
        committee = record.get('Committee', 'نامشخص')

        rejected_msg = self.config.get('messages', {}).get(
            'status_rejected',
            "😔 {name} عزیز، درخواست شما برای کمیته «{committee}» رد شد."
        )

        message = rejected_msg.format(name=name, committee=committee)

        await update.message.reply_text(message, parse_mode='Markdown')
        return ConversationHandler.END

    async def send_pending(self, update: Update, record: Dict):
        name = record.get('Name', 'کاربر گرامی')
        pending_msg = self.config.get('messages', {}).get('status_pending', 
            "⏳ {name} عزیز، درخواست شما هنوز در حال بررسی است.")
        message = pending_msg.format(name=name)
        await update.message.reply_text(message, parse_mode='Markdown')
        return ConversationHandler.END

    def run(self):
        app = Application.builder().token(self.token).build()
        conv = ConversationHandler(
            entry_points=[CommandHandler("start", self.start_command)],
            states={WAITING_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_password)]},
            fallbacks=[],
            allow_reentry=True
        )
        app.add_handler(conv)
        app.run_polling()

if __name__ == "__main__":
    config = ConfigManager("config.json")
    bot = ClientBot(config)
    bot.run()