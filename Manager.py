import json
import logging
import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import io

import gspread
import aiohttp
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import TelegramError

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Config file {self.config_file} not found.")
            raise
        except json.JSONDecodeError:
            logger.error(f"JSON decode error in config file {self.config_file}.")
            raise
    
    def get(self, key: str, default=None):
        return self.config.get(key, default)

class OptimizedGoogleSheetsManager:
    
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

    def __init__(self, credentials_file: str, sheet_url: str, worksheet_name: str = "Sheet1"):
        self.credentials_file = credentials_file
        self.sheet_url = sheet_url
        self.worksheet_name = worksheet_name
        self.client = None
        self.worksheet = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_file(self.credentials_file, scopes=scope)
            self.client = gspread.authorize(creds)
            spreadsheet = self.client.open_by_url(self.sheet_url)
            self.worksheet = spreadsheet.worksheet(self.worksheet_name)
            logger.info("Connected to Google Sheets successfully.")
        except Exception as e:
            logger.error(f"Error connecting to Google Sheets: {e}")
            raise

    def _get_column_letter(self, col_num: int) -> str:
        return chr(64 + col_num)
    
    def get_pending_records(self) -> List[Dict]:
        try:
            all_values = self.worksheet.get_all_values()
            if not all_values or len(all_values) < 2:
                return []
            
            max_cols = max(self.HEADERS.values())
            pending_records = []
            
            for i, row in enumerate(all_values[1:], start=2):
                while len(row) < max_cols:
                    row.append('')
                
                record = {}
                for header, col_index in self.HEADERS.items():
                    value = row[col_index - 1] if col_index <= len(row) else ""
                    record[header] = str(value).strip() if value else ""
                
                status = record.get('Status', '').strip()
                
                if status == "" or status.lower() == "pending":
                    record['row_number'] = i
                    pending_records.append(record)
            
            logger.info(f"Found {len(pending_records)} pending records")
            return pending_records
        except Exception as e:
            logger.error(f"Error fetching pending records: {e}")
            return []

    def get_record_by_row(self, row_number: int) -> Dict:
        try:
            values = self.worksheet.row_values(row_number)
            if not values:
                return {}
            
            max_cols = max(self.HEADERS.values())
            while len(values) < max_cols:
                values.append('')
            
            record = {}
            for header, col_index in self.HEADERS.items():
                value = values[col_index - 1] if col_index <= len(values) else ""
                record[header] = str(value).strip() if value else ""
            
            record['row_number'] = row_number
            return record
        except Exception as e:
            logger.error(f"Error fetching record for row {row_number}: {e}")
            return {}
    
    def update_status_and_group_link(self, row_number: int, status: str, group_link: str = None):
        try:
            updates = []
            status_col = self._get_column_letter(self.HEADERS['Status'])
            updates.append({
                'range': f'{status_col}{row_number}',
                'values': [[status]]
            })
            if group_link:
                group_link_col = self._get_column_letter(self.HEADERS['Group_Link'])
                updates.append({
                    'range': f'{group_link_col}{row_number}',
                    'values': [[group_link]]
                })
            self.worksheet.batch_update(updates, value_input_option='RAW')
        except Exception as e:
            logger.error(f"Error updating row {row_number}: {e}")
            raise

class ManagerBot:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.token = config.get('manager_bot_token')
        self.client_token = config.get('client_bot_token')
        self.admin_ids = config.get('admin_ids', [])
        
        self.processed_records = set()
        self.committee_links = config.get('committee_links', {})
        
        self.sheets_manager = OptimizedGoogleSheetsManager(
            credentials_file=config.get('google_credentials_file'),
            sheet_url=config.get('google_sheet_url'),
            worksheet_name=config.get('worksheet_name', 'Sheet1')
        )
        
        self.application = None

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in self.admin_ids:
            await update.message.reply_text("⛔ دسترسی مجاز نیست.")
            return
        
        await update.message.reply_text(
            "👋 **خوش آمدید ادمین عزیز!**\n\n"
            "📋 از دستور /check برای بررسی درخواست‌های بررسی نشده استفاده کنید.\n"
            "📊 از دستور /stats برای مشاهده آمار استفاده کنید.",
            parse_mode='Markdown'
        )

    async def check_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in self.admin_ids:
            await update.message.reply_text("⛔ دسترسی مجاز نیست.")
            return

        status_msg = await update.message.reply_text(
            "🔄 **در حال دریافت درخواست های بررسی نشده...**", 
            parse_mode='Markdown'
        )
        
        pending_records = self.sheets_manager.get_pending_records()
        
        if not pending_records:
            await status_msg.edit_text("✅ هیچ درخواست بررسی نشده‌ای وجود ندارد.", parse_mode='Markdown')
            return

        await status_msg.edit_text(
            f"📦 **{len(pending_records)} درخواست یافت شد. در حال ارسال...**", 
            parse_mode='Markdown'
        )

        success_count = 0
        failed_count = 0
        
        for idx, record in enumerate(pending_records, 1):
            if idx % 5 == 0:
                await status_msg.edit_text(
                    f"🔄 **در حال پردازش... ({idx}/{len(pending_records)})**\n"
                    f"✅ موفق: {success_count} | ❌ ناموفق: {failed_count}",
                    parse_mode='Markdown'
                )
            
            sent = await self.send_request_for_review(context, record)
            
            if sent:
                self.sheets_manager.update_status_and_group_link(record['row_number'], "Pending")
                success_count += 1
            else:
                failed_count += 1
            
            await asyncio.sleep(1)

        final_message = (
            f"✅ **بررسی کامل شد!**\n\n"
            f"📊 نتایج:\n"
            f"✅ ارسال موفق: {success_count}\n"
            f"❌ ارسال ناموفق: {failed_count}\n"
        )
        
        await status_msg.edit_text(final_message, parse_mode='Markdown')

    async def download_image(self, url: str) -> Optional[io.BytesIO]:
        if not url or not isinstance(url, str) or url.strip() == "":
            return None

        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            return None
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            blocked_hosts = ['localhost', '127.0.0.1', '0.0.0.0', '::1']
            hostname_lower = parsed.netloc.lower()
            if any(blocked in hostname_lower for blocked in blocked_hosts):
                return None
            if hostname_lower.startswith(('10.', '192.168.', '172.16.', '172.17.', '172.18.', '172.19.', '172.20.', '172.21.', '172.22.', '172.23.', '172.24.', '172.25.', '172.26.', '172.27.', '172.28.', '172.29.', '172.30.', '172.31.')):
                return None
        except Exception:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, 
                    timeout=aiohttp.ClientTimeout(total=20, connect=10),
                    max_redirects=5,
                    allow_redirects=True
                ) as resp:
                    if resp.status != 200:
                        return None

                    content_type = resp.headers.get("Content-Type", "").lower()
                    if not any(img_type in content_type for img_type in ["image/", "application/octet-stream"]):
                        return None

                    content_length = resp.headers.get("Content-Length")
                    if content_length and int(content_length) > 20 * 1024 * 1024:
                        return None

                    image_data = await resp.read()
                    
                    if len(image_data) > 20 * 1024 * 1024 or len(image_data) < 100:
                        return None

                    return io.BytesIO(image_data)

        except (asyncio.TimeoutError, aiohttp.ClientError):
            return None
        except Exception:
            return None

    def _escape_text(self, text: str) -> str:
        if not text or not isinstance(text, str):
            return ""
        text = str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        return text

    async def send_request_for_review(self, context: ContextTypes.DEFAULT_TYPE, record: Dict) -> bool:
        image_bytes = None
        try:
            row_number = record.get('row_number')
            if not row_number:
                return False
                
            name = self._escape_text(record.get('Name', 'نامشخص'))
            student_number = self._escape_text(record.get('Student_Number', 'نامشخص'))
            major = self._escape_text(record.get('Major', 'نامشخص'))
            email = self._escape_text(record.get('Email', 'نامشخص'))
            info = self._escape_text(record.get('Info', 'اطلاعات اضافی ندارد'))
            committee = self._escape_text(record.get('Committee', 'نامشخص'))
            username = self._escape_text(record.get('Username', 'نامشخص'))
            telegram_id = self._escape_text(record.get('Telegram_ID', 'نامشخص'))
            signature_url = record.get('Signature', '').strip()
            time_str = self._escape_text(datetime.now().strftime('%Y-%m-%d %H:%M'))

            message_text = (
                f"🆔 <b>درخواست جدید #{row_number - 1}</b>\n\n"
                f"📝 <b>اطلاعات تکمیلی:</b>\n{info}\n\n"
                f"👤 <b>نام:</b> {name}\n"
                f"🎓 <b>شماره دانشجویی:</b> {student_number}\n"
                f"📚 <b>رشته:</b> {major}\n"
                f"📧 <b>ایمیل:</b> {email}\n"
                f"🏢 <b>کمیته:</b> {committee}\n"
                f"📱 <b>نام کاربری:</b> @{username}\n"
                f"📱 <b>آیدی تلگرام:</b> {telegram_id}\n"
                f"⏰ <b>زمان:</b> {time_str}\n\n"
                f"لطفاً تصمیم خود را اعلام کنید:"
            )

            accept_callback = f"accept_{row_number}"
            reject_callback = f"reject_{row_number}"

            keyboard = [
                [
                    InlineKeyboardButton("✅ پذیرش", callback_data=accept_callback),
                    InlineKeyboardButton("❌ رد", callback_data=reject_callback)
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            if signature_url and signature_url.startswith(('http://', 'https://')):
                image_bytes = await self.download_image(signature_url)
            
            sent_to_all = True
            for admin_id in self.admin_ids:
                try:
                    if image_bytes:
                        image_bytes.seek(0)
                        await context.bot.send_photo(
                            chat_id=admin_id,
                            photo=image_bytes,
                            caption=message_text,
                            reply_markup=reply_markup,
                            parse_mode="HTML",
                            read_timeout=30,
                            write_timeout=30,
                            connect_timeout=30
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=f"⚠️ <b>مشکل در بارگذاری تصویر</b>\n\n{message_text}",
                            reply_markup=reply_markup,
                            parse_mode="HTML"
                        )
                    
                except (TelegramError, Exception) as e:
                    logger.error(f"Error sending to admin {admin_id}, row {row_number}: {e}")
                    sent_to_all = False
            
            return sent_to_all
            
        except Exception as e:
            logger.error(f"Error in send_request_for_review for row {record.get('row_number')}: {e}")
            return False
        finally:
            if image_bytes is not None:
                try:
                    image_bytes.close()
                except Exception:
                    pass

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in self.admin_ids:
            await update.message.reply_text("⛔ دسترسی مجاز نیست.")
            return
        try:
            all_values = self.sheets_manager.worksheet.get_all_values()
            total = len(all_values) - 1 
            
            status_counts = {'Accepted': 0, 'Rejected': 0, 'Pending': 0, '': 0}
            for row in all_values[1:]:
                status = row[self.sheets_manager.HEADERS['Status'] - 1] if len(row) >= self.sheets_manager.HEADERS['Status'] else ""
                status = str(status).strip()
                status_counts[status] = status_counts.get(status, 0) + 1

            accepted = status_counts.get('Accepted', 0)
            rejected = status_counts.get('Rejected', 0)
            pending = status_counts.get('Pending', 0) + status_counts.get('', 0)
            
            stats_message = (
                f"📊 **گزارش آماری**\n\n"
                f"📝 **کل درخواست‌ها:** {total}\n"
                f"✅ **پذیرفته شده:** {accepted}\n"
                f"❌ **رد شده:** {rejected}\n"
                f"⏳ **در انتظار:** {pending}\n\n"
                f"📈 **نرخ پذیرش:** {(accepted/total*100):.1f}%" if total > 0 else "📈 **نرخ پذیرش:** 0%"
            )
            await update.message.reply_text(stats_message, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error generating stats: {e}")
            await update.message.reply_text("❌ خطا در تولید آمار.")

    async def send_notification_to_user(self, telegram_id: str, username: str, message: str, parse_mode: str = 'HTML') -> bool:
        if not telegram_id or telegram_id.strip() == "":
            return False
        url = f"https://api.telegram.org/bot{self.client_token}/sendMessage"
        payload = {
            "chat_id": telegram_id,
            "text": message,
            "parse_mode": parse_mode
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    return resp.status == 200
        except Exception as e:
            logger.error(f"Error sending notification to {telegram_id}: {e}")
            return False

    def _is_user_logged_in(self, record: Dict) -> bool:
        logged_in = str(record.get("Logged_In", "")).strip().lower()
        return logged_in in ["yes", "y", "true", "1"]

    async def handle_decision(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = update.effective_user.id
        if user_id not in self.admin_ids:
            await query.answer("⛔ دسترسی مجاز نیست.", show_alert=True)
            return

        callback_data = query.data

        try:
            parts = callback_data.split('_')
            if len(parts) < 2:
                await query.answer("❌ داده callback نامعتبر است.", show_alert=True)
                return

            action = parts[0]
            row_number = int(parts[1])

            if row_number in self.processed_records:
                await query.answer("⚠️ این درخواست قبلاً پردازش شده است.", show_alert=True)
                return

            record = self.sheets_manager.get_record_by_row(row_number)
            if not record:
                await query.answer("❌ رکورد یافت نشد.", show_alert=True)
                return

            current_status = record.get('Status', '').strip()
            if current_status not in ['', 'Pending']:
                await query.answer(f"⚠️ وضعیت این درخواست قبلاً {current_status} شده است.", show_alert=True)
                return

            telegram_id = str(record.get("Telegram_ID", "")).strip()
            username = str(record.get("Username", "")).strip()
            name = record.get("Name", "کاربر گرامی")
            student_number = record.get("Student_Number", "نامشخص")
            committee = record.get("Committee", "نامشخص").strip()
            team_link = self.config.get("executive_team", {}).get("Link", "")

            self.processed_records.add(row_number)

            user_logged_in = self._is_user_logged_in(record)

            if action == "accept":
                group_link = self.committee_links.get(committee, "")
                self.sheets_manager.update_status_and_group_link(row_number, "Accepted", group_link)
                status_text = "✅ پذیرفته شد"
                
                if user_logged_in:
                    user_message = f"🎉 {name} عزیز، درخواست شما برای مشارکت در کمیته «{committee}» پذیرفته شد."
                    if group_link:
                        user_message += f"\n🔗 لینک گروه: <a href='{group_link}'>اینجا کلیک کنید</a>"
                    if team_link:
                        user_message += "\n<a href='{0}'>گروه کادر اجرایی</a>".format(team_link)
                    notification_sent = await self.send_notification_to_user(telegram_id, username, user_message, parse_mode='HTML')
                    notification_status = '✅ اعلان ارسال شد' if notification_sent else '❌ خطا در ارسال اعلان'
                else:
                    notification_status = "⏳ کاربر لاگین نکرده - نتیجه را هنگام لاگین خواهد دید"
                    notification_sent = None

            else:
                self.sheets_manager.update_status_and_group_link(row_number, "Rejected")
                status_text = "❌ رد شد"
                
                if user_logged_in:
                    user_message = f"😔 {name} عزیز، درخواست شما برای کمیته «{committee}» رد شد."
                    notification_sent = await self.send_notification_to_user(telegram_id, username, user_message)
                    notification_status = '✅ اعلان ارسال شد' if notification_sent else '❌ خطا در ارسال اعلان'
                else:
                    notification_status = "⏳ کاربر لاگین نکرده - نتیجه را هنگام لاگین خواهد دید"
                    notification_sent = None

            info_text = record.get('Info', 'اطلاعات اضافی ندارد')
            email_text = record.get('Email', 'نامشخص')
            
            updated_caption = (
                f"📝 درخواست #{row_number - 1} - {status_text}\n\n"
                f"📝 اطلاعات تکمیلی:\n{info_text}\n\n"
                f"👤 نام: {name}\n"
                f"🎓 شماره دانشجویی: {student_number}\n"
                f"📚 رشته: {record.get('Major', 'نامشخص')}\n"
                f"📧 ایمیل: {email_text}\n"
                f"🏢 کمیته: {committee}\n"
                f"📱 نام کاربری: @{username}\n"
                f"📊 وضعیت: {status_text}\n"
                f"📢 اعلان: {notification_status}\n"
                f"👨‍💼 پردازش شده توسط: {update.effective_user.first_name}\n"
                f"⏰ زمان: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            await query.edit_message_caption(caption=updated_caption, reply_markup=None)
            
            if user_logged_in and notification_sent is not None:
                await query.answer(f"✅ درخواست {status_text}! اعلان به کاربر ارسال شد.", show_alert=False)
            else:
                await query.answer(f"✅ درخواست {status_text}! کاربر نتیجه را هنگام لاگین خواهد دید.", show_alert=False)

        except Exception as e:
            logger.error(f"Error processing decision: {e}")
            await query.answer("❌ خطا در پردازش تصمیم. لطفاً دوباره تلاش کنید.", show_alert=True)
            if 'row_number' in locals():
                self.processed_records.discard(row_number)

    def run(self):
        if not self.token:
            raise ValueError("Manager bot token is not set in the configuration.")

        self.application = Application.builder().token(self.token).build()
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("check", self.check_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CallbackQueryHandler(self.handle_decision))

        logger.info("Manager Bot is starting...")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        config = ConfigManager("config.json")
        bot = ManagerBot(config)
        bot.run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise