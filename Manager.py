import json
import logging
import asyncio
from typing import Dict, List
from datetime import datetime

import gspread
import aiohttp
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

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
        """Convert column number to letter"""
        return chr(64 + col_num)
    
    def get_pending_records(self) -> List[Dict]:
        """Scan the sheet and return all records with empty Status"""
        try:
            all_values = self.worksheet.get_all_values()
            if not all_values or len(all_values) < 2:
                return []
            
            pending_records = []
            for i, row in enumerate(all_values[1:], start=2):
                record = {}
                for header, col_index in self.HEADERS.items():
                    record[header] = row[col_index - 1] if col_index - 1 < len(row) else ""
                
                if record.get('Status', '').strip() == "":
                    record['row_number'] = i
                    pending_records.append(record)
            
            return pending_records
        except Exception as e:
            logger.error(f"Error fetching pending records: {e}")
            return []

    def get_record_by_row(self, row_number: int) -> Dict:
        """Retrieve a single record by its row number"""
        try:
            values = self.worksheet.row_values(row_number)
            if not values:
                return {}
            
            record = {}
            for header, col_index in self.HEADERS.items():
                record[header] = values[col_index - 1] if col_index - 1 < len(values) else ""
            
            record['row_number'] = row_number
            return record
        except Exception as e:
            logger.error(f"Error fetching record for row {row_number}: {e}")
            return {}
    
    def update_status_and_group_link(self, row_number: int, status: str, group_link: str = None):
        """Batch update Status and Group_Link"""
        try:
            updates = []
            # Status
            status_col = self._get_column_letter(self.HEADERS['Status'])
            updates.append({
                'range': f'{status_col}{row_number}',
                'values': [[status]]
            })
            # Group_Link
            if group_link:
                group_link_col = self._get_column_letter(self.HEADERS['Group_Link'])
                updates.append({
                    'range': f'{group_link_col}{row_number}',
                    'values': [[group_link]]
                })
            # Batch update
            self.worksheet.batch_update(updates, value_input_option='RAW')
            logger.info(f"Updated row {row_number} with Status: {status}, Group_Link: {group_link}")
        except Exception as e:
            logger.error(f"Error updating row {row_number}: {e}")
            raise

class ManagerBot:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.token = config.get('manager_bot_token')
        self.client_token = config.get('client_bot_token')
        self.admin_ids = config.get('admin_ids', [])
        
        # ذخیره وضعیت رکوردهای پردازش شده (بجای callback tracking)
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

        await update.message.reply_text("🔄 **در حال دریافت درخواست های بررسی نشده...**", parse_mode='Markdown')
        pending_records = self.sheets_manager.get_pending_records()
        if not pending_records:
            await update.message.reply_text("✅ هیچ درخواستی یافت نشد.", parse_mode='Markdown')
            return

        sent_count = 0
        for record in pending_records:
            sent = await self.send_request_for_review(context, record)
            if sent:
                # Update status to Pending
                self.sheets_manager.update_status_and_group_link(record['row_number'], "Pending")
                sent_count += 1
                await asyncio.sleep(1)

        await update.message.reply_text(f"✅ **بررسی کامل شد!** {sent_count} درخواست برای بررسی ارسال شد.", parse_mode='Markdown')

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
                status_counts[status] = status_counts.get(status, 0) + 1

            accepted = status_counts['Accepted']
            rejected = status_counts['Rejected']
            pending = status_counts['Pending'] + status_counts[''] 
            
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

    async def send_request_for_review(self, context: ContextTypes.DEFAULT_TYPE, record: Dict) -> bool:
        try:
            row_number = record['row_number']
            name = record.get('Name', 'نامشخص')
            student_number = record.get('Student_Number', 'نامشخص')
            major = record.get('Major', 'نامشخص')
            email = record.get('Email', 'نامشخص')
            info = record.get('Info', 'اطلاعات اضافی ندارد')
            committee = record.get('Committee', 'نامشخص')
            username = record.get('Username', 'نامشخص')
            telegram_id = record.get('Telegram_ID', 'نامشخص')
            signature_url = record.get('Signature', '')

            message_text = (
                f"🆔 **درخواست جدید #{row_number - 1}**\n\n"
                f"📝 **اطلاعات تکمیلی:**\n{info}\n\n"
                f"👤 **نام:** {name}\n"
                f"🎓 **شماره دانشجویی:** {student_number}\n"
                f"📚 **رشته:** {major}\n"
                f"📧 **ایمیل:** {email}\n"
                f"🏢 **کمیته:** {committee}\n"
                f"📱 **نام کاربری:** @{username}\n"
                f"📱 **آیدی تلگرام:** {telegram_id}\n"
                f"⏰ **زمان:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
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

            for admin_id in self.admin_ids:
                try:
                    await context.bot.send_photo(
                        chat_id=admin_id,
                        photo=signature_url if signature_url else "https://via.placeholder.com/300x200.png?text=No+Image",
                        caption=message_text,
                        reply_markup=reply_markup,
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.error(f"Error sending to admin {admin_id}: {e}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error in send_request_for_review: {e}")
            return False

    async def send_notification_to_user(self, telegram_id: str, username: str, message: str, parse_mode: str = 'HTML') -> bool:
        if not telegram_id or telegram_id.strip() == "":
            logger.warning(f"Telegram ID is empty for user @{username}.")
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
                    if resp.status == 200:
                        logger.info(f"Notification sent to {telegram_id}")
                        return True
                    else:
                        text = await resp.text()
                        logger.error(f"Error sending notification to {telegram_id}: {text}")
                        return False
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
            # Parsing callback data
            parts = callback_data.split('_')
            if len(parts) < 2:
                await query.answer("❌ داده callback نامعتبر است.", show_alert=True)
                return

            action = parts[0]  # Accept or Reject
            row_number = int(parts[1])

            # Check if already processed
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

            # Adding to processed set
            self.processed_records.add(row_number)

            user_logged_in = self._is_user_logged_in(record)

            if action == "accept":
                group_link = self.committee_links.get(committee, "")
                self.sheets_manager.update_status_and_group_link(row_number, "Accepted", group_link)
                status_text = "✅ پذیرفته شد"
                
                if user_logged_in:
                    # User logged in - send notification
                    user_message = f"🎉 {name} عزیز، درخواست شما برای مشارکت در کمیته «{committee}» پذیرفته شد."
                    if group_link:
                        user_message += f"\n🔗 لینک گروه: <a href='{group_link}'>اینجا کلیک کنید</a>"
                    notification_sent = await self.send_notification_to_user(telegram_id, username, user_message, parse_mode='HTML')
                    notification_status = '✅ اعلان ارسال شد' if notification_sent else '❌ خطا در ارسال اعلان'

                else:
                    # User not logged in - do not send notification
                    notification_status = "⏳ کاربر لاگین نکرده - نتیجه را هنگام لاگین خواهد دید"
                    notification_sent = None

            else:  # reject
                self.sheets_manager.update_status_and_group_link(row_number, "Rejected")
                status_text = "❌ رد شد"
                
                if user_logged_in:
                    # User logged in - send notification
                    user_message = f"😔 {name} عزیز، درخواست شما برای کمیته «{committee}» رد شد."
                    notification_sent = await self.send_notification_to_user(telegram_id, username, user_message)
                    notification_status = '✅ اعلان ارسال شد' if notification_sent else '❌ خطا در ارسال اعلان'
                else:
                    # User not logged in - do not send notification
                    notification_status = "⏳ کاربر لاگین نکرده - نتیجه را هنگام لاگین خواهد دید"
                    notification_sent = None

            updated_caption = (
                f"🔍 **درخواست #{row_number - 1} - {status_text}**\n\n"
                f"📝 **اطلاعات تکمیلی:**\n{record.get('Info', 'اطلاعات اضافی ندارد')}\n\n"
                f"👤 **نام:** {name}\n"
                f"🎓 **شماره دانشجویی:** {student_number}\n"
                f"📚 **رشته:** {record.get('Major', 'نامشخص')}\n"
                f"📧 **ایمیل:** {record.get('Email', 'نامشخص')}\n"
                f"🏢 **کمیته:** {committee}\n"
                f"📱 **نام کاربری:** @{username}\n"
                f"📊 **وضعیت:** {status_text}\n"
                f"🔔 **اعلان:** {notification_status}\n"
                f"👨‍💼 **پردازش شده توسط:** {update.effective_user.first_name}\n"
                f"⏰ **زمان:** {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )

            await query.edit_message_caption(caption=updated_caption, parse_mode="Markdown", reply_markup=None)
            
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