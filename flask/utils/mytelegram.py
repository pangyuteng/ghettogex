import datetime
import traceback
import logging
logger = logging.getLogger(__file__)
import os
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

import pandas as pd
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, JobQueue

from .misc import get_market_open_close, is_market_open
from .postgres_utils import apostgres_execute



last_notified_tstamp = None
VOLUME_THRESHOLD = 25000
CHECK_INTERVAL_SECONDS = 1

async def get_last_1min_spx_volume():
    try:
        ticker_alt = "SPXW"
        day_stamp = datetime.datetime.now().strftime("%Y-%m-%d")
        market_open,market_close = get_market_open_close(day_stamp,no_tzinfo=True)
        expiration = market_open.strftime("%Y-%m-%d")

        fetched = await apostgres_execute(
            None,
            "select * from candle_1min where ticker = %s and expiration = %s and tstamp <= now() and tstamp >= now() - interval '2 minute'",
            (ticker_alt,expiration)
        )
        cdf = pd.DataFrame([dict(x) for x in fetched])
        if len(cdf) == 0:
            return 600000

        cdf['tstamp_1min'] = cdf.tstamp.apply(lambda x: x.replace(second=0,microsecond=0))
        cdf.ask_volume = cdf.ask_volume.fillna(0)
        cdf.bid_volume = cdf.bid_volume.fillna(0)

        ndf = cdf.groupby(['tstamp_1min','strike']).agg(
            ask_volume=pd.NamedAgg(column="ask_volume", aggfunc="sum"),
            bid_volume=pd.NamedAgg(column="bid_volume", aggfunc="sum"),
        ).reset_index()
        ndf['volume'] = ndf.ask_volume+ndf.bid_volume
        last_volume = ndf.volume[ndf.volume.last_valid_index()]
    except:
        logger.error(traceback.format_exc())
        return None

    return last_volume

async def volume_alert(context):
    global last_notified_tstamp

    if not is_market_open():
        #logger.warning("market closed...")
        return
    
    tstamp = datetime.datetime.now()
    
    if last_notified_tstamp is not None:
        if tstamp - last_notified_tstamp < datetime.timedelta(seconds=5):
            logger.warning("already notified 5 seconds ago")
            return

    volume = await get_last_1min_spx_volume()
    
    if volume is None:
        logger.warning("volume is None???")
        return
    logger.warning(f"volume is {volume}")
    
    now_str = tstamp.strftime("%Y-%m-%d %H:%M:%S")
    triggered = False
    msg = ""
    if volume >= VOLUME_THRESHOLD:
        msg += f"\n\n🚨 **SPXW 1-min volume exceeded {VOLUME_THRESHOLD}**!\nvolume is {volume} \n{now_str} "
        last_notified_tstamp = tstamp
        triggered = True

    if triggered:
        try:
            query_str = "select * from external_apps where app_name = 'telegram' and alert_type = 'volume'"
            fetched = await apostgres_execute(None,query_str,(),is_commit=False)
            fetched = [dict(x) for x in fetched]
            for row in fetched:
                print(row['chat_id'])
                await context.bot.send_message(
                    chat_id=row['chat_id'],
                    text=msg,
                    parse_mode="Markdown"
                )
                logger.info(f"Alert sent: {msg}")
        except:
            logger.error(traceback.format_exc())

async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(f"effective_chat.id {update.effective_chat.id}")
    await update.message.reply_text(f'Hello {update.effective_user.first_name} from ghettogex_bot')

async def telegram_bot():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("hello", hello))

    job_queue: JobQueue = app.job_queue
    job_queue.run_repeating(
        callback=volume_alert,
        interval=CHECK_INTERVAL_SECONDS,
        first=3
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message"]
    )

    # Keep running until Ctrl+C
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(telegram_bot())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")