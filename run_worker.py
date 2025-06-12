import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from services.worker import SignalWorker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("📊 Запуск пересчёта сигналов по текущим данным")
    loop = asyncio.get_running_loop()

    def run_signals():
        worker = SignalWorker()
        return worker.process_all_pairs()

    with ThreadPoolExecutor() as pool:
        results = await loop.run_in_executor(pool, run_signals)

    if results:
        logger.info(f"✅ Сигналы обновлены: {len(results)}")
        for signal in results:
            logger.info(f"🟢 {signal['symbol']} {signal['timeframe']} — {signal['signal_type']} @ {signal['current_price']:.4f}")
    else:
        logger.info("⚠️ Ни одного сигнала не прошло фильтрацию")

if __name__ == "__main__":
    asyncio.run(main())
