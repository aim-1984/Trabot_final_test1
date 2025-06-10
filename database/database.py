# database/database.py
import os
import logging
import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import json
import numpy as np
from datetime import datetime
import time
import re

from config.constants import DB_CONFIG  # теперь так

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        logger.debug("🔌 Инициализация DatabaseManager")
        self.db_config = {
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '123'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        # Создаем пул соединений ДО создания таблиц
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=20,
            **self.db_config
        )
        logger.debug("✅ Успешное подключение к БД")
        # Теперь можно безопасно создавать таблицы
        self._create_tables()
        self._create_indexes()
        logger.info("Таблицы созданы/проверены")

    def get_connection(self):
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        self.connection_pool.putconn(conn)

    def _create_tables(self):
        """Создание и изменение структуры таблиц, очистка содержимого в конце"""
        # --- Шаг 1: Создание и изменение структуры таблиц ---
        start_time = time.time()
        ddl_queries = [
            # Таблица collected_candles
            """
            CREATE TABLE IF NOT EXISTS collected_candles (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(5) NOT NULL,
                candles JSONB NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (symbol, timeframe)
            );
            """,

            # Таблица levels
            """
            CREATE TABLE IF NOT EXISTS levels (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(5) NOT NULL,
                price DECIMAL(20,8) NOT NULL,
                type VARCHAR(10) NOT NULL,
                strength INT NOT NULL,
                upper DECIMAL(20,8),
                lower DECIMAL(20,8),
                distance DECIMAL(10,4),
                touched INT DEFAULT 0,
                broken BOOLEAN DEFAULT FALSE,
                last_touched TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,

            # Таблица alerts
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20),
                level_price DECIMAL(20,8),
                current_price DECIMAL(20,8),
                type VARCHAR(20),
                distance DECIMAL(10,4),
                strength INT,
                timeframe VARCHAR(10),
                source VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            ALTER TABLE alerts
            ADD COLUMN IF NOT EXISTS source VARCHAR(20);
            """,

            # Таблица pairs_cache
            """
            CREATE TABLE IF NOT EXISTS pairs_cache (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                volume DECIMAL(20,8) NOT NULL,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                missing_periods INT DEFAULT 0
            );
            """,

            # Таблица trend_cache
            """
            CREATE TABLE IF NOT EXISTS trend_cache (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL UNIQUE,
                direction VARCHAR(10) NOT NULL,
                ema50 DECIMAL(20,8),
                ema200 DECIMAL(20,8),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,

            # Таблица indicators
            """
            CREATE TABLE IF NOT EXISTS indicators (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(5) NOT NULL,
                indicator_type VARCHAR(20) NOT NULL,
                value DECIMAL(20,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            ALTER TABLE IF EXISTS indicators
            ALTER COLUMN value TYPE VARCHAR(50)
            USING value::VARCHAR(50);
            """,
            # В метод _create_tables, в список ddl_queries добавьте:
            """
            CREATE TABLE IF NOT EXISTS market_cap (
                id SERIAL PRIMARY KEY,
                total_cap DECIMAL(20,2) NOT NULL,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,

            # Таблица signals
            """
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                timeframe VARCHAR(5) NOT NULL,
                signal_type VARCHAR(10) NOT NULL,
                price DECIMAL(20,8) NOT NULL,
                time BIGINT NOT NULL,
                indicator VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            ALTER TABLE signals
            ADD COLUMN IF NOT EXISTS score INT,          
            ADD COLUMN IF NOT EXISTS details TEXT,
            ADD COLUMN IF NOT EXISTS recommendation VARCHAR(50),
            ADD COLUMN IF NOT EXISTS current_price DECIMAL(20,8);
            """,
        ]

        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                for i, query in enumerate(ddl_queries):
                    table_name = self._extract_table_name(query)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logger.info(f"[{timestamp}] ⏳ Создание таблицы: {table_name}")
                    cur.execute(query)
            conn.commit()
            duration = round(time.time() - start_time, 2)
            logger.info(
                f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Всего создано {len(ddl_queries)} таблиц за {duration} сек.")
        except Exception as e:
            logger.error(f"❌ Ошибка при создании таблиц: {e}", exc_info=True)
            conn.rollback()
        finally:
            self.release_connection(conn)

    def _extract_table_name(self, query: str) -> str:
        import re
        match = re.search(r"(CREATE|ALTER)\s+TABLE(?: IF NOT EXISTS)?\s+(\w+)", query, re.IGNORECASE)
        return match.group(2) if match else "неизвестно"

    def _create_indexes(self):
        """Создание индексов для оптимизации запросов"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_candles_symbol_timeframe ON collected_candles (symbol, timeframe);",
            "CREATE INDEX IF NOT EXISTS idx_candles_last_updated ON collected_candles (last_updated);",
            "CREATE INDEX IF NOT EXISTS idx_levels_symbol_timeframe ON levels (symbol, timeframe);",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_unique ON signals (symbol, timeframe, signal_type, time);"
            "CREATE INDEX IF NOT EXISTS idx_levels_price ON levels (price);"
        ]
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                for idx in indexes:
                    cur.execute(idx)
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка создания индексов: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def upsert_candles(self, symbol, timeframe, new_candles):
        logger.debug(f"🔄 Обновление свечей {symbol} {timeframe}")
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                # Получаем текущие свечи
                cur.execute("""
                            SELECT candles
                            FROM collected_candles
                            WHERE symbol = %s
                              AND timeframe = %s
                            """, (symbol, timeframe))
                result = cur.fetchone()
                # Убираем json.loads, так как данные уже десериализованы
                current_candles = result[0] if result else []
                # Фильтрация новых свечей
                existing_times = {candle['time'] for candle in current_candles}
                filtered_new = [
                    candle for candle in new_candles
                    if candle['time'] not in existing_times
                ]
                if not filtered_new:
                    logger.debug("Нет новых свечей для добавления")
                    return len(current_candles)
                # Объединение и сортировка
                merged = current_candles + filtered_new
                merged.sort(key=lambda x: x['time'])
                # Ограничение количества
                max_candles = {
                    '1d': 900,
                    '4h': 800,
                    '1h': 700,
                    '15m': 500
                }.get(timeframe, 500)
                if len(merged) > max_candles:
                    merged = merged[-max_candles:]
                # Обновление или вставка
                if result:
                    cur.execute("""
                                UPDATE collected_candles
                                SET candles      = %s,
                                    last_updated = CURRENT_TIMESTAMP
                                WHERE symbol = %s
                                  AND timeframe = %s
                                """, (json.dumps(merged), symbol, timeframe))
                else:
                    cur.execute("""
                                INSERT INTO collected_candles (symbol, timeframe, candles)
                                VALUES (%s, %s, %s)
                                """, (symbol, timeframe, json.dumps(merged)))
                conn.commit()
                return len(merged)
        except Exception as e:
            logger.error(f"❌ Ошибка обновления свечей {symbol} {timeframe}: {e}", exc_info=True)
            conn.rollback()
            return 0
        finally:
            self.release_connection(conn)

    def get_candles(self, symbol, timeframe):
        """Получение свечей для конкретной пары и таймфрейма"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                            SELECT candles
                            FROM collected_candles
                            WHERE symbol = %s
                              AND timeframe = %s
                            """, (symbol, timeframe))
                result = cur.fetchone()
                # Возвращаем список напрямую
                return result[0] if result else []
        except Exception as e:
            logger.error(f"Ошибка получения свечей {symbol} {timeframe}: {e}")
            return []
        finally:
            self.release_connection(conn)

    def get_all_candles(self, timeframe=None):
        """Получение всех свечей (по всем парам и таймфреймам)"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                if timeframe:
                    cur.execute("""
                                SELECT symbol, timeframe, candles
                                FROM collected_candles
                                WHERE timeframe = %s
                                """, (timeframe,))
                else:
                    cur.execute("SELECT symbol, timeframe, candles FROM collected_candles")
                result = cur.fetchall()
                # Убираем json.loads, так как данные уже десериализованы
                return {(row[0], row[1]): row[2] for row in result}
        except Exception as e:
            logger.error(f"Ошибка получения всех свечей: {e}")
            return {}
        finally:
            self.release_connection(conn)

    def clear_old_candles(self):
        """Очистка устаревших свечей с защитой от NULL"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Очищаем 1d старше 3 месяцев
                cur.execute(""" 
                    UPDATE collected_candles
                    SET candles = COALESCE((
                        SELECT jsonb_agg(c)
                        FROM jsonb_array_elements(candles) AS c
                        WHERE (c ->> 'time')::BIGINT > EXTRACT(EPOCH FROM NOW() - INTERVAL '3 months') * 1000
                    ), '[]'::jsonb)
                    WHERE timeframe = '1d'
                """)

                # Очищаем 4h и 1h старше 1 месяца
                cur.execute(""" 
                    UPDATE collected_candles
                    SET candles = COALESCE((
                        SELECT jsonb_agg(c)
                        FROM jsonb_array_elements(candles) AS c
                        WHERE (c ->> 'time')::BIGINT > EXTRACT(EPOCH FROM NOW() - INTERVAL '1 month') * 1000
                    ), '[]'::jsonb)
                    WHERE timeframe IN ('4h', '1h')
                """)

                # Очищаем 15m старше 2 недель
                cur.execute(""" 
                    UPDATE collected_candles
                    SET candles = COALESCE((
                        SELECT jsonb_agg(c)
                        FROM jsonb_array_elements(candles) AS c
                        WHERE (c ->> 'time')::BIGINT > EXTRACT(EPOCH FROM NOW() - INTERVAL '2 weeks') * 1000
                    ), '[]'::jsonb)
                    WHERE timeframe = '15m'
                """)

            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка очистки старых свечей: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def save_levels(self, levels):
        """Сохранение уровней в БД"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Очищаем старые уровни
                cur.execute("DELETE FROM levels")
                # Вставляем новые
                insert_query = """
                               INSERT INTO levels
                               (symbol, timeframe, price, type, strength,
                                upper, lower, distance, touched, broken, last_touched)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                               """
                for level in levels:
                    # Преобразуем numpy.float64 в float
                    price = float(level['price'])
                    distance = float(level.get('distance', 0.0))  # Добавить
                    cur.execute(insert_query, (
                        level['symbol'],
                        level['timeframe'],
                        price,
                        level['type'],
                        int(level['strength']),
                        float(level.get('upper')) if level.get('upper') else None,
                        float(level.get('lower')) if level.get('lower') else None,
                        distance,  # Исправлено
                        int(level.get('touched', 0)),
                        bool(level.get('broken', False)),
                        datetime.fromtimestamp(level['last_touched']) if level.get('last_touched') else None
                    ))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения уровней: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def get_levels(self):
        """Получение уровней из БД"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM levels")
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения уровней: {e}")
            return []
        finally:
            self.release_connection(conn)

    def _get_unique_symbols(self):
        """Получение уникальных торговых пар из БД"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT symbol FROM collected_candles")
                symbols = [row[0] for row in cur.fetchall()]
                return symbols
        except Exception as e:
            logger.error(f"❌ Ошибка получения уникальных пар: {e}")
            return []
        finally:
            self.release_connection(conn)

    def get_symbols_from_cache(self):
        """Получение уникальных торговых пар из кэша"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM pairs_cache")
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения пар из кэша: {e}")
            return []
        finally:
            self.release_connection(conn)

    def get_trend(self, symbol: str):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT direction, ema50, ema200
                    FROM trend_cache
                    WHERE symbol = %s
                    ORDER BY last_updated DESC
                    LIMIT 1
                """, (symbol,))
                row = cur.fetchone()
                if row:
                    return {
                        "direction": row[0],
                        "ema50": float(row[1]),
                        "ema200": float(row[2])
                    }
                return {}
        except Exception as e:
            logger.error(f"Ошибка получения тренда для {symbol}: {e}")
            return {}
        finally:
            self.release_connection(conn)

    def save_trends(self, trends):
        """Сохранение трендов в БД"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Используем UPSERT для обновления или вставки
                insert_query = """
                               INSERT INTO trend_cache (symbol, direction, ema50, ema200, last_updated)
                               VALUES (%s, %s, %s, %s, TO_TIMESTAMP(%s)) ON CONFLICT (symbol) 
                    DO \
                               UPDATE SET
                                   direction = EXCLUDED.direction, \
                                   ema50 = EXCLUDED.ema50, \
                                   ema200 = EXCLUDED.ema200, \
                                   last_updated = EXCLUDED.last_updated \
                               """
                for symbol, data in trends.items():
                    cur.execute(insert_query, (
                        symbol,
                        data['direction'],
                        float(data['ema50']),
                        float(data['ema200']),
                        data['last_updated']
                    ))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения трендов: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def get_current_price(self, symbol, timeframe):
        """Получение текущей цены закрытия"""
        candles = self.get_candles(symbol, timeframe)
        if candles:
            return candles[-1]['close']
        return None

    def save_alerts(self, alerts):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                for alert in alerts:
                    cur.execute("""
                                INSERT INTO alerts (symbol, level_price, current_price, type, distance, strength,
                                                    timeframe, source)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    alert["symbol"],
                                    alert["level_price"],
                                    alert["current_price"],
                                    alert["type"],
                                    alert["distance"],
                                    alert["strength"],
                                    alert["timeframe"],
                                    alert.get("source", "level")
                                ))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка сохранения алертов: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

    def get_alerts(self):
        """Получение последних алертов"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, level_price AS price, type AS signal_type, timeframe
                    FROM alerts
                    ORDER BY created_at DESC
                    LIMIT 200
                """)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения алертов: {e}")
            return []
        finally:
            self.release_connection(conn)

    def save_signals(self, signals):
        if not signals:
            logger.info("📭 Нет сигналов для сохранения")
            return

        query = """
        INSERT INTO signals (
            symbol, timeframe, signal_type, price,
            recommendation, score, details, current_price, time
        )
        VALUES %s
        ON CONFLICT (symbol, timeframe, signal_type, time) DO UPDATE
        SET current_price = EXCLUDED.current_price,
            recommendation = EXCLUDED.recommendation,
            score = EXCLUDED.score,
            details = EXCLUDED.details
        """

        # Убираем np.float64 → float
        for s in signals:
            if isinstance(s.get("current_price"), np.generic):
                s["current_price"] = float(s["current_price"])

        values = [
            (
                s["symbol"],
                s["timeframe"],
                s["signal_type"],
                s.get("price", s.get("current_price", 0.0)),  # → поле price
                s.get("recommendation", ""),
                s.get("score", 0),
                s.get("details", ""),
                s.get("current_price", 0.0),
                int(time.time() * 1000)
            )
            for s in signals
        ]

        conn = self.connection_pool.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, query, values)
            conn.commit()
            logger.info(f"💾 Сохранено {len(signals)} сигналов в базу данных")
        finally:
            self.connection_pool.putconn(conn)

    def get_signals(self, limit=100):
        query = """
        SELECT symbol, timeframe, signal_type, current_price, recommendation, score, details, time
        FROM signals
        ORDER BY time DESC
        LIMIT %s
        """
        conn = self.connection_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall()
        finally:
            self.connection_pool.putconn(conn)

        result = []
        for row in rows:
            result.append({
                "symbol": row[0],
                "timeframe": row[1],
                "signal_type": row[2],
                "current_price": row[3],
                "recommendation": row[4],
                "score": row[5],
                "details": row[6],
                "created_at": int(row[7]) // 1000  # UNIX timestamp в секундах
            })
        return result

    def get_indicators(self, symbol, timeframe):
        """Получение всех индикаторов по паре и таймфрейму"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT indicator_type, value
                    FROM indicators
                    WHERE symbol = %s AND timeframe = %s
                    ORDER BY created_at DESC
                """, (symbol, timeframe))
                rows = cur.fetchall()
                return {row[0]: row[1] for row in rows}
        except Exception as e:
            logger.error(f"Ошибка получения индикаторов: {e}")
            return {}
        finally:
            self.release_connection(conn)

    def get_market_cap(self, days=30):
        """Получение капитализации за последние N дней"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_cap, fetched_at
                    FROM market_cap
                    WHERE fetched_at >= NOW() - INTERVAL '%s days'
                    ORDER BY fetched_at ASC
                """, (days,))
                return cur.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения капитализации: {e}")
            return []
        finally:
            self.release_connection(conn)

    def truncate_all_tables(self):
        """Очистка содержимого всех таблиц"""
        logger.info("⏳ Начало очистки таблиц...")
        tables = [
            "collected_candles", "levels", "alerts",
            "pairs_cache", "trend_cache", "indicators", "signals"
        ]
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f"TRUNCATE TABLE {table};")
                    logger.debug(f"✅ Таблица {table} очищена")
            conn.commit()
            logger.info("✅ Все таблицы успешно очищены")
        except Exception as e:
            logger.error(f"❌ Ошибка очистки таблиц: {e}")
            conn.rollback()
        finally:
            self.release_connection(conn)

