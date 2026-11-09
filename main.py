import os
import time
import threading
import queue
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pathlib import Path
from pypinyin import lazy_pinyin, Style
import asyncio

load_dotenv()
app = FastAPI(title="电影爬虫")

BASE_DIR = Path(__file__).parent
TEMPLATE_DIR = BASE_DIR / "templates"


def render_template(filename: str):
    path = TEMPLATE_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# 任务控制
task_log_queues = {}
task_cancel_flags = {}
task_threads = {}

# PostgreSQL 数据库配置
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = int(os.getenv("PG_PORT", 6543))
DB_NAME = os.getenv("PG_DATABASE")
DB_USER = os.getenv("PG_USER")
DB_PASS = os.getenv("PG_PASSWORD")

MAX_THREAD = 3
BATCH_SIZE = 20
PAGE_DELAY = 0.8

KEEP_FIELDS = [
    "vod_id", "type_id", "type_name", "type_id_1",
    "vod_name", "vod_sub", "vod_en", "vod_letter",
    "vod_class", "vod_pic", "vod_actor", "vod_director",
    "vod_area", "vod_lang", "vod_year", "vod_douban_id",
    "vod_content", "vod_remarks",
    "vod_score", "vod_play_url", "vod_status", "vod_time",
    "vod_name_letter"
]


def get_chinese_first_letter(text: str) -> str:
    """
    获取字符串中所有汉字拼音首字母，过滤非字母字符，只保留大写字母，最多50个字符
    :param text: 输入混合文字
    :return: 大写首字母拼接字符串（仅字母，最长50位）
    """
    if not text:
        return ""
    result = []
    # 遍历每个字符单独转拼音取首字母
    for char in text:
        if '\u4e00' <= char <= '\u9fff':  # 判断是否汉字
            pinyin_list = lazy_pinyin(char, style=Style.FIRST_LETTER)
            if pinyin_list:
                first_letter = pinyin_list[0].upper()
                # 只保留字母
                if first_letter.isalpha():
                    result.append(first_letter)
        else:
            # 非汉字只保留英文字母并转大写，数字符号直接丢弃
            if char.isalpha():
                result.append(char.upper())
            elif char.isdigit():
                result.append(char)
        # 提前截断，避免多余遍历
        if len(result) >= 50:
            break
    # 最多截取前50个大写字母返回
    return ''.join(result[:50])


def console_log(task_id, msg):
    log_msg = f"[{task_id}] {msg}"
    print(log_msg)
    if task_id in task_log_queues:
        try:
            task_log_queues[task_id].put_nowait(msg)
        except queue.Full:
            pass


def get_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "close"
    })
    return session


def clean_field(val):
    if not val:
        return ""
    return val.split("$$$")[-1] if "$$$" in val else val


def clean_video_data(v):
    cleaned = {}
    for k in KEEP_FIELDS:
        if k == "vod_name_letter":
            # 根据影片名称生成拼音首字母
            vod_name = v.get("vod_name", "")
            cleaned[k] = get_chinese_first_letter(vod_name)
        else:
            value = v.get(k, "")
            if k == "vod_play_url":
                value = clean_field(value)
            cleaned[k] = value
    return cleaned


# ===================== 修复：增加重试 + sslmode="require" =====================
def get_conn(retry=2):
    for i in range(retry):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                connect_timeout=10,
                sslmode="require"
            )
        except Exception as e:
            print(f"❌ DB连接失败 重试{i+1}/{retry}: {e}")
            time.sleep(1)
    return None


# 建表（PostgreSQL 语法）
def create_table_if_not_exists(table_name):
    if table_name.lower() == "vod":
        return True
    try:
        conn = get_conn()
        if conn is None:
            print("❌ 获取数据库连接失败，建表失败")
            return False
        cur = conn.cursor()

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            LIKE vod INCLUDING ALL
        );
        """
        cur.execute(create_sql)
        conn.commit()

        cur.close()
        conn.close()
        return True
    except Exception as e:
        print("建表失败", e)
        return False


def movie_spider(task_id, table_name, api_base_url, start_page=1):
    cache = []
    cache_lock = threading.Lock()

    console_log(task_id, "✅ 任务启动")
    console_log(task_id, f"✅ 表名：{table_name}")
    console_log(task_id, f"✅ API：{api_base_url}")
    console_log(task_id, f"✅ 从第 {start_page} 页开始爬")

    if not create_table_if_not_exists(table_name):
        console_log(task_id, "❌ 数据表创建失败")
        return
    console_log(task_id, "✅ 数据表准备完成")

    # PostgreSQL 批量插入 + 存在更新
    def real_batch_insert(videos):
        if not videos:
            return
        try:
            conn = get_conn()
            if conn is None:
                console_log(task_id, f"❌ 获取数据库连接失败，跳过本次入库")
                return
            cur = conn.cursor()
            cleaned = [clean_video_data(v) for v in videos]
            cols = list(cleaned[0].keys())

            insert_sql = sql.SQL("""
                                INSERT INTO {table} ({fields})
                                VALUES %s
                                ON CONFLICT (vod_id) DO
                                UPDATE SET
                                    {updates}
                                """).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, cols)),
                updates=sql.SQL(', ').join(
                    [sql.Identifier(c) + sql.SQL(" = EXCLUDED.") + sql.Identifier(c) for c in cols]
                )
            )

            values = [tuple(item[k] for k in cols) for item in cleaned]
            execute_values(cur, insert_sql, values)

            conn.commit()
            cur.close()
            conn.close()
            console_log(task_id, f"✅ 批量入库 {len(videos)} 条")
        except Exception as e:
            console_log(task_id, f"❌ 入库失败：{str(e)}")

    def add_cache(v):
        nonlocal cache
        with cache_lock:
            cache.extend(v)
            if len(cache) >= BATCH_SIZE:
                real_batch_insert(cache)
                cache.clear()

    def flush():
        nonlocal cache
        with cache_lock:
            if cache:
                real_batch_insert(cache)
                cache.clear()

    def get_total():
        try:
            r = get_session().get(f"{api_base_url}?pg=1", timeout=8)
            r.raise_for_status()
            total = int(r.json().get("pagecount", 1))
            console_log(task_id, f"🚀 检测到总页数：{total}")
            return total
        except Exception as e:
            console_log(task_id, f"⚠️ 获取总页数失败：{str(e)}，默认 5000 页")
            # 标记任务取消
            task_cancel_flags[task_id] = True
            return 0  # 返回0页，直接结束爬取

    def crawl_page(page):
        if task_cancel_flags.get(task_id, False):
            return
        try:
            time.sleep(PAGE_DELAY)
            session = get_session()
            detail_resp = session.get(f"{api_base_url}?ac=detail&pg={page}", timeout=10)
            detail_resp.raise_for_status()
            detail = detail_resp.json()
            add_cache(detail.get("list", []))
            console_log(task_id, f"✅ 第{page}页抓取完成")
        except Exception as e:
            console_log(task_id, f"⚠️ 第{page}页抓取失败，已跳过：{str(e)[:50]}")

    try:
        total = get_total()
        console_log(task_id, f"⚡ 开始并发爬取（线程={MAX_THREAD}）")
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(MAX_THREAD) as executor:
            pages = range(start_page, total + 1)
            executor.map(crawl_page, pages)
        flush()
        console_log(task_id, "🎉 全部爬取完成！")
    except Exception as e:
        console_log(task_id, f"❌ 任务异常：{str(e)}")


# 异步日志流
async def log_stream(task_id: str, request: Request):
    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no"
    }

    async def generator():
        try:
            while True:
                if await request.is_disconnected():
                    print(f"Client {task_id} disconnected")
                    break

                yield ": keep-alive\n\n"
                await asyncio.sleep(2)

                while not task_log_queues[task_id].empty():
                    msg = task_log_queues[task_id].get()
                    yield f"data: {msg}\n\n"

                if task_cancel_flags.get(task_id, False) and task_log_queues[task_id].empty():
                    yield "data: 🛑 任务结束，连接关闭\n\n"
                    break
        except Exception as e:
            print(f"Stream error: {e}")

    return StreamingResponse(generator(), headers=headers)


# 路由
@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(render_template("index.html"))


@app.get("/api/ping")
async def ping():
    return {"ok": True, "time": time.time()}


@app.get("/api/run")
async def run_task(request: Request, taskId: str, tableName: str, apiUrl: str, startPage: int = 1):
    task_cancel_flags[taskId] = False
    task_log_queues[taskId] = queue.Queue(maxsize=500)

    t = threading.Thread(
        target=movie_spider,
        args=(taskId, tableName, apiUrl, startPage),
        daemon=True
    )
    t.start()
    task_threads[taskId] = t

    return await log_stream(taskId, request)


@app.get("/api/cancel")
async def cancel(taskId: str):
    task_cancel_flags[taskId] = True
    console_log(taskId, "🛑 前端触发取消任务")
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
