#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库初始化脚本

功能：
1. 创建所有数据表（如果不存在）
2. 创建默认管理员用户 (admin/admin123)
3. 初始化系统设置

使用方法：
- 服务器启动时自动调用
- 也可以手动运行：python -m common.db.init_database
"""

from __future__ import annotations

import logging
import warnings
from contextlib import asynccontextmanager, contextmanager

from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, SAWarning

from common.db.default_publish_addresses import (
    REMOVED_PUBLISH_ADDRESS_PREFIXES,
    build_default_publish_addresses
)
from common.db.auto_relist_schema import ensure_auto_relist_schema
from common.db.session import async_engine, async_session_maker
from common.utils.time_utils import get_beijing_now_naive
from common.utils.security import generate_secret_key, get_password_hash


# 建表/改表时的锁等待上限（秒）。MySQL 的 lock_wait_timeout 默认 31536000 秒（一年），
# 一旦某张表被别的事务或 ALTER 占着元数据锁，DDL 会无限挂住且没有任何日志，
# 收敛成 30 秒后报错，让启动日志能直接看出是锁冲突。
DDL_LOCK_WAIT_TIMEOUT_SECONDS = 30

# 多服务（backend-web / scheduler / websocket 等）同时启动时，用 MySQL 命名锁把自检串行化，
# 避免彼此争抢同一张表的元数据锁。名称与库同级，不同库互不影响。
INIT_ADVISORY_LOCK_NAME = "xy_db_init"
INIT_ADVISORY_LOCK_WAIT_SECONDS = 300


@contextmanager
def suppress_db_warnings():
    """
    上下文管理器：抑制数据库初始化时的重复警告日志
    
    包括：
    - Table 'xxx' already exists
    - Duplicate entry 'xxx' for key 'PRIMARY'
    """
    # 保存原始日志级别
    sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
    original_level = sqlalchemy_logger.level
    
    # 过滤MySQL警告
    warnings.filterwarnings('ignore', category=SAWarning)
    
    # 设置自定义过滤器
    class DBInitFilter(logging.Filter):
        """过滤数据库初始化时的常见警告"""
        IGNORE_PATTERNS = [
            'already exists',
            'Duplicate entry',
        ]
        
        def filter(self, record):
            msg = record.getMessage()
            for pattern in self.IGNORE_PATTERNS:
                if pattern in msg:
                    return False
            return True
    
    db_filter = DBInitFilter()
    
    # 添加过滤器到所有相关logger
    loggers_to_filter = [
        logging.getLogger('sqlalchemy.engine'),
        logging.getLogger('sqlalchemy.pool'),
        logging.getLogger('asyncmy'),
        logging.getLogger(),  # root logger
    ]
    
    for lg in loggers_to_filter:
        lg.addFilter(db_filter)
    
    try:
        yield
    finally:
        # 恢复原始状态
        for lg in loggers_to_filter:
            lg.removeFilter(db_filter)
        warnings.filterwarnings('default', category=SAWarning)


def describe_ddl_error(exc: Exception) -> str:
    """
    把建表/改表异常转成带排查提示的信息。

    Args:
        exc: DDL 执行抛出的异常。
    Returns:
        原始错误信息；锁等待超时时追加持锁方的排查指引。
    """
    message = str(exc)
    lowered = message.lower()
    if "lock wait timeout" in lowered or "metadata lock" in lowered:
        return (
            f"{message} ｜ 该表被其他连接的事务或 ALTER 占用元数据锁，"
            f"已按 {DDL_LOCK_WAIT_TIMEOUT_SECONDS} 秒上限放弃等待。"
            f"排查：SELECT trx_mysql_thread_id, trx_started, trx_query "
            f"FROM information_schema.innodb_trx ORDER BY trx_started; "
            f"确认非必要事务后 KILL 对应线程再重启服务"
        )
    return message


async def _apply_lock_wait_timeout(conn) -> None:
    """
    给当前连接设置锁等待上限，避免建表/改表卡在元数据锁上无限等待。

    Args:
        conn: 已建立的数据库连接。
    Returns:
        无返回值；非 MySQL 或无权限时只记录告警，沿用数据库默认值。
    """
    try:
        await conn.execute(text(f"SET SESSION lock_wait_timeout = {DDL_LOCK_WAIT_TIMEOUT_SECONDS}"))
        await conn.execute(
            text(f"SET SESSION innodb_lock_wait_timeout = {DDL_LOCK_WAIT_TIMEOUT_SECONDS}")
        )
    except Exception as exc:
        logger.warning(f"设置锁等待上限失败，本次自检沿用数据库默认超时: {exc}")


@asynccontextmanager
async def ddl_connection():
    """
    获取执行建表/改表用的连接，已带锁等待上限。

    Returns:
        事务内的数据库连接，退出时自动提交。
    """
    async with async_engine.begin() as conn:
        await _apply_lock_wait_timeout(conn)
        yield conn


@asynccontextmanager
async def init_advisory_lock():
    """
    用 MySQL 命名锁把多服务的启动自检串行化。

    拿不到锁（超时或数据库不支持）时不阻断启动，只记录告警后继续执行自检：
    此时建表/改表已有 30 秒锁等待上限，最坏情况是单张表报错并在日志中写明，
    不会像以前那样无提示地永久卡住。

    Returns:
        上下文本身不返回值，退出时释放命名锁。
    """
    acquired = False
    conn = None
    try:
        conn = await async_engine.connect()
        result = await conn.execute(
            text("SELECT GET_LOCK(:lock_name, :wait_seconds)"),
            {
                "lock_name": INIT_ADVISORY_LOCK_NAME,
                "wait_seconds": INIT_ADVISORY_LOCK_WAIT_SECONDS,
            }
        )
        acquired = result.scalar() == 1
        if not acquired:
            logger.warning(
                f"等待 {INIT_ADVISORY_LOCK_WAIT_SECONDS} 秒仍未取得数据库自检锁 "
                f"{INIT_ADVISORY_LOCK_NAME}，可能有其他服务正在执行自检；"
                f"本次继续执行，如遇表被占用会在日志中逐表提示"
            )
    except Exception as exc:
        logger.warning(f"获取数据库自检锁失败（不影响自检执行）: {exc}")

    try:
        yield
    finally:
        if conn is not None:
            try:
                if acquired:
                    await conn.execute(
                        text("SELECT RELEASE_LOCK(:lock_name)"),
                        {"lock_name": INIT_ADVISORY_LOCK_NAME}
                    )
            except Exception as exc:
                logger.warning(f"释放数据库自检锁失败（连接关闭后会自动释放）: {exc}")
            finally:
                await conn.close()


class DatabaseInitializer:
    """数据库初始化器"""

    DEFAULT_SETTINGS = (
        (
            "disclaimer.title",
            "免责声明",
            "系统免责声明标题"
        ),
        (
            "disclaimer.content",
            "数据存储说明\n1. 本系统在运行过程中，为保障服务正常运行，会存储用户账号密码、登录 Cookie、商品信息、卡券信息等业务数据。\n2. 上述数据仅用于系统功能运行、自动化处理和业务管理，不作为其他用途。\n3. 请您自行确认服务器环境、账号权限和数据保管措施的安全性。\n\n用户须知\n1. 用户应确保使用本系统的行为符合相关平台规则和法律法规。\n2. 因用户自身违规操作、账号共享、密码泄露、服务器安全问题导致的损失，由用户自行承担。\n3. 建议用户定期备份重要数据，因系统故障、第三方平台变更、不可抗力等导致的异常或损失，本系统不承担责任。\n4. 本系统依赖第三方平台接口和网络环境，无法保证服务始终连续、稳定、无中断。\n\n隐私与风险提示\n1. 请勿在未充分评估风险的情况下接入生产环境或敏感账号。\n2. 使用本系统即表示您已充分理解并接受相关风险，并愿意自行承担相应责任。",
            "系统免责声明正文"
        ),
        (
            "disclaimer.checkbox_text",
            "我已阅读并同意以上免责声明",
            "免责声明勾选提示文案"
        ),
        (
            "disclaimer.agree_button_text",
            "同意并继续",
            "免责声明同意按钮文案"
        ),
        (
            "disclaimer.disagree_button_text",
            "不同意",
            "免责声明不同意按钮文案"
        ),
        (
            "login.system_name",
            "闲鱼管理系统",
            "登录页系统名称"
        ),
        (
            "login.system_title",
            "高效专业的\n闲鱼自动化管理平台",
            "登录页系统标题"
        ),
        (
            "login.system_description",
            "自动回复、智能客服、订单管理、数据分析，一站式解决闲鱼运营难题",
            "登录页系统描述"
        ),
        (
            "auth.footer_ad_html",
            "© 2026 划算云服务器 ·<a href=\"http://www.hsykj.com\" target=\"_BLANK\">www.hsykj.com</a>",
            "登录页和注册页底部广告 HTML"
        ),
        (
            "theme.effect",
            "solid",
            "系统主题效果（solid-纯色，gradient-炫彩）"
        ),
        (
            "theme.color_preset",
            "ocean",
            "系统主题颜色预设"
        ),
        (
            "log.retention_days",
            "7",
            "日志保留天数（所有模块生效，修改后重启服务生效）"
        ),
        (
            "password_login.mode",
            "browser",
            "账号密码登录模式：protocol/browser"
        ),
        (
            "captcha.real_mouse_weight_local",
            "1",
            "real_mouse过滑块本地排队权重"
        ),
        (
            "captcha.real_mouse_weight_remote",
            "1",
            "real_mouse过滑块远程排队权重"
        ),
        (
            "captcha.block_remote_calls",
            "true",
            "是否禁止外部远程调用backend-web过滑块接口"
        ),
        (
            "captcha.local_slider_disabled",
            "false",
            "本机是否停止处理滑块；缓存缺失时仍请求Token接口"
        ),
        (
            "captcha.slider_mode",
            "browser",
            "滑块滑动方式：browser/real_mouse"
        ),
        (
            "token.api_mode",
            "web",
            "Token获取方式：web-网页接口/remote-远程接口"
        ),
        (
            "captcha.remote_processing_max",
            "20",
            "远程调用允许的最大处理中滑块日志数，0=不限制"
        ),
        (
            "captcha.remote_cooldown_seconds",
            "600",
            "远程调用达到处理中上限后的冷却秒数，0=不冷却"
        ),
        (
            "captcha.remote_cooldown_until",
            "0",
            "远程过滑块调用冷却截止时间戳"
        ),
        (
            "show_default_login_info",
            "true",
            "登录页是否展示默认账号密码提示"
        )
    )

    DEFAULT_SCHEDULED_TASKS = (
        (
            "redelivery",
            "补发货任务",
            5,
            True,
            "定时补发货任务"
        ),
        (
            "rate",
            "补评价任务",
            20,
            True,
            "定时补评价任务"
        ),
        (
            "polish",
            "擦亮任务",
            60,
            True,
            "定时擦亮商品任务"
        ),
        (
            "day_switch",
            "平台日切换任务",
            60,
            True,
            "定时执行平台日切换任务"
        ),
        (
            "cleanup_browser_data",
            "清理被禁用账号浏览器数据任务",
            600,
            False,
            "定时清理被禁用账号的浏览器数据"
        ),
        (
            "cleanup_unconfigured_browser_data",
            "清理未配置账号密码的浏览器数据任务",
            10800,
            False,
            "定时清理未配置登录账号密码（username 或 login_password 为空）账号的浏览器数据"
        ),
        (
            "fetch_orders",
            "获取闲鱼订单任务",
            600,
            True,
            "定时获取闲鱼订单数据"
        ),
        (
            "fetch_pending_orders",
            "获取待发货订单任务",
            60,
            True,
            "定时获取待发货订单并同步收货人姓名/手机号/地址等信息"
        ),
        (
            "fetch_refund_orders",
            "退款订单获取任务",
            120,
            True,
            "定时获取退款订单数据，更新订单状态并触发退款订单注销"
        ),
        (
            "fetch_items",
            "获取闲鱼商品任务",
            1200,
            True,
            "定时获取所有启用账号的闲鱼在售商品并入库（新增或更新）"
        ),
        (
            "login_renew",
            "登录续期任务",
            600,
            False,
            "定时执行闲鱼账号登录续期"
        ),
        (
            "token_renewal",
            "Token续期任务",
            20,
            True,
            "定时为未来1小时内到期的启用账号预取IM Token，并写入续期到期日"
        ),
        (
            "cookies_refresh",
            "COOKIES续期任务",
            600,
            False,
            "定时执行闲鱼账号浏览器COOKIES续期"
        ),
        (
            "api_cookie_renew",
            "接口续期Cookies任务",
            3600,
            True,
            "定时通过 hasLogin.do 接口为启用账号续期Cookies并同步Set-Cookie"
        ),
        (
            "close_notice",
            "关闭账号消息通知任务",
            600,
            False,
            "定时关闭账号消息通知"
        ),
        (
            "red_flower",
            "求小红花任务",
            300,
            True,
            "定时自动求小红花"
        ),
        (
            "db_backup",
            "数据库备份任务",
            3600,
            True,
            "定时备份数据库所有表结构与数据到文件"
        ),
        (
            "delivery_timeout",
            "发货超时检测任务",
            60,
            True,
            "定时将超过阈值仍处于 unknown 的自动发货消息日志标记为 timeout"
        ),
        (
            "listing_monitor",
            "商品监控任务",
            60,
            True,
            "定时执行商品监控：按监控类型调用闲鱼搜索接口采集商品并入库，每次记录监控日志"
        ),
        (
            "seller_fill",
            "采集商品卖家ID补全",
            60,
            True,
            "定时查询采集商品中卖家ID为空的数据，调用商品详情接口补全卖家真实ID与详情"
        ),
        (
            "dm_send",
            "采集商品发送私信",
            60,
            True,
            "定时查询卖家ID已补全且未私信的采集商品，用监控任务配置的私信账号发起私信"
        ),
        (
            "auto_order",
            "采集商品自动下单",
            60,
            True,
            "定时查询已私信且未下单的采集商品，用监控任务配置的下单账号创建订单（拍下，不自动付款）"
        ),
        (
            "image_cleanup",
            "图片清理",
            1200,
            True,
            "定时扫描卡券与素材库专属图片目录，删除已删除对象遗留的孤儿图片（仅清理各自目录，不影响其它功能图片）"
        ),
        (
            "auto_relist_scan",
            "商品自动续售",
            5,
            True,
            "检测已成交并完成发货的商品，确认旧商品下架后使用原素材自动续售"
        )
    )
    
    # ========== 所有数据表的DDL定义 ==========
    # 表名统一使用 xy_ 前缀
    # 所有表都有主键，无外键约束
    
    TABLES_DDL = {
        # 1. 用户表
        "xy_users": """
            CREATE TABLE IF NOT EXISTS xy_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id VARCHAR(64),
                username VARCHAR(64) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL UNIQUE,
                phone VARCHAR(32),
                password_hash VARCHAR(255) NOT NULL,
                status TEXT DEFAULT 'ACTIVE',
                role TEXT DEFAULT 'MEMBER',
                account_limit INT DEFAULT NULL,
                last_login_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 2. 用户设置表
        "xy_user_settings": """
            CREATE TABLE IF NOT EXISTS xy_user_settings (
                id INT PRIMARY KEY ,
                user_id INT NOT NULL,
                key VARCHAR(120) NOT NULL,
                value TEXT NOT NULL,
                description TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, key)
            );
        """,
        
        # 3. 系统设置表
        "xy_system_settings": """
            CREATE TABLE IF NOT EXISTS xy_system_settings (
                key VARCHAR(120) PRIMARY KEY,
                value TEXT NOT NULL,
                description TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
        
        # 4. 闲鱼账号表
        "xy_accounts": """
            CREATE TABLE IF NOT EXISTS xy_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                display_name VARCHAR(120),
                unb VARCHAR(64),
                cookie TEXT NOT NULL,
                login_method VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'active',
                username VARCHAR(120),
                login_password TEXT,
                remark VARCHAR(255),
                pause_duration INT DEFAULT 10,
                auto_confirm TINYINT(1) DEFAULT 0,
                show_browser TINYINT(1) DEFAULT 0,
                metadata TEXT,
                last_login_at DATETIME,
                last_refresh_at DATETIME,
                proxy_type VARCHAR(20) DEFAULT 'none',
                proxy_host VARCHAR(255),
                proxy_port INT,
                proxy_user VARCHAR(120),
                proxy_pass VARCHAR(255),
                message_expire_time INT DEFAULT 3600,
                reply_delay_seconds INT DEFAULT 0,
                disable_reason VARCHAR(255),
                scheduled_redelivery TINYINT(1) NOT NULL DEFAULT 0,
                scheduled_rate TINYINT(1) NOT NULL DEFAULT 0,
                auto_polish TINYINT(1) NOT NULL DEFAULT 0,
                confirm_before_send TINYINT(1) NOT NULL DEFAULT 0,
                only_send_card TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id)
            );
        """,

        # 5. 关键词规则表
        "xy_keyword_rules": """
            CREATE TABLE IF NOT EXISTS xy_keyword_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                account_id INTEGER,
                keyword VARCHAR(120) NOT NULL,
                reply_content TEXT,
                reply_type VARCHAR(32),
                image_url VARCHAR(512),
                location_name VARCHAR(255),
                location_longitude VARCHAR(32),
                location_latitude VARCHAR(32),
                location_title VARCHAR(128),
                location_subtitle VARCHAR(255),
                item_id VARCHAR(64),
                priority INT DEFAULT 100,
                is_active TINYINT(1) DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
        
        # 6. 商品目录表
        "xy_catalog_items": """
            CREATE TABLE IF NOT EXISTS xy_catalog_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                item_id VARCHAR(64) NOT NULL,
                title VARCHAR(255),
                price VARCHAR(32),
                ai_prompt TEXT,
                is_polished TINYINT(1) DEFAULT 0,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
        
        # 8. 订单表
        "xy_orders": """
            CREATE TABLE IF NOT EXISTS xy_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                order_no VARCHAR(64) NOT NULL,
                status VARCHAR(32) NOT NULL,
                buyer_nick VARCHAR(120),
                buyer_id VARCHAR(64),
                chat_id VARCHAR(64),
                item_id VARCHAR(64),
                spec_name VARCHAR(120),
                spec_value VARCHAR(120),
                quantity INT DEFAULT 1,
                amount REAL,
                currency VARCHAR(8) DEFAULT 'CNY',
                account_id VARCHAR(64),
                account_name VARCHAR(120),
                is_bargain TINYINT(1) DEFAULT 0,
                receiver_name VARCHAR(120),
                receiver_phone VARCHAR(32),
                receiver_address VARCHAR(512),
                delivery_fail_reason VARCHAR(2000),
                card_only_delivered TINYINT(1) NOT NULL DEFAULT 0,
                item_snapshot TEXT,
                metadata TEXT,
                source VARCHAR(32),
                placed_at DATETIME,
                synced_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id, order_no)
            );
        """,

        # 9. 卡券表
        "xy_cards": """
            CREATE TABLE IF NOT EXISTS xy_cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_id VARCHAR(64),
                name VARCHAR(255) NOT NULL,
                type VARCHAR(50) NOT NULL,
                description TEXT,
                enabled TINYINT(1) DEFAULT 1,
                delay_seconds INT DEFAULT 0,
                use_no_logistics_form TINYINT(1) NOT NULL DEFAULT 0,
                delivery_count INT DEFAULT 0,
                price VARCHAR(32),
                is_dockable TINYINT(1) DEFAULT 0,
                fee_payer VARCHAR(32),
                is_multi_spec TINYINT(1) DEFAULT 0,
                spec_name VARCHAR(255),
                spec_value VARCHAR(255),
                api_config TEXT,
                text_content TEXT,
                data_content TEXT,
                image_url VARCHAR(512),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
        
        # 10. 默认回复表
        "xy_default_replies": """
            CREATE TABLE IF NOT EXISTS xy_default_replies (
                id INT PRIMARY KEY ,
                account_id VARCHAR(80) NOT NULL,
                item_id VARCHAR(64) DEFAULT NULL,
                enabled TINYINT(1) DEFAULT 0,
                reply_type VARCHAR(32) DEFAULT 'text',
                reply_content TEXT,
                reply_image VARCHAR(512),
                api_url VARCHAR(1024) DEFAULT NULL,
                api_timeout INT DEFAULT 80,
                location_name VARCHAR(255) DEFAULT NULL,
                location_longitude VARCHAR(32) DEFAULT NULL,
                location_latitude VARCHAR(32) DEFAULT NULL,
                location_title VARCHAR(128) DEFAULT NULL,
                location_subtitle VARCHAR(255) DEFAULT NULL,
                reply_once TINYINT(1) DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id, item_id)
            );
        """,
        
        # 11. 默认回复记录表
        "xy_default_reply_records": """
            CREATE TABLE IF NOT EXISTS xy_default_reply_records (
                id INT PRIMARY KEY ,
                account_id VARCHAR(80) NOT NULL,
                item_id VARCHAR(64) DEFAULT NULL,
                user_id VARCHAR(64) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
        
        # 12. AI聊天消息表
        "xy_ai_chat_messages": """
            CREATE TABLE IF NOT EXISTS xy_ai_chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id VARCHAR(64) NOT NULL,
                cookie_id VARCHAR(80) NOT NULL,
                user_id VARCHAR(64) NOT NULL,
                item_id VARCHAR(64),
                role VARCHAR(20) NOT NULL,
                content TEXT NOT NULL,
                intent VARCHAR(20),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 14. 风控日志表
        "xy_risk_control_logs": """
            CREATE TABLE IF NOT EXISTS xy_risk_control_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                account_id INTEGER,
                account_identifier VARCHAR(80),
                event_type VARCHAR(64) DEFAULT 'slider_captcha',
                event_description TEXT,
                processing_result TEXT,
                processing_status VARCHAR(32) DEFAULT 'processing',
                captcha_engine VARCHAR(32) DEFAULT NULL,
                call_type VARCHAR(16) DEFAULT 'local',
                call_user VARCHAR(128) DEFAULT NULL,
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 14.1 账号登录日志表（记录密码登录的每一次尝试与最终结果）
        "xy_account_login_logs": """
            CREATE TABLE IF NOT EXISTS xy_account_login_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                account_id INTEGER,
                account_identifier VARCHAR(80),
                username VARCHAR(255),
                trigger_reason VARCHAR(128),
                login_status VARCHAR(32) DEFAULT 'failed',
                failure_reason VARCHAR(64),
                error_message TEXT,
                updated_cookie_names VARCHAR(500) DEFAULT NULL,
                duration_ms INT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 14.2 用户登录日志表（记录系统用户登录本系统的每一次尝试与结果，成功/失败都写入）
        "xy_user_login_logs": """
            CREATE TABLE IF NOT EXISTS xy_user_login_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER DEFAULT NULL,
                login_account VARCHAR(255) DEFAULT NULL,
                login_method VARCHAR(32) NOT NULL,
                success TINYINT(1) NOT NULL DEFAULT 0,
                client_ip VARCHAR(64) DEFAULT NULL,
                result_message VARCHAR(512) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 14.3 邮件发送日志表（记录所有邮件触发尝试，成功/失败/限流/配置缺失都写入）
        "xy_email_logs": """
            CREATE TABLE IF NOT EXISTS xy_email_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER DEFAULT NULL,
                recipient_email VARCHAR(255) NOT NULL,
                trigger_operation VARCHAR(64) NOT NULL,
                success TINYINT(1) NOT NULL DEFAULT 0,
                client_ip VARCHAR(255) DEFAULT NULL,
                result_message TEXT DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 14.4 通用数据字典表（所有枚举值统一入库）
        "xy_data_dictionaries": """
            CREATE TABLE IF NOT EXISTS xy_data_dictionaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dict_type VARCHAR(64) NOT NULL,
                dict_value VARCHAR(64) NOT NULL,
                dict_label VARCHAR(128) NOT NULL,
                sort_order INT NOT NULL DEFAULT 0,
                enabled TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dict_type, dict_value)
            );
        """,

        # 15. 通知渠道表
        "xy_notification_channels": """
            CREATE TABLE IF NOT EXISTS xy_notification_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                name VARCHAR(120) NOT NULL,
                channel_type VARCHAR(32) NOT NULL,
                config TEXT,
                enabled TINYINT(1) DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
        
        # 16. 消息通知表
        "xy_message_notifications": """
            CREATE TABLE IF NOT EXISTS xy_message_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                account_pk INTEGER NOT NULL,
                account_identifier VARCHAR(80) NOT NULL,
                channel_id INTEGER NOT NULL,
                enabled TINYINT(1) DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 17. 消息过滤规则表
        "xy_message_filters": """
            CREATE TABLE IF NOT EXISTS xy_message_filters (
                id INT PRIMARY KEY ,
                account_id VARCHAR(80) NOT NULL,
                keyword VARCHAR(255) NOT NULL,
                filter_type VARCHAR(20) NOT NULL,
                enabled TINYINT(1) DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id, keyword, filter_type)
            );
        """,
        
        # 18. 意见反馈表
        "xy_feedbacks": """
            CREATE TABLE IF NOT EXISTS xy_feedbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                cookie_id VARCHAR(64),
                title VARCHAR(100) NOT NULL,
                content TEXT NOT NULL,
                feedback_type TEXT DEFAULT 'OTHER',
                images TEXT,
                is_resolved TINYINT(1) DEFAULT 0,
                resolved_at DATETIME,
                admin_reply TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 18.1 意见反馈消息表（对话记录）
        "xy_feedback_messages": """
            CREATE TABLE IF NOT EXISTS xy_feedback_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                feedback_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                is_admin TINYINT(1) DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 18.2 广告表
        "xy_advertisements": """
            CREATE TABLE IF NOT EXISTS xy_advertisements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title VARCHAR(200) NOT NULL,
                content TEXT,
                link VARCHAR(500),
                expire_date DATE,
                image_url VARCHAR(500),
                ad_type TEXT DEFAULT 'text',
                months INT,
                total_amount VARCHAR(32),
                status TEXT DEFAULT 'unpaid',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 19. Goofish 定时抓取任务表
        "xy_goofish_crawl_jobs": """
            CREATE TABLE IF NOT EXISTS xy_goofish_crawl_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                cookie_id VARCHAR(80) NOT NULL,
                keyword VARCHAR(80) NOT NULL,
                interval_seconds INT NOT NULL DEFAULT 900,
                start_page INT NOT NULL DEFAULT 1,
                pages INT NOT NULL DEFAULT 1,
                page_size INT NOT NULL DEFAULT 20,
                fetch_detail TINYINT(1) DEFAULT 1,
                detail_limit INT NOT NULL DEFAULT 20,
                enabled TINYINT(1) DEFAULT 1,
                last_run_at DATETIME,
                last_error TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 20. Goofish 定时抓取商品表
        "xy_goofish_crawl_items": """
            CREATE TABLE IF NOT EXISTS xy_goofish_crawl_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                item_id VARCHAR(64) NOT NULL,
                title TEXT,
                price VARCHAR(64),
                area VARCHAR(120),
                seller_name VARCHAR(120),
                item_url TEXT,
                main_image VARCHAR(512),
                publish_time VARCHAR(64),
                want_count INT,
                view_count INT,
                description TEXT,
                detail_error VARCHAR(255),
                raw_json TEXT,
                fetched_at DATETIME NOT NULL,
                UNIQUE(job_id, item_id)
            );
        """,

        # 21. 定时补发货执行日志表
        "xy_scheduled_redelivery_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_redelivery_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                order_no VARCHAR(64) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 22. 公告信息表
        "xy_announcements": """
            CREATE TABLE IF NOT EXISTS xy_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title VARCHAR(200) NOT NULL,
                content TEXT NOT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 22.1 弹窗公告表（用户每次登录时弹窗展示）
        "xy_popup_announcements": """
            CREATE TABLE IF NOT EXISTS xy_popup_announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title VARCHAR(200) NOT NULL,
                content TEXT NOT NULL,
                link VARCHAR(500) NULL,
                is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 23. 确认收货消息表
        "xy_confirm_receipt_messages": """
            CREATE TABLE IF NOT EXISTS xy_confirm_receipt_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id VARCHAR(80) NOT NULL,
                enabled TINYINT(1) DEFAULT 0,
                message_content TEXT,
                message_image VARCHAR(512),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id)
            );
        """,

        # 24. 自动评价配置表
        "xy_auto_rate_configs": """
            CREATE TABLE IF NOT EXISTS xy_auto_rate_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id VARCHAR(80) NOT NULL,
                enabled TINYINT(1) DEFAULT 0,
                rate_type VARCHAR(20) DEFAULT 'text',
                text_content TEXT,
                api_url VARCHAR(512),
                thanks_enabled TINYINT(1) DEFAULT 0,
                thanks_content TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id)
            );
        """,

        # 25. 定时补评价执行日志表
        "xy_scheduled_rate_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_rate_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                order_no VARCHAR(64) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 26. 定时擦亮执行日志表
        "xy_scheduled_polish_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_polish_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                item_id VARCHAR(64) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 26.1 登录续期执行日志表
        "xy_scheduled_login_renew_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_login_renew_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 26.1.1 Token 续期执行日志表
        "xy_scheduled_token_renewal_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_token_renewal_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                token_user_id VARCHAR(128) NOT NULL,
                status VARCHAR(20) NOT NULL,
                renew_expire_at DATETIME DEFAULT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 26.2 Cookie续期计划表
        "xy_cookie_refresh_schedules": """
            CREATE TABLE IF NOT EXISTS xy_cookie_refresh_schedules (
                id INTEGER NOT NULL ,
                account_id VARCHAR(80) NOT NULL,
                expire_at DATETIME NOT NULL,
                last_refresh_at DATETIME DEFAULT NULL,
                last_status VARCHAR(20) DEFAULT NULL,
                last_error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 26.3 COOKIES刷新日志表
        "xy_scheduled_cookies_refresh_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_cookies_refresh_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                status VARCHAR(20) NOT NULL,
                updated_cookie_count INT NOT NULL DEFAULT 0,
                next_expire_at DATETIME DEFAULT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 26.4 接口续期Cookies执行日志表
        "xy_scheduled_api_cookie_renew_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_api_cookie_renew_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                status VARCHAR(30) NOT NULL,
                updated_cookie_count INT NOT NULL DEFAULT 0,
                updated_cookie_names TEXT DEFAULT NULL,
                response_content TEXT DEFAULT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 27. 定时任务配置表
        "xy_scheduled_tasks": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_tasks (
                id INTEGER NOT NULL ,
                task_code VARCHAR(50) NOT NULL,
                task_name VARCHAR(100) NOT NULL,
                interval_seconds INT NOT NULL DEFAULT 60,
                enabled TINYINT(1) NOT NULL DEFAULT 1,
                description TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 28. 卡券与商品多对多关联表
        "xy_card_item_relations": """
            CREATE TABLE IF NOT EXISTS xy_card_item_relations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                card_id INTEGER NOT NULL,
                item_id VARCHAR(64) NOT NULL,
                source VARCHAR(20) DEFAULT 'own',
                dock_record_id INTEGER NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(card_id, item_id, dock_record_id)
            );
        """,

        # 29. 对接记录表
        "xy_dock_records": """
            CREATE TABLE IF NOT EXISTS xy_dock_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                card_id INTEGER NOT NULL,
                dock_name VARCHAR(255) NOT NULL,
                markup_amount VARCHAR(32) NOT NULL DEFAULT '0.00',
                remark TEXT,
                delivery_count INT NOT NULL DEFAULT 0,
                status TINYINT(1) DEFAULT 1,
                disable_reason VARCHAR(255) DEFAULT NULL,
                owner_disabled TINYINT(1) NOT NULL DEFAULT 0,
                level INT NOT NULL DEFAULT 1,
                parent_dock_id INTEGER DEFAULT NULL,
                source_user_id INTEGER DEFAULT NULL,
                allow_sub_dock TINYINT(1) DEFAULT 0,
                sub_dock_price VARCHAR(32) DEFAULT NULL,
                sub_dock_visibility VARCHAR(32) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 30. 资金流水表
        "xy_fund_flows": """
            CREATE TABLE IF NOT EXISTS xy_fund_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type VARCHAR(32) NOT NULL,
                amount VARCHAR(32) NOT NULL,
                balance_before VARCHAR(32) NOT NULL,
                balance_after VARCHAR(32) NOT NULL,
                order_id INTEGER DEFAULT NULL,
                dock_record_id INTEGER DEFAULT NULL,
                description VARCHAR(500) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 31. 充值订单表
        "xy_recharge_orders": """
            CREATE TABLE IF NOT EXISTS xy_recharge_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_no VARCHAR(64) NOT NULL,
                user_id INTEGER NOT NULL,
                amount VARCHAR(32) NOT NULL,
                order_type VARCHAR(20) NOT NULL DEFAULT 'recharge',
                status VARCHAR(32) NOT NULL DEFAULT 'pending',
                trade_no VARCHAR(128) DEFAULT NULL,
                qr_code VARCHAR(512) DEFAULT NULL,
                paid_at DATETIME DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 32. 对接码绑定表
        "xy_dock_code_bindings": """
            CREATE TABLE IF NOT EXISTS xy_dock_code_bindings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                dock_code VARCHAR(32) NOT NULL,
                target_user_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 33. 代理订单表（对接卡券发货记录）
        "xy_agent_orders": """
            CREATE TABLE IF NOT EXISTS xy_agent_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                order_no VARCHAR(64) NOT NULL,
                item_id VARCHAR(64) NOT NULL,
                card_id INTEGER NOT NULL,
                dock_record_id INTEGER NOT NULL,
                dock_level INT NOT NULL,
                sale_price VARCHAR(32) NOT NULL,
                dock_price VARCHAR(32) NOT NULL,
                card_price VARCHAR(32) DEFAULT NULL,
                level2_cost VARCHAR(32) DEFAULT NULL,
                profit VARCHAR(32) NOT NULL DEFAULT '0.00',
                fee_amount VARCHAR(32) DEFAULT NULL,
                fee_payer VARCHAR(32) DEFAULT NULL,
                upstream_user_id INTEGER DEFAULT NULL,
                upstream_dock_record_id INTEGER DEFAULT NULL,
                owner_user_id INTEGER DEFAULT NULL,
                delivery_content TEXT,
                buyer_id VARCHAR(64) DEFAULT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'delivered',
                settle_remark VARCHAR(500) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 34. Token缓存表
        "xy_token_cache": """
            CREATE TABLE IF NOT EXISTS xy_token_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id VARCHAR(128) NOT NULL,
                token TEXT NOT NULL,
                device_id VARCHAR(128) NOT NULL,
                expire_at DATETIME NOT NULL,
                renew_expire_at DATETIME DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id)
            );
        """,

        # 35. 结算记录表
        "xy_settlement_records": """
            CREATE TABLE IF NOT EXISTS xy_settlement_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                alipay_id VARCHAR(128) NOT NULL,
                amount VARCHAR(32) NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'pending_review',
                remark TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 36. 激活码生成日志表
        "xy_activation_logs": """
            CREATE TABLE IF NOT EXISTS xy_activation_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                machine_id VARCHAR(32) NOT NULL,
                code_type VARCHAR(20) NOT NULL,
                generated_code VARCHAR(255) NOT NULL,
                days INT NOT NULL,
                ip_address VARCHAR(64),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 37. 商品素材库表
        "xy_product_materials": """
            CREATE TABLE IF NOT EXISTS xy_product_materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title VARCHAR(200) NOT NULL,
                description TEXT NOT NULL,
                price REAL NOT NULL,
                original_price REAL DEFAULT NULL,
                category VARCHAR(100) DEFAULT NULL,
                platform_category_id VARCHAR(64) DEFAULT NULL,
                platform_category_name VARCHAR(100) DEFAULT NULL,
                platform_channel_category_id VARCHAR(64) DEFAULT NULL,
                platform_channel_category_name VARCHAR(100) DEFAULT NULL,
                platform_leaf_id VARCHAR(64) DEFAULT NULL,
                platform_tb_category_id VARCHAR(64) DEFAULT NULL,
                platform_category_path TEXT DEFAULT NULL,
                platform_attributes TEXT DEFAULT NULL,
                category_source VARCHAR(20) NOT NULL DEFAULT 'manual',
                category_confidence REAL DEFAULT NULL,
                images TEXT DEFAULT NULL,
                videos TEXT DEFAULT NULL,
                specifications TEXT DEFAULT NULL,
                sku_rows TEXT DEFAULT NULL,
                quantity INT NOT NULL DEFAULT 1,
                delivery_method VARCHAR(20) DEFAULT 'express',
                shipping_method VARCHAR(20) DEFAULT 'free',
                support_pickup TINYINT(1) NOT NULL DEFAULT 0,
                postage REAL DEFAULT 0,
                address VARCHAR(200) DEFAULT NULL,
                address_expected_text VARCHAR(200) DEFAULT NULL,
                brand VARCHAR(100) DEFAULT NULL,
                condition VARCHAR(20) DEFAULT '全新',
                remark VARCHAR(500) DEFAULT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 38.1 定时求小红花执行日志表
        "xy_scheduled_red_flower_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_red_flower_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                order_no VARCHAR(64) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 38.2 账号消息通知关闭执行日志表
        "xy_scheduled_close_notice_log": """
            CREATE TABLE IF NOT EXISTS xy_scheduled_close_notice_log (
                id INTEGER NOT NULL ,
                batch_id VARCHAR(36) NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message VARCHAR(500) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        # 38.3 数据库备份日志表（记录每次数据库备份任务的结果与备份文件信息）
        "xy_db_backup_log": """
            CREATE TABLE IF NOT EXISTS xy_db_backup_log (
                id INTEGER NOT NULL ,
                status VARCHAR(20) NOT NULL,
                file_name VARCHAR(255) DEFAULT NULL,
                file_path VARCHAR(500) DEFAULT NULL,
                file_size INTEGER DEFAULT NULL,
                table_count INT DEFAULT NULL,
                total_rows INTEGER DEFAULT NULL,
                duration_ms INT DEFAULT NULL,
                error_message VARCHAR(1000) DEFAULT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
        """,

        "xy_auto_reply_message_logs": """
            CREATE TABLE IF NOT EXISTS xy_auto_reply_message_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER DEFAULT NULL,
                owner_username VARCHAR(120) DEFAULT NULL,
                account_pk INTEGER DEFAULT NULL,
                account_id VARCHAR(80) NOT NULL,
                account_name VARCHAR(120) DEFAULT NULL,
                chat_id VARCHAR(128) NOT NULL,
                item_id VARCHAR(64) DEFAULT NULL,
                item_title VARCHAR(255) DEFAULT NULL,
                order_no VARCHAR(64) DEFAULT NULL,
                source_message_id VARCHAR(128) DEFAULT NULL,
                sender_user_id VARCHAR(64) NOT NULL,
                sender_user_name VARCHAR(120) DEFAULT NULL,
                source_message TEXT,
                source_message_time DATETIME DEFAULT NULL,
                process_status VARCHAR(20) NOT NULL DEFAULT 'processing',
                decision_reason VARCHAR(64) NOT NULL DEFAULT 'processing',
                reply_strategy VARCHAR(20) NOT NULL DEFAULT 'none',
                reply_mode VARCHAR(20) NOT NULL DEFAULT 'none',
                matched_keyword VARCHAR(255) DEFAULT NULL,
                matched_rule_type VARCHAR(32) DEFAULT NULL,
                default_reply_scope VARCHAR(20) DEFAULT NULL,
                default_reply_once TINYINT(1) NOT NULL DEFAULT 0,
                ai_model_name VARCHAR(120) DEFAULT NULL,
                ai_provider_name VARCHAR(80) DEFAULT NULL,
                reply_text TEXT,
                reply_image_url VARCHAR(1000) DEFAULT NULL,
                reply_segments TEXT DEFAULT NULL,
                error_message TEXT,
                send_status VARCHAR(20) NOT NULL DEFAULT 'unknown',
                send_fail_reason TEXT,
                raw_message_json TEXT DEFAULT NULL,
                context_snapshot TEXT DEFAULT NULL,
                send_result_json TEXT DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        "xy_publish_addresses": """
            CREATE TABLE IF NOT EXISTS xy_publish_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(120) NOT NULL,
                search_keyword VARCHAR(200) NOT NULL,
                expected_text VARCHAR(200) DEFAULT NULL,
                account_id VARCHAR(80) DEFAULT NULL,
                weight INT NOT NULL DEFAULT 1,
                sort_order INT NOT NULL DEFAULT 100,
                is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                use_count INT NOT NULL DEFAULT 0,
                last_used_at DATETIME DEFAULT NULL,
                created_by INTEGER DEFAULT NULL,
                remark VARCHAR(500) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        "xy_user_publish_addresses": """
            CREATE TABLE IF NOT EXISTS xy_user_publish_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                address VARCHAR(200) NOT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                use_count INT NOT NULL DEFAULT 0,
                last_used_at DATETIME DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 38. 商品发布日志表
        "xy_publish_logs": """
            CREATE TABLE IF NOT EXISTS xy_publish_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                account_id VARCHAR(80) NOT NULL,
                title VARCHAR(200) NOT NULL,
                description TEXT DEFAULT NULL,
                price VARCHAR(20) DEFAULT NULL,
                material_id INTEGER DEFAULT NULL,
                batch_id VARCHAR(36) DEFAULT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                item_url VARCHAR(500) DEFAULT NULL,
                item_id VARCHAR(64) DEFAULT NULL,
                error_message VARCHAR(1000) DEFAULT NULL,
                resolved_address_id INTEGER DEFAULT NULL,
                resolved_address_text VARCHAR(200) DEFAULT NULL,
                address_source VARCHAR(20) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 45.1 商品上新监控任务表
        # 45.1 商品监控分类表
        "xy_listing_monitor_categories": """
            CREATE TABLE IF NOT EXISTS xy_listing_monitor_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                name VARCHAR(100) NOT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 45.2 商品监控任务表
        "xy_listing_monitor_tasks": """
            CREATE TABLE IF NOT EXISTS xy_listing_monitor_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER DEFAULT NULL,
                category_id INTEGER DEFAULT NULL,
                monitor_type VARCHAR(20) NOT NULL DEFAULT 'listing',
                keyword VARCHAR(200) NOT NULL,
                price_min REAL DEFAULT NULL,
                price_max REAL DEFAULT NULL,
                publish_days INT DEFAULT NULL,
                interval_minutes INT NOT NULL DEFAULT 5,
                collect_pages INT NOT NULL DEFAULT 1,
                proxy_url VARCHAR(255) DEFAULT NULL,
                account_ids TEXT DEFAULT NULL,
                order_account_ids TEXT DEFAULT NULL,
                dm_content VARCHAR(1000) DEFAULT NULL,
                dm_batch_size INT NOT NULL DEFAULT 5,
                order_batch_size INT NOT NULL DEFAULT 5,
                direct_order TINYINT(1) NOT NULL DEFAULT 0,
                is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                last_run_at DATETIME DEFAULT NULL,
                created_by INTEGER DEFAULT NULL,
                remark VARCHAR(500) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 45.3 商品监控采集商品信息表
        "xy_listing_monitor_items": """
            CREATE TABLE IF NOT EXISTS xy_listing_monitor_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_task_id INTEGER NOT NULL,
                owner_id INTEGER DEFAULT NULL,
                item_id VARCHAR(64) NOT NULL,
                title VARCHAR(500) DEFAULT NULL,
                price VARCHAR(32) DEFAULT NULL,
                area VARCHAR(120) DEFAULT NULL,
                pic_url VARCHAR(1000) DEFAULT NULL,
                seller_id VARCHAR(120) DEFAULT NULL,
                seller_user_id VARCHAR(64) DEFAULT NULL,
                seller_nick VARCHAR(120) DEFAULT NULL,
                seller_avatar VARCHAR(1000) DEFAULT NULL,
                want_count VARCHAR(32) DEFAULT NULL,
                tags VARCHAR(500) DEFAULT NULL,
                publish_time DATETIME DEFAULT NULL,
                target_url VARCHAR(1000) DEFAULT NULL,
                raw_json TEXT DEFAULT NULL,
                detail_json MEDIUMTEXT DEFAULT NULL,
                seller_fill_status VARCHAR(20) DEFAULT NULL,
                seller_fill_fail_reason VARCHAR(500) DEFAULT NULL,
                is_dm_sent TINYINT(1) NOT NULL DEFAULT 0,
                dm_account_id VARCHAR(80) DEFAULT NULL,
                dm_chat_id VARCHAR(80) DEFAULT NULL,
                dm_status VARCHAR(20) DEFAULT NULL,
                dm_fail_reason VARCHAR(500) DEFAULT NULL,
                dm_attempts INT NOT NULL DEFAULT 0,
                is_ordered TINYINT(1) NOT NULL DEFAULT 0,
                order_id VARCHAR(64) DEFAULT NULL,
                order_account_id VARCHAR(80) DEFAULT NULL,
                order_status VARCHAR(20) DEFAULT NULL,
                order_fail_reason VARCHAR(500) DEFAULT NULL,
                order_attempts INT NOT NULL DEFAULT 0,
                dm_sent_at DATETIME DEFAULT NULL,
                ordered_at DATETIME DEFAULT NULL,
                last_seen_at DATETIME DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(monitor_task_id, item_id)
            );
        """,

        # 45.3 商品监控执行日志表
        "xy_listing_monitor_logs": """
            CREATE TABLE IF NOT EXISTS xy_listing_monitor_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_task_id INTEGER NOT NULL,
                owner_id INTEGER DEFAULT NULL,
                monitor_type VARCHAR(20) DEFAULT NULL,
                keyword VARCHAR(200) DEFAULT NULL,
                trigger_type VARCHAR(10) NOT NULL DEFAULT 'auto',
                account_id VARCHAR(80) DEFAULT NULL,
                used_account_ids TEXT DEFAULT NULL,
                pages INT NOT NULL DEFAULT 0,
                fetched_count INT NOT NULL DEFAULT 0,
                inserted_count INT NOT NULL DEFAULT 0,
                updated_count INT NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL DEFAULT 'success',
                message VARCHAR(1000) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 45.6 用户级兜底下单账号配置表（任务无可用下单账号时回退使用）
        "xy_order_fallback_accounts": """
            CREATE TABLE IF NOT EXISTS xy_order_fallback_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                category_id INTEGER DEFAULT NULL,
                account_ids TEXT DEFAULT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(owner_id, category_id)
            );
        """,

        # 45.7 用户级兜底采集账号配置表（任务无可用采集账号时回退使用）
        "xy_collect_fallback_accounts": """
            CREATE TABLE IF NOT EXISTS xy_collect_fallback_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                category_id INTEGER DEFAULT NULL,
                account_ids TEXT DEFAULT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(owner_id, category_id)
            );
        """,

        # 46. 共享扫码登录会话表
        "xy_shared_scan_sessions": """
            CREATE TABLE IF NOT EXISTS xy_shared_scan_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id VARCHAR(36) NOT NULL UNIQUE,
                owner_id INTEGER NOT NULL,
                owner_username VARCHAR(120) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'active',
                expires_at DATETIME NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 48. 禁止发货规则配置表
        "xy_delivery_block_rules": """
            CREATE TABLE IF NOT EXISTS xy_delivery_block_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id VARCHAR(64) NOT NULL,
                rule_code VARCHAR(50) NOT NULL,
                enabled TINYINT(1) NOT NULL DEFAULT 0,
                priority INT NOT NULL DEFAULT 0,
                block_reason VARCHAR(500) DEFAULT NULL,
                auto_close_order TINYINT(1) NOT NULL DEFAULT 0,
                only_card_after_close TINYINT(1) NOT NULL DEFAULT 0,
                excluded_item_ids TEXT DEFAULT NULL,
                config TEXT DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(account_id, rule_code)
            );
        """,

        # 47. 共享扫码登录兼职工作者表
        "xy_shared_scan_workers": """
            CREATE TABLE IF NOT EXISTS xy_shared_scan_workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shared_session_id VARCHAR(36) NOT NULL,
                sub_session_id VARCHAR(36) NOT NULL UNIQUE,
                xianyu_session_id VARCHAR(36) DEFAULT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'qrcode_ready',
                qr_code_url LONGTEXT DEFAULT NULL,
                account_id VARCHAR(80) DEFAULT NULL,
                cookie_saved TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 49. 个人黑名单表
        "xy_personal_blacklist": """
            CREATE TABLE IF NOT EXISTS xy_personal_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                account_id VARCHAR(64) DEFAULT NULL,
                buyer_id VARCHAR(64) NOT NULL,
                buyer_nick VARCHAR(120) DEFAULT NULL,
                item_id VARCHAR(64) DEFAULT NULL,
                reason VARCHAR(500) DEFAULT NULL,
                is_enabled TINYINT(1) NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 50. 闲鱼黑名单表
        "xy_platform_blacklist": """
            CREATE TABLE IF NOT EXISTS xy_platform_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                buyer_id VARCHAR(64) NOT NULL,
                buyer_nick VARCHAR(120) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 51. 在线聊天快捷短语表
        "xy_chat_quick_phrases": """
            CREATE TABLE IF NOT EXISTS xy_chat_quick_phrases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                title VARCHAR(80) NOT NULL,
                content TEXT NOT NULL,
                sort_order INT NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 52. AI铺货配置表
        "xy_ai_listing_configs": """
            CREATE TABLE IF NOT EXISTS xy_ai_listing_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                name VARCHAR(80) NOT NULL,
                provider_type VARCHAR(30) NOT NULL DEFAULT 'openai_compatible',
                text_base_url VARCHAR(300) NOT NULL,
                text_api_key VARCHAR(500) NOT NULL,
                text_model VARCHAR(100) NOT NULL,
                text_temperature REAL NOT NULL DEFAULT 0.70,
                text_max_tokens INT NOT NULL DEFAULT 2048,
                prompt_template TEXT DEFAULT NULL,
                image_enabled TINYINT(1) NOT NULL DEFAULT 0,
                image_base_url VARCHAR(300) DEFAULT NULL,
                image_api_key VARCHAR(500) DEFAULT NULL,
                image_model VARCHAR(100) DEFAULT NULL,
                image_size VARCHAR(20) NOT NULL DEFAULT '1024x1024',
                image_count INT NOT NULL DEFAULT 1,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,

        # 53. AI铺货任务表
        "xy_ai_listing_tasks": """
            CREATE TABLE IF NOT EXISTS xy_ai_listing_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                task_id VARCHAR(36) NOT NULL,
                config_id INTEGER NOT NULL,
                config_name VARCHAR(80) DEFAULT NULL,
                keyword VARCHAR(200) NOT NULL,
                total_count INT NOT NULL DEFAULT 0,
                success_count INT NOT NULL DEFAULT 0,
                failed_count INT NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                error_message VARCHAR(1000) DEFAULT NULL,
                params TEXT DEFAULT NULL,
                started_at DATETIME DEFAULT NULL,
                finished_at DATETIME DEFAULT NULL,
                is_deleted TINYINT(1) NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(task_id)
            );
        """,

        # 54. AI铺货任务明细表
        "xy_ai_listing_task_items": """
            CREATE TABLE IF NOT EXISTS xy_ai_listing_task_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id VARCHAR(36) NOT NULL,
                owner_id INTEGER NOT NULL,
                seq INT NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                title VARCHAR(200) DEFAULT NULL,
                material_id INTEGER DEFAULT NULL,
                image_count INT NOT NULL DEFAULT 0,
                error_message VARCHAR(1000) DEFAULT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """,
    }
    
    # 字段迁移定义：表名 -> [(字段名, 字段定义, 在哪个字段后面)]
    COLUMN_MIGRATIONS = {
        "xy_keyword_rules": [
            ("location_name", "VARCHAR(255) DEFAULT NULL", "image_url"),
            ("location_longitude", "VARCHAR(32) DEFAULT NULL", "location_name"),
            ("location_latitude", "VARCHAR(32) DEFAULT NULL", "location_longitude"),
            ("location_title", "VARCHAR(128) DEFAULT NULL", "location_latitude"),
            ("location_subtitle", "VARCHAR(255) DEFAULT NULL", "location_title"),
        ],
        "xy_recharge_orders": [
            ("order_type", "VARCHAR(20) NOT NULL DEFAULT 'recharge'", "amount"),
        ],
        "xy_token_cache": [
            ("renew_expire_at", "DATETIME DEFAULT NULL", "expire_at"),
        ],
        "xy_product_materials": [
            ("platform_category_id", "VARCHAR(64) DEFAULT NULL", "category"),
            ("platform_category_name", "VARCHAR(100) DEFAULT NULL", "platform_category_id"),
            ("platform_channel_category_id", "VARCHAR(64) DEFAULT NULL", "platform_category_name"),
            ("platform_channel_category_name", "VARCHAR(100) DEFAULT NULL", "platform_channel_category_id"),
            ("platform_leaf_id", "VARCHAR(64) DEFAULT NULL", "platform_channel_category_name"),
            ("platform_tb_category_id", "VARCHAR(64) DEFAULT NULL", "platform_leaf_id"),
            ("platform_category_path", "TEXT DEFAULT NULL", "platform_tb_category_id"),
            ("platform_attributes", "TEXT DEFAULT NULL", "platform_category_path"),
            ("category_source", "VARCHAR(20) NOT NULL DEFAULT 'manual'", "platform_attributes"),
            ("category_confidence", "REAL DEFAULT NULL", "category_source"),
            ("videos", "TEXT DEFAULT NULL", "images"),
            ("specifications", "TEXT DEFAULT NULL", "videos"),
            ("sku_rows", "TEXT DEFAULT NULL", "specifications"),
            ("quantity", "INT NOT NULL DEFAULT 1", "sku_rows"),
            ("delivery_method", "VARCHAR(20) DEFAULT 'express'", "quantity"),
            ("shipping_method", "VARCHAR(20) DEFAULT 'free'", "delivery_method"),
            ("support_pickup", "TINYINT(1) NOT NULL DEFAULT 0", "shipping_method"),
            ("postage", "REAL DEFAULT 0", "support_pickup"),
            ("address_expected_text", "VARCHAR(200) DEFAULT NULL", "address"),
            ("is_deleted", "TINYINT(1) NOT NULL DEFAULT 0", "remark"),
        ],
        "xy_listing_monitor_tasks": [
            ("monitor_type", "VARCHAR(20) NOT NULL DEFAULT 'listing'", "owner_id"),
            ("category_id", "INTEGER DEFAULT NULL", "owner_id"),
            ("collect_pages", "INT NOT NULL DEFAULT 1", "interval_minutes"),
            ("dm_content", "VARCHAR(1000) DEFAULT NULL", "account_ids"),
            ("order_account_ids", "TEXT DEFAULT NULL", "account_ids"),
            ("dm_batch_size", "INT NOT NULL DEFAULT 5", "dm_content"),
            ("order_batch_size", "INT NOT NULL DEFAULT 5", "dm_batch_size"),
            ("publish_days", "INT DEFAULT NULL", "price_max"),
            ("proxy_url", "VARCHAR(255) DEFAULT NULL", "collect_pages"),
            ("direct_order", "TINYINT(1) NOT NULL DEFAULT 0", "order_batch_size"),
        ],
        "xy_listing_monitor_logs": [
            ("used_account_ids", "TEXT DEFAULT NULL", "account_id"),
            ("trigger_type", "VARCHAR(10) NOT NULL DEFAULT 'auto'", "keyword"),
        ],
        "xy_listing_monitor_items": [
            ("is_dm_sent", "TINYINT(1) NOT NULL DEFAULT 0", "raw_json"),
            ("is_ordered", "TINYINT(1) NOT NULL DEFAULT 0", "is_dm_sent"),
            ("seller_user_id", "VARCHAR(64) DEFAULT NULL", "seller_id"),
            ("detail_json", "MEDIUMTEXT DEFAULT NULL", "raw_json"),
            ("order_id", "VARCHAR(64) DEFAULT NULL", "is_ordered"),
            ("order_account_id", "VARCHAR(80) DEFAULT NULL", "order_id"),
            ("dm_status", "VARCHAR(20) DEFAULT NULL", "is_dm_sent"),
            ("dm_fail_reason", "VARCHAR(500) DEFAULT NULL", "dm_status"),
            ("dm_attempts", "INT NOT NULL DEFAULT 0", "dm_fail_reason"),
            ("order_status", "VARCHAR(20) DEFAULT NULL", "order_id"),
            ("order_fail_reason", "VARCHAR(500) DEFAULT NULL", "order_status"),
            ("order_attempts", "INT NOT NULL DEFAULT 0", "order_fail_reason"),
            ("dm_sent_at", "DATETIME DEFAULT NULL", "dm_attempts"),
            ("ordered_at", "DATETIME DEFAULT NULL", "order_attempts"),
            ("dm_account_id", "VARCHAR(80) DEFAULT NULL", "is_dm_sent"),
            ("dm_chat_id", "VARCHAR(80) DEFAULT NULL", "dm_account_id"),
            ("seller_fill_status", "VARCHAR(20) DEFAULT NULL", "detail_json"),
            ("seller_fill_fail_reason", "VARCHAR(500) DEFAULT NULL", "seller_fill_status"),
            ("seller_avatar", "VARCHAR(1000) DEFAULT NULL", "seller_nick"),
            ("tags", "VARCHAR(500) DEFAULT NULL", "want_count"),
        ],
        "xy_order_fallback_accounts": [
            ("category_id", "INTEGER DEFAULT NULL", "owner_id"),
            ("is_deleted", "TINYINT(1) NOT NULL DEFAULT 0", "account_ids"),
        ],
        "xy_collect_fallback_accounts": [
            ("category_id", "INTEGER DEFAULT NULL", "owner_id"),
            ("is_deleted", "TINYINT(1) NOT NULL DEFAULT 0", "account_ids"),
        ],
        "xy_auto_reply_message_logs": [
            ("send_status", "VARCHAR(20) NOT NULL DEFAULT 'unknown'", "error_message"),
            ("send_fail_reason", "TEXT", "send_status"),
            ("order_no", "VARCHAR(64) DEFAULT NULL", "item_title"),
        ],
        "xy_risk_control_logs": [
            ("captcha_engine", "VARCHAR(32) DEFAULT NULL", "processing_status"),
            ("call_type", "VARCHAR(16) DEFAULT 'local'", "captcha_engine"),
            ("call_user", "VARCHAR(128) DEFAULT NULL", "call_type"),
        ],
        "xy_accounts": [
            ("proxy_type", "VARCHAR(20) DEFAULT 'none'", "last_refresh_at"),
            ("proxy_host", "VARCHAR(255)", "proxy_type"),
            ("proxy_port", "INT", "proxy_host"),
            ("proxy_user", "VARCHAR(120)", "proxy_port"),
            ("proxy_pass", "VARCHAR(255)", "proxy_user"),
            ("message_expire_time", "INT DEFAULT 3600", "proxy_pass"),
            ("reply_delay_seconds", "INT DEFAULT 0", "message_expire_time"),
            ("disable_reason", "VARCHAR(255)", "message_expire_time"),
            ("scheduled_redelivery", "TINYINT(1) NOT NULL DEFAULT 0", "disable_reason"),
            ("scheduled_rate", "TINYINT(1) NOT NULL DEFAULT 0", "scheduled_redelivery"),
            ("auto_polish", "TINYINT(1) NOT NULL DEFAULT 0", "scheduled_rate"),
            ("confirm_before_send", "TINYINT(1) NOT NULL DEFAULT 0", "auto_polish"),
            ("send_before_confirm", "TINYINT(1) NOT NULL DEFAULT 0", "confirm_before_send"),
            ("only_send_card", "TINYINT(1) NOT NULL DEFAULT 0", "send_before_confirm"),
            ("auto_red_flower", "TINYINT(1) NOT NULL DEFAULT 0", "send_before_confirm"),
            ("delivery_disabled", "TINYINT(1) NOT NULL DEFAULT 0", "auto_red_flower"),
            ("delivery_disabled_reason", "VARCHAR(500) DEFAULT NULL", "delivery_disabled"),
            ("auto_close_order", "TINYINT(1) NOT NULL DEFAULT 0", "delivery_disabled_reason"),
            ("delivery_only_card_after_close", "TINYINT(1) NOT NULL DEFAULT 0", "auto_close_order"),
            ("delivery_disabled_excluded_items", "TEXT DEFAULT NULL", "delivery_only_card_after_close"),
            ("ai_reply_block_ordered_users", "TINYINT(1) NOT NULL DEFAULT 0", "delivery_disabled_excluded_items"),
            ("refund_cancel_enabled", "TINYINT(1) NOT NULL DEFAULT 0", "ai_reply_block_ordered_users"),
            ("refund_cancel_url", "VARCHAR(255) DEFAULT NULL", "refund_cancel_enabled"),
            ("refund_cancel_timeout", "INT DEFAULT 60", "refund_cancel_url"),
            ("agree_deliver_enabled", "TINYINT(1) NOT NULL DEFAULT 0", "refund_cancel_timeout"),
            ("agree_deliver_notify_message", "VARCHAR(2000) DEFAULT NULL", "agree_deliver_enabled"),
            ("agree_deliver_pickup_url", "VARCHAR(255) DEFAULT NULL", "agree_deliver_notify_message"),
        ],
        "xy_orders": [
            ("card_only_delivered", "TINYINT(1) NOT NULL DEFAULT 0", "delivery_fail_reason"),
            ("is_bargain", "TINYINT(1) DEFAULT 0", "account_name"),
            ("chat_id", "VARCHAR(64)", "buyer_id"),
            ("buyer_fish_nick", "VARCHAR(120)", "buyer_nick"),
            ("receiver_name", "VARCHAR(120)", "is_bargain"),
            ("receiver_phone", "VARCHAR(32)", "receiver_name"),
            ("receiver_address", "VARCHAR(512)", "receiver_phone"),
            ("is_rated", "TINYINT(1) DEFAULT 0", "receiver_address"),
            ("delivery_method", "VARCHAR(32)", "is_rated"),
            ("delivery_content", "VARCHAR(2000)", "delivery_method"),
            ("delivery_fail_reason", "VARCHAR(2000)", "delivery_content"),
            ("source", "VARCHAR(32)", "metadata"),
            ("is_red_flower", "TINYINT(1) DEFAULT 0", "is_rated"),
            ("is_thanks_sent", "TINYINT(1) DEFAULT 0", "is_red_flower"),
            ("is_unregistered", "TINYINT(1) DEFAULT 0", "is_red_flower"),
            ("unregister_error_reason", "VARCHAR(500) DEFAULT NULL", "is_unregistered"),
            ("agree_deliver_agreed", "TINYINT(1) NOT NULL DEFAULT 0", "card_only_delivered"),
            ("agree_deliver_agreed_at", "DATETIME DEFAULT NULL", "agree_deliver_agreed"),
        ],
        "xy_auto_rate_configs": [
            ("thanks_enabled", "TINYINT(1) DEFAULT 0", "api_url"),
            ("thanks_content", "TEXT", "thanks_enabled"),
        ],
        "xy_cards": [
            ("delivery_count", "INT DEFAULT 0", "delay_seconds"),
            ("use_no_logistics_form", "TINYINT(1) NOT NULL DEFAULT 0", "delay_seconds"),
            ("price", "VARCHAR(32)", "delivery_count"),
            ("is_dockable", "TINYINT(1) DEFAULT 0", "price"),
            ("image_urls", "TEXT", "image_url"),
            ("fee_payer", "VARCHAR(32)", "is_dockable"),
            ("min_price", "VARCHAR(32)", "fee_payer"),
            ("dock_visibility", "VARCHAR(32) DEFAULT NULL", "min_price"),
        ],
        "xy_dock_records": [
            ("delivery_count", "INT NOT NULL DEFAULT 0", "remark"),
            ("disable_reason", "VARCHAR(255) DEFAULT NULL", "status"),
            ("owner_disabled", "TINYINT(1) NOT NULL DEFAULT 0", "disable_reason"),
            ("level", "INT NOT NULL DEFAULT 1", "disable_reason"),
            ("parent_dock_id", "INTEGER DEFAULT NULL", "level"),
            ("source_user_id", "INTEGER DEFAULT NULL", "parent_dock_id"),
            ("allow_sub_dock", "TINYINT(1) DEFAULT 0", "source_user_id"),
            ("sub_dock_price", "VARCHAR(32) DEFAULT NULL", "allow_sub_dock"),
            ("sub_dock_visibility", "VARCHAR(32) DEFAULT NULL", "sub_dock_price"),
        ],
        "xy_users": [
            ("account_limit", "INT DEFAULT NULL", "role"),
            ("login_fail_count", "INT DEFAULT 0", "last_login_at"),
            ("login_locked_until", "DATETIME", "login_fail_count"),
            ("dock_code", "VARCHAR(32) DEFAULT NULL UNIQUE", "login_locked_until"),
            ("secret_key", "VARCHAR(64) DEFAULT NULL UNIQUE", "dock_code"),
            ("expire_at", "DATETIME DEFAULT NULL", "secret_key"),
        ],
        "xy_default_replies": [
            ("item_id", "VARCHAR(64) DEFAULT NULL", "account_id"),
            ("reply_image", "VARCHAR(512)", "reply_content"),
            ("reply_type", "VARCHAR(32) DEFAULT 'text'", "enabled"),
            ("api_url", "VARCHAR(1024) DEFAULT NULL", "reply_image"),
            ("api_timeout", "INT DEFAULT 80", "api_url"),
            ("location_name", "VARCHAR(255) DEFAULT NULL", "api_timeout"),
            ("location_longitude", "VARCHAR(32) DEFAULT NULL", "location_name"),
            ("location_latitude", "VARCHAR(32) DEFAULT NULL", "location_longitude"),
            ("location_title", "VARCHAR(128) DEFAULT NULL", "location_latitude"),
            ("location_subtitle", "VARCHAR(255) DEFAULT NULL", "location_title"),
        ],
        "xy_default_reply_records": [
            ("item_id", "VARCHAR(64) DEFAULT NULL", "account_id"),
        ],
        "xy_catalog_items": [
            ("ai_prompt", "TEXT", "price"),
            ("is_polished", "TINYINT(1) DEFAULT 0", "ai_prompt"),
            ("updated_at", "DATETIME", "created_at"),
        ],
        "xy_announcements": [
            ("is_deleted", "TINYINT(1) NOT NULL DEFAULT 0", "content"),
        ],
        "xy_card_item_relations": [
            ("source", "VARCHAR(20) DEFAULT 'own'", "item_id"),
            ("dock_record_id", "INTEGER DEFAULT NULL", "source"),
        ],
        "xy_agent_orders": [
            ("card_price", "VARCHAR(32) DEFAULT NULL", "dock_price"),
            ("level2_cost", "VARCHAR(32) DEFAULT NULL", "card_price"),
            ("fee_payer", "VARCHAR(32) DEFAULT NULL", "fee_amount"),
            ("owner_user_id", "INTEGER DEFAULT NULL", "upstream_dock_record_id"),
        ],
        "xy_advertisements": [
            ("months", "INT", "ad_type"),
            ("total_amount", "VARCHAR(32)", "months"),
        ],
        "xy_settlement_records": [
            ("payment_type", "VARCHAR(16)", "alipay_id"),
            ("payment_qrcode", "VARCHAR(512)", "payment_type"),
            ("reject_reason", "VARCHAR(512)", "remark"),
        ],
        "xy_publish_logs": [
            ("resolved_address_id", "INTEGER DEFAULT NULL", "error_message"),
            ("resolved_address_text", "VARCHAR(200) DEFAULT NULL", "resolved_address_id"),
            ("address_source", "VARCHAR(20) DEFAULT NULL", "resolved_address_text"),
        ],
        "xy_account_login_logs": [
            ("updated_cookie_names", "VARCHAR(500) DEFAULT NULL", "error_message"),
        ],
    }

    async def init_all(self):
        """初始化所有数据库内容"""
        try:
            logger.info("=" * 50)
            logger.info("开始初始化数据库...")

            # 命名锁保证多服务同时启动时只有一个进程在建表/改表，避免元数据锁互相阻塞
            async with init_advisory_lock():
                # 使用上下文管理器抑制初始化时的重复警告日志
                with suppress_db_warnings():
                    # 1. 创建所有表
                    await self.create_all_tables()

                    # 1.1 初始化所有枚举数据字典
                    await self.init_data_dictionaries()

                    # 自动续售表和发布日志关联字段独立幂等迁移，避免依赖旧版本 DDL 顺序。
                    async with ddl_connection() as conn:
                        await ensure_auto_relist_schema(conn, get_beijing_now_naive())

                    # 2. 创建默认管理员用户
                    await self.create_default_admin()

                    # 3. 初始化系统设置
                    await self.init_system_settings()

                    # 4. 初始化定时任务配置
                    await self.init_scheduled_tasks()

                    # 5. 初始化随机地址默认数据
                    await self.init_publish_addresses()

                    # 6. 初始化Redis平台日
                    await self.init_redis_platform_day()

                    # 7. 迁移卡券商品关联数据（从 xy_cards.item_id 到关联表）
                    await self.migrate_card_item_relations()

                    # 8. 迁移旧禁止发货设置到规则配置表
                    await self.migrate_delivery_block_rules()

                    # 9. 为历史用户回填分销秘钥（secret_key 为空的存量用户）
                    await self.backfill_user_secret_keys()

            logger.info("数据库初始化完成")
            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    # 旧表名 → 新表名 的重命名映射（统一加 xy_ 前缀）
    TABLES_TO_RENAME = {
        "scheduled_redelivery_log": "xy_scheduled_redelivery_log",
        "scheduled_rate_log": "xy_scheduled_rate_log",
        "scheduled_polish_log": "xy_scheduled_polish_log",
    }

    async def create_all_tables(self):
        """创建所有数据表"""
        logger.info("开始创建数据表...")
        
        # 先重命名旧表（如果存在）
        await self.rename_legacy_tables()
        
        async with ddl_connection() as conn:
            for table_name, ddl in self.TABLES_DDL.items():
                try:
                    await conn.execute(text(ddl))
                    logger.info(f"✓ 表 {table_name} 已就绪")
                except Exception as e:
                    logger.warning(f"✗ 表 {table_name} 创建失败: {describe_ddl_error(e)}")
        
        logger.info(f"数据表创建完成，共 {len(self.TABLES_DDL)} 张表")
        
        # 执行字段迁移
        await self.migrate_columns()
        
        # 执行索引迁移
        await self.migrate_indexes()
    
    async def init_data_dictionaries(self):
        """幂等初始化数据字典中的枚举值。"""
        items = (
            # 邮件触发操作
            ("email_trigger_operation", "register_verification_code", "注册验证码", 10),
            ("email_trigger_operation", "reset_password_verification_code", "找回密码验证码", 20),
            ("email_trigger_operation", "email_login_verification_code", "邮箱验证码登录", 30),
            ("email_trigger_operation", "test_email", "测试邮件", 40),
            ("email_trigger_operation", "withdraw_notification", "提现通知邮件", 50),
            ("email_trigger_operation", "notification_channel_test", "通知渠道测试邮件", 60),
            # 邮件发送结果
            ("email_send_result", "success", "成功", 10),
            ("email_send_result", "failure", "失败", 20)
        )
        async with ddl_connection() as conn:
            for dict_type, dict_value, dict_label, sort_order in items:
                await conn.execute(
                    text(
                        "INSERT OR IGNORE INTO xy_data_dictionaries "
                        "(dict_type, dict_value, dict_label, sort_order, enabled) "
                        "VALUES (:dict_type, :dict_value, :dict_label, :sort_order, 1)"
                    ),
                    {
                        "dict_type": dict_type,
                        "dict_value": dict_value,
                        "dict_label": dict_label,
                        "sort_order": sort_order,
                    }
                )
            await conn.execute(
                text(
                    "UPDATE xy_data_dictionaries SET enabled = 0 "
                    "WHERE dict_type = :dict_type "
                    "AND dict_value IN (:notification_email, :auto_reply_notification)"
                ),
                {
                    "dict_type": "email_trigger_operation",
                    "notification_email": "notification_email",
                    "auto_reply_notification": "auto_reply_notification",
                }
            )
        logger.info(f"数据字典初始化完成，共 {len(items)} 项")

    async def rename_legacy_tables(self):
        # SQLite: 全新库无旧表需重命名
        return
        """重命名旧表（统一加 xy_ 前缀）"""
        async with ddl_connection() as conn:
            for old_name, new_name in self.TABLES_TO_RENAME.items():
                try:
                    # 检查旧表是否存在
                    check_sql = text(f"""
                        SELECT COUNT(*) FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = '{old_name}'
                    """)
                    result = await conn.execute(check_sql)
                    old_exists = result.scalar() > 0
                    if not old_exists:
                        continue
                    
                    # 检查新表是否已存在
                    check_new_sql = text(f"""
                        SELECT COUNT(*) FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = '{new_name}'
                    """)
                    result = await conn.execute(check_new_sql)
                    new_exists = result.scalar() > 0
                    if new_exists:
                        logger.debug(f"✓ 新表 {new_name} 已存在，跳过重命名")
                        continue
                    
                    # 执行重命名
                    rename_sql = text(f"RENAME TABLE {old_name} TO {new_name}")
                    await conn.execute(rename_sql)
                    logger.info(f"✓ 表 {old_name} 已重命名为 {new_name}")
                except Exception as e:
                    logger.warning(f"✗ 重命名表 {old_name} → {new_name} 失败: {e}")

    async def migrate_columns(self):
        # SQLite: 全新库 DDL 已是最新结构, 跳过基于 information_schema 的历史列迁移
        return

        """检查并添加/修改字段"""
        logger.info("检查字段迁移...")
        
        async with ddl_connection() as conn:
            for table_name, columns in self.COLUMN_MIGRATIONS.items():
                for col_name, col_def, after_col in columns:
                    try:
                        # 检查字段是否存在
                        check_sql = text(f"""
                            SELECT COUNT(*) FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = DATABASE() 
                            AND TABLE_NAME = '{table_name}' 
                            AND COLUMN_NAME = '{col_name}'
                        """)
                        result = await conn.execute(check_sql)
                        exists = result.scalar() > 0

                        # Existing installations may have the original VARCHAR(16) reply type.
                        # Expand it before storing external_contact.
                        if exists and table_name == "xy_default_replies" and col_name == "reply_type":
                            length_result = await conn.execute(text("""
                                SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
                                WHERE TABLE_SCHEMA = DATABASE()
                                AND TABLE_NAME = 'xy_default_replies'
                                AND COLUMN_NAME = 'reply_type'
                            """))
                            current_length = length_result.scalar()
                            if current_length and current_length < 32:
                                await conn.execute(text(
                                    "ALTER TABLE xy_default_replies MODIFY COLUMN reply_type VARCHAR(32) DEFAULT 'text'"
                                ))
                                logger.info("Expanded xy_default_replies.reply_type to VARCHAR(32)")
                        
                        if not exists:
                            # 添加字段
                            try:
                                alter_sql = text(
                                    f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def} AFTER {after_col}"
                                )
                                await conn.execute(alter_sql)
                            except Exception:
                                # AFTER 失败则追加到表末尾
                                alter_sql = text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")
                                await conn.execute(alter_sql)
                            logger.info(f"✓ 表 {table_name} 添加字段 {col_name}")
                        else:
                            logger.debug(f"✓ 表 {table_name} 已有字段 {col_name}")
                    except Exception as e:
                        logger.warning(
                            f"✗ 表 {table_name} 字段 {col_name} 迁移失败: {describe_ddl_error(e)}"
                        )

            # xy_users: account_limit 字段允许为空且默认值为空
            try:
                check_account_limit = text("""
                    SELECT IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_users'
                    AND COLUMN_NAME = 'account_limit'
                """)
                result = await conn.execute(check_account_limit)
                account_limit_row = result.fetchone()
                if account_limit_row and (
                    account_limit_row.IS_NULLABLE != 'YES'
                    or account_limit_row.COLUMN_DEFAULT is not None
                ):
                    await conn.execute(text(
                        "ALTER TABLE xy_users MODIFY COLUMN account_limit INT DEFAULT NULL"
                    ))
                    logger.info("✓ xy_users: account_limit 字段已调整为允许为空且默认值为空")
            except Exception as e:
                logger.warning(f"✗ xy_users account_limit 字段迁移失败: {e}")

            # xy_advertisements: 将 status 枚举扩展为包含 'unpaid'
            try:
                check_enum = text("""
                    SELECT COLUMN_TYPE FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_advertisements'
                    AND COLUMN_NAME = 'status'
                """)
                result = await conn.execute(check_enum)
                col_type = result.scalar()
                if col_type and 'unpaid' not in col_type:
                    await conn.execute(text(
                        "ALTER TABLE xy_advertisements MODIFY COLUMN status TEXT DEFAULT 'unpaid'"
                    ))
                    logger.info("✓ xy_advertisements: status 枚举已扩展（新增 unpaid）")
            except Exception as e:
                logger.warning(f"✗ xy_advertisements status 枚举迁移失败: {e}")

            # xy_token_cache: 将 user_id 字段从 VARCHAR(64) 扩展到 VARCHAR(128)
            try:
                check_len = text("""
                    SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_token_cache'
                    AND COLUMN_NAME = 'user_id'
                """)
                result = await conn.execute(check_len)
                max_len = result.scalar()
                if max_len and max_len < 128:
                    await conn.execute(text(
                        "ALTER TABLE xy_token_cache MODIFY COLUMN user_id VARCHAR(128) NOT NULL"
                    ))
                    logger.info("✓ xy_token_cache: user_id 字段长度已扩展为 VARCHAR(128)")
            except Exception as e:
                logger.warning(f"✗ xy_token_cache user_id 字段迁移失败: {e}")

            # xy_scheduled_api_cookie_renew_log: status 字段长度扩展（20→30，支持 browser_renewed/need_password_login）
            try:
                check_status_len = text("""
                    SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_api_cookie_renew_log'
                    AND COLUMN_NAME = 'status'
                """)
                result = await conn.execute(check_status_len)
                status_len = result.scalar()
                if status_len and status_len < 30:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_api_cookie_renew_log MODIFY COLUMN "
                        "status VARCHAR(30) NOT NULL "
                        "COMMENT '状态：success/cookie_updated/browser_renewed/need_password_login/failed'"
                    ))
                    logger.info("✓ xy_scheduled_api_cookie_renew_log: status 字段长度已扩展为 VARCHAR(30)")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_api_cookie_renew_log status 字段迁移失败: {e}")

            # xy_cards: 将 text_content / data_content 从 TEXT 升级为 LONGTEXT（支持超大卡券内容）
            for card_col in ("text_content", "data_content"):
                try:
                    check_card_col = text("""
                        SELECT DATA_TYPE FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_cards'
                        AND COLUMN_NAME = :col_name
                    """)
                    result = await conn.execute(check_card_col, {"col_name": card_col})
                    data_type = result.scalar()
                    # DATA_TYPE 返回小写类型名（如 text/longtext），非 longtext 时升级
                    if data_type and data_type.lower() != 'longtext':
                        await conn.execute(text(
                            f"ALTER TABLE xy_cards MODIFY COLUMN {card_col} LONGTEXT NULL"
                        ))
                        logger.info(f"✓ xy_cards: {card_col} 字段已升级为 LONGTEXT")
                except Exception as e:
                    logger.warning(f"✗ xy_cards {card_col} 字段迁移失败: {e}")

            # 归一化历史冲突配置：只发卡券模式优先于自动确认及确认顺序设置。
            try:
                result = await conn.execute(text("""
                    UPDATE xy_accounts
                    SET auto_confirm = 0,
                        confirm_before_send = 0,
                        send_before_confirm = 0
                    WHERE only_send_card = 1
                      AND (
                          auto_confirm <> 0
                          OR confirm_before_send <> 0
                          OR send_before_confirm <> 0
                      )
                """))
                if result.rowcount:
                    logger.info(f"✓ xy_accounts: 已修复 {result.rowcount} 条只发卡券冲突配置")
            except Exception as e:
                logger.warning(f"✗ xy_accounts 发货开关冲突配置修复失败: {e}")

    async def migrate_indexes(self):
        # SQLite: 索引已在建表DDL中处理, 跳过基于 information_schema 的幂等迁移
        return
        """检查并迁移索引（如更新 UNIQUE KEY 等）"""
        logger.info("检查索引迁移...")
        
        async with ddl_connection() as conn:
            try:
                # xy_card_item_relations: 将旧的 uk_card_item(card_id, item_id) 替换为 uk_card_item_dock(card_id, item_id, dock_record_id)
                # 检查旧索引是否存在
                check_old = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_card_item_relations'
                    AND INDEX_NAME = 'uk_card_item'
                """)
                result = await conn.execute(check_old)
                old_exists = result.scalar() > 0
                
                if old_exists:
                    # 先修复历史数据：确保 source 字段有值
                    await conn.execute(text(
                        "UPDATE xy_card_item_relations SET source = 'own' WHERE source IS NULL"
                    ))
                    # 删除旧索引并创建新索引
                    pass  # SQLite: 索引随建表生成, 无需迁移
                    logger.info("✓ xy_card_item_relations: uk_card_item → uk_card_item_dock 迁移完成（历史数据已修复）")
                else:
                    # 检查新索引是否已存在
                    check_new = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_card_item_relations'
                        AND INDEX_NAME = 'uk_card_item_dock'
                    """)
                    result = await conn.execute(check_new)
                    new_exists = result.scalar() > 0
                    if new_exists:
                        logger.debug("✓ xy_card_item_relations: uk_card_item_dock 索引已存在")
                    else:
                        # 新表没有任何唯一索引，创建新的
                        pass  # SQLite: 索引随建表生成, 无需迁移
            except Exception as e:
                logger.warning(f"✗ 索引迁移失败: {e}")

            # 为 xy_token_cache 补建到期时间复合索引，降低 20 秒续期扫描开销
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_token_cache'
                    AND INDEX_NAME = 'idx_token_cache_expiries'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_token_cache "
                        "ADD"
                    ))
                    logger.info("✓ xy_token_cache: 创建 idx_token_cache_expiries 索引")
            except Exception as e:
                logger.warning(f"✗ xy_token_cache idx_token_cache_expiries 创建失败: {e}")

            # 为 xy_users 补建 created_at 索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_users'
                    AND INDEX_NAME = 'idx_user_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_users ADD"
                    ))
                    logger.info("✓ xy_users: 创建 idx_user_created 索引")
            except Exception as e:
                logger.warning(f"✗ xy_users idx_user_created 创建失败: {e}")

            # 为 xy_accounts 补建 created_at 索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_accounts'
                    AND INDEX_NAME = 'idx_account_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_accounts ADD"
                    ))
                    logger.info("✓ xy_accounts: 创建 idx_account_created 索引")
            except Exception as e:
                logger.warning(f"✗ xy_accounts idx_account_created 创建失败: {e}")

            # 为 xy_keyword_rules 补建 (account_id, item_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_keyword_rules'
                    AND INDEX_NAME = 'idx_kw_account_item'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_keyword_rules ADD"
                    ))
                    logger.info("✓ xy_keyword_rules: 创建 idx_kw_account_item 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_keyword_rules 复合索引创建失败: {e}")

            # 为 xy_keyword_rules 补建 (account_id, is_active) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_keyword_rules'
                    AND INDEX_NAME = 'idx_kw_account_active'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_keyword_rules ADD"
                    ))
                    logger.info("✓ xy_keyword_rules: 创建 idx_kw_account_active 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_keyword_rules idx_kw_account_active 创建失败: {e}")

            # 为风控日志补建 (processing_status, event_type) 复合索引，
            # 加速远程调用前统计全部处理中滑块日志。
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_risk_control_logs'
                    AND INDEX_NAME = 'idx_rcl_status_event'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_risk_control_logs "
                        "ADD"
                    ))
                    logger.info("✓ xy_risk_control_logs: 创建 idx_rcl_status_event 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_risk_control_logs idx_rcl_status_event 创建失败: {e}")

            # 为 xy_catalog_items 补建 (account_id, item_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_catalog_items'
                    AND INDEX_NAME = 'idx_cat_account_item'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_catalog_items ADD"
                    ))
                    logger.info("✓ xy_catalog_items: 创建 idx_cat_account_item 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_catalog_items 复合索引创建失败: {e}")

            # 为 xy_cards 补建 (user_id, item_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_cards'
                    AND INDEX_NAME = 'idx_card_user_item'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_cards ADD"
                    ))
                    logger.info("✓ xy_cards: 创建 idx_card_user_item 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_cards 复合索引创建失败: {e}")

            # 为 xy_dock_records 补建 parent_dock_id 索引和 (user_id, level) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_dock_records'
                    AND INDEX_NAME = 'idx_parent_dock_id'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_dock_records ADD"
                    ))
                    logger.info("✓ xy_dock_records: 创建 idx_parent_dock_id 索引")
            except Exception as e:
                logger.warning(f"✗ xy_dock_records idx_parent_dock_id 创建失败: {e}")

            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_dock_records'
                    AND INDEX_NAME = 'idx_dock_user_level'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_dock_records ADD"
                    ))
                    logger.info("✓ xy_dock_records: 创建 idx_dock_user_level 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_dock_records idx_dock_user_level 创建失败: {e}")

            # 为 xy_catalog_items 补建 (owner_id, created_at) 复合索引 —— 加速商品管理列表分页（owner_id 过滤 + created_at 倒序）
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_catalog_items'
                    AND INDEX_NAME = 'idx_cat_owner_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_catalog_items ADD"
                    ))
                    logger.info("✓ xy_catalog_items: 创建 idx_cat_owner_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_catalog_items idx_cat_owner_created 创建失败: {e}")

            # 为 xy_catalog_items 补建 (account_id, item_id) 唯一约束 —— 防止「定时获取闲鱼商品任务」
            # 与「商品管理页手动触发同步」两个流程并发 upsert 时重复插入同一商品（兜底）。
            # 注意：项目硬性规范禁止删除数据，存在历史重复数据时不自动清理，
            # 仅打印警告并跳过创建，待人工合并后下次启动自检再补建。
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_catalog_items'
                    AND INDEX_NAME = 'uk_cat_account_item'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    # 先检测是否存在 (account_id, item_id) 重复数据
                    dup_check = text("""
                        SELECT COUNT(*) FROM (
                            SELECT account_id, item_id
                            FROM xy_catalog_items
                            GROUP BY account_id, item_id
                            HAVING COUNT(*) > 1
                        ) AS dup
                    """)
                    dup_result = await conn.execute(dup_check)
                    dup_groups = dup_result.scalar() or 0
                    if dup_groups > 0:
                        logger.warning(
                            f"✗ xy_catalog_items 存在 {dup_groups} 组 (account_id, item_id) 重复数据，"
                            f"为遵守禁止删除数据规范，暂不创建 uk_cat_account_item 唯一约束。"
                            f"请人工合并重复商品后，重启服务自动补建（当前由 Redis 账号锁兜底防并发）"
                        )
                    else:
                        pass  # SQLite: 索引随建表生成, 无需迁移
            except Exception as e:
                logger.warning(f"✗ xy_catalog_items uk_cat_account_item 创建失败: {e}")

            # 为 xy_card_item_relations 补建 (user_id, item_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_card_item_relations'
                    AND INDEX_NAME = 'idx_cir_user_item'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_card_item_relations ADD"
                    ))
                    logger.info("✓ xy_card_item_relations: 创建 idx_cir_user_item 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_card_item_relations idx_cir_user_item 创建失败: {e}")

            # 为 xy_card_item_relations 补建 (item_id, card_id) 复合索引 —— 加速关联卡券弹窗 JOIN 查询
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_card_item_relations'
                    AND INDEX_NAME = 'idx_cir_item_card'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_card_item_relations ADD"
                    ))
                    logger.info("✓ xy_card_item_relations: 创建 idx_cir_item_card 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_card_item_relations idx_cir_item_card 创建失败: {e}")

            # 为 xy_cards 补建 (user_id, id) 复合索引 —— 加速按用户分页查询（ORDER BY id DESC）
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_cards'
                    AND INDEX_NAME = 'idx_cards_user_id_desc'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_cards ADD"
                    ))
                    logger.info("✓ xy_cards: 创建 idx_cards_user_id_desc 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_cards idx_cards_user_id_desc 创建失败: {e}")

            # 为 xy_cards 补建 (user_id, enabled) 复合索引 —— 加速发货匹配时过滤启用卡券
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_cards'
                    AND INDEX_NAME = 'idx_cards_user_enabled'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_cards ADD"
                    ))
                    logger.info("✓ xy_cards: 创建 idx_cards_user_enabled 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_cards idx_cards_user_enabled 创建失败: {e}")

            # 为 xy_agent_orders 补建 (upstream_user_id, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_agent_orders'
                    AND INDEX_NAME = 'idx_ao_upstream_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_agent_orders ADD"
                    ))
                    logger.info("✓ xy_agent_orders: 创建 idx_ao_upstream_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_agent_orders 复合索引创建失败: {e}")

            # 为 xy_risk_control_logs 补建 (account_id, processing_status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_risk_control_logs'
                    AND INDEX_NAME = 'idx_rcl_account_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_risk_control_logs ADD"
                    ))
                    logger.info("✓ xy_risk_control_logs: 创建 idx_rcl_account_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_risk_control_logs 复合索引创建失败: {e}")

            # 为 xy_risk_control_logs 补建 (account_identifier, processing_status, created_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_risk_control_logs'
                    AND INDEX_NAME = 'idx_rcl_identifier_status_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_risk_control_logs ADD"
                    ))
                    logger.info("✓ xy_risk_control_logs: 创建 idx_rcl_identifier_status_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_risk_control_logs idx_rcl_identifier_status_created 创建失败: {e}")

            # 为 xy_scheduled_redelivery_log 补建 (created_at, batch_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_redelivery_log'
                    AND INDEX_NAME = 'idx_srl_created_batch'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_redelivery_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_redelivery_log: 创建 idx_srl_created_batch 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_redelivery_log idx_srl_created_batch 创建失败: {e}")

            # 为 xy_scheduled_redelivery_log 补建 (batch_id, created_at, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_redelivery_log'
                    AND INDEX_NAME = 'idx_srl_batch_created_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_redelivery_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_redelivery_log: 创建 idx_srl_batch_created_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_redelivery_log idx_srl_batch_created_status 创建失败: {e}")

            # 为 xy_scheduled_rate_log 补建 (created_at, batch_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_rate_log'
                    AND INDEX_NAME = 'idx_srate_created_batch'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_rate_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_rate_log: 创建 idx_srate_created_batch 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_rate_log idx_srate_created_batch 创建失败: {e}")

            # 为 xy_scheduled_rate_log 补建 (batch_id, created_at, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_rate_log'
                    AND INDEX_NAME = 'idx_srate_batch_created_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_rate_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_rate_log: 创建 idx_srate_batch_created_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_rate_log idx_srate_batch_created_status 创建失败: {e}")

            # 为 xy_scheduled_polish_log 补建 (created_at, batch_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_polish_log'
                    AND INDEX_NAME = 'idx_spol_created_batch'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_polish_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_polish_log: 创建 idx_spol_created_batch 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_polish_log idx_spol_created_batch 创建失败: {e}")

            # 为 xy_scheduled_polish_log 补建 (batch_id, created_at, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_polish_log'
                    AND INDEX_NAME = 'idx_spol_batch_created_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_polish_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_polish_log: 创建 idx_spol_batch_created_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_polish_log idx_spol_batch_created_status 创建失败: {e}")

            # 为 xy_scheduled_red_flower_log 补建 (created_at, batch_id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_red_flower_log'
                    AND INDEX_NAME = 'idx_srf_created_batch'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_red_flower_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_red_flower_log: 创建 idx_srf_created_batch 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_red_flower_log idx_srf_created_batch 创建失败: {e}")

            # 为 xy_scheduled_red_flower_log 补建 (batch_id, created_at, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_scheduled_red_flower_log'
                    AND INDEX_NAME = 'idx_srf_batch_created_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_scheduled_red_flower_log ADD"
                    ))
                    logger.info("✓ xy_scheduled_red_flower_log: 创建 idx_srf_batch_created_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_scheduled_red_flower_log idx_srf_batch_created_status 创建失败: {e}")

            # 为 xy_fund_flows 补建 (user_id, id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_fund_flows'
                    AND INDEX_NAME = 'idx_ff_user_id_desc'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_fund_flows ADD"
                    ))
                    logger.info("✓ xy_fund_flows: 创建 idx_ff_user_id_desc 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_fund_flows idx_ff_user_id_desc 创建失败: {e}")

            # 为 xy_fund_flows 补建 (user_id, type, id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_fund_flows'
                    AND INDEX_NAME = 'idx_ff_user_type_id_desc'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_fund_flows ADD"
                    ))
                    logger.info("✓ xy_fund_flows: 创建 idx_ff_user_type_id_desc 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_fund_flows idx_ff_user_type_id_desc 创建失败: {e}")

            # 为 xy_settlement_records 补建 (user_id, created_at, id) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_settlement_records'
                    AND INDEX_NAME = 'idx_sr_user_created_id'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_settlement_records ADD"
                    ))
                    logger.info("✓ xy_settlement_records: 创建 idx_sr_user_created_id 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_settlement_records idx_sr_user_created_id 创建失败: {e}")

            # 为 xy_orders 补建 (owner_id, account_id, placed_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_owner_account_placed'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_owner_account_placed 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_owner_account_placed 创建失败: {e}")

            # 为 xy_orders 补建 (owner_id, placed_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_owner_placed'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_owner_placed 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_owner_placed 创建失败: {e}")

            # 为 xy_orders 补建 (owner_id, account_id, buyer_id, created_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_owner_account_buyer_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_owner_account_buyer_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_owner_account_buyer_created 创建失败: {e}")

            # 为 xy_orders 补建 created_at 索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_created_at'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_created_at 索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_created_at 创建失败: {e}")

            # 为 xy_orders 补建 (placed_at, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_placed_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_placed_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_placed_status 创建失败: {e}")

            # 为 xy_orders 补建 (created_at, status) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_created_status'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_created_status 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_created_status 创建失败: {e}")

            # 为 xy_orders 补建 (owner_id, created_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'idx_order_owner_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_orders ADD"
                    ))
                    logger.info("✓ xy_orders: 创建 idx_order_owner_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_orders idx_order_owner_created 创建失败: {e}")

            # 为 xy_agent_orders 补建 created_at 索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_agent_orders'
                    AND INDEX_NAME = 'idx_agent_order_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_agent_orders ADD"
                    ))
                    logger.info("✓ xy_agent_orders: 创建 idx_agent_order_created 索引")
            except Exception as e:
                logger.warning(f"✗ xy_agent_orders idx_agent_order_created 创建失败: {e}")

            # 为 xy_product_materials 补建 (user_id, created_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_product_materials'
                    AND INDEX_NAME = 'idx_pm_user_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_product_materials ADD"
                    ))
                    logger.info("✓ xy_product_materials: 创建 idx_pm_user_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_product_materials idx_pm_user_created 创建失败: {e}")

            # 为 xy_product_materials 补建平台分类索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_product_materials'
                    AND INDEX_NAME = 'idx_pm_platform_category'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_product_materials ADD"
                    ))
                    logger.info("✓ xy_product_materials: 创建 idx_pm_platform_category 索引")
            except Exception as e:
                logger.warning(f"✗ xy_product_materials idx_pm_platform_category 创建失败: {e}")

            # 为 xy_publish_logs 补建 (user_id, created_at) 复合索引
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_publish_logs'
                    AND INDEX_NAME = 'idx_publish_user_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_publish_logs ADD"
                    ))
                    logger.info("✓ xy_publish_logs: 创建 idx_publish_user_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_publish_logs idx_publish_user_created 创建失败: {e}")

            auto_reply_log_table_exists = False
            try:
                table_check = text("""
                    SELECT COUNT(*) FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_auto_reply_message_logs'
                """)
                table_result = await conn.execute(table_check)
                auto_reply_log_table_exists = table_result.scalar() > 0
            except Exception:
                auto_reply_log_table_exists = False

            if auto_reply_log_table_exists:
                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_arml_account_status_created'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_arml_account_status_created 复合索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_arml_account_status_created 创建失败: {e}")

                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_arml_owner_status_created'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_arml_owner_status_created 复合索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_arml_owner_status_created 创建失败: {e}")

                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_arml_status_strategy_created'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_arml_status_strategy_created 复合索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_arml_status_strategy_created 创建失败: {e}")

                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_arml_status_created'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_arml_status_created 复合索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_arml_status_created 创建失败: {e}")

                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_order_no'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_order_no 索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_order_no 创建失败: {e}")

                # 补建 (order_no, reply_strategy, id) 复合索引 —— 加速「按订单号+回复策略取最新一条日志」的查询
                # （订单列表关联自动发货发送状态：WHERE reply_strategy='auto_delivery' AND order_no IN (...) GROUP BY order_no, MAX(id)）
                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_arml_order_strategy_id'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_arml_order_strategy_id 复合索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_arml_order_strategy_id 创建失败: {e}")

                # 补建 (reply_strategy, order_no, id) 复合索引 —— 加速订单列表「发送状态」筛选
                # （子查询 WHERE reply_strategy='auto_delivery' GROUP BY order_no, MAX(id)，无 order_no 限定；
                #  reply_strategy 最左直接定位、order_no 有序分组、id 覆盖，避免百万级日志表全索引扫描）
                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_auto_reply_message_logs'
                        AND INDEX_NAME = 'idx_arml_strategy_order_id'
                    """)
                    result = await conn.execute(check)
                    if result.scalar() == 0:
                        await conn.execute(text(
                            "ALTER TABLE xy_auto_reply_message_logs ADD"
                        ))
                        logger.info("✓ xy_auto_reply_message_logs: 创建 idx_arml_strategy_order_id 复合索引")
                except Exception as e:
                    logger.warning(f"✗ xy_auto_reply_message_logs idx_arml_strategy_order_id 创建失败: {e}")

            # 为 xy_dock_records 补建 (source_user_id, level) 复合索引 —— 加速二级分销商列表查询
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_dock_records'
                    AND INDEX_NAME = 'idx_dock_source_level'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_dock_records ADD"
                    ))
                    logger.info("✓ xy_dock_records: 创建 idx_dock_source_level 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_dock_records idx_dock_source_level 创建失败: {e}")

            # 为 xy_cards 补建 (is_dockable, enabled) 复合索引 —— 加速可对接卡券列表主筛选
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_cards'
                    AND INDEX_NAME = 'idx_cards_dockable_enabled'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_cards ADD"
                    ))
                    logger.info("✓ xy_cards: 创建 idx_cards_dockable_enabled 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_cards idx_cards_dockable_enabled 创建失败: {e}")

            # 为 xy_personal_blacklist 补建 (owner_id, created_at) 复合索引 —— 加速个人黑名单列表分页排序
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_personal_blacklist'
                    AND INDEX_NAME = 'idx_pb_owner_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_personal_blacklist ADD"
                    ))
                    logger.info("✓ xy_personal_blacklist: 创建 idx_pb_owner_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_personal_blacklist idx_pb_owner_created 创建失败: {e}")

            # 为 xy_platform_blacklist 补建 created_at 索引 —— 加速闲鱼黑名单列表分页排序
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_platform_blacklist'
                    AND INDEX_NAME = 'idx_plb_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_platform_blacklist ADD"
                    ))
                    logger.info("✓ xy_platform_blacklist: 创建 idx_plb_created 索引")
            except Exception as e:
                logger.warning(f"✗ xy_platform_blacklist idx_plb_created 创建失败: {e}")

            # 为 xy_listing_monitor_items 补建 created_at 索引 —— 加速「卖家ID补全」等定时任务按当天采集入库时间过滤
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_listing_monitor_items'
                    AND INDEX_NAME = 'idx_lmi_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_listing_monitor_items ADD"
                    ))
                    logger.info("✓ xy_listing_monitor_items: 创建 idx_lmi_created 索引")
            except Exception as e:
                logger.warning(f"✗ xy_listing_monitor_items idx_lmi_created 创建失败: {e}")

            # 为 xy_listing_monitor_items 补建查询索引 —— 原仅有 task/owner/publish_time/created_at 索引，
            # 未覆盖调度任务/去重/列表的高频过滤字段，表数据增大后会全表扫描导致查询很慢
            lmi_query_indexes = [
                # 「采集商品发送私信」定时任务：order_status='success' + is_dm_sent=0 + ordered_at>=cutoff
                ("idx_lmi_dm_send", "(order_status, is_dm_sent, ordered_at)"),
                # 「采集商品自动下单」定时任务：is_ordered=0 + order_attempts<上限
                ("idx_lmi_order_pending", "(is_ordered, order_attempts)"),
                # 下单去重 has_owner_ordered_item：item_id + is_ordered（item_id 原仅为联合唯一键非最左列）
                ("idx_lmi_item_ordered", "(item_id, is_ordered)"),
                # 前端列表分页：owner_id 过滤 + 按 publish_time 排序
                ("idx_lmi_owner_publish", "(owner_id, publish_time)"),
                # 「采集商品自动下单」按用户配额取数：owner_id + is_ordered=0 + created_at>=窗口 + order_attempts<上限
                (
                    "idx_lmi_owner_order_pending",
                    "(owner_id, is_ordered, created_at, order_attempts)"
                ),
                # 「采集商品卖家ID补全」按用户/任务两级配额取数：owner_id + monitor_task_id 分组统计 + created_at>=窗口
                ("idx_lmi_owner_task_created", "(owner_id, monitor_task_id, created_at)"),
            ]
            for idx_name, idx_cols in lmi_query_indexes:
                try:
                    check = text("""
                        SELECT COUNT(*) FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = 'xy_listing_monitor_items'
                        AND INDEX_NAME = :idx_name
                    """)
                    result = await conn.execute(check, {"idx_name": idx_name})
                    if result.scalar() == 0:
                        await conn.execute(text(
                            f"ALTER TABLE xy_listing_monitor_items ADD INDEX {idx_name} {idx_cols}"
                        ))
                        logger.info(f"✓ xy_listing_monitor_items: 创建 {idx_name} 索引")
                except Exception as e:
                    logger.warning(f"✗ xy_listing_monitor_items {idx_name} 创建失败: {e}")

            # 为 xy_listing_monitor_tasks 补建 category_id 索引 —— 加速按分类筛选任务
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_listing_monitor_tasks'
                    AND INDEX_NAME = 'idx_lmt_category'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_listing_monitor_tasks ADD"
                    ))
                    logger.info("✓ xy_listing_monitor_tasks: 创建 idx_lmt_category 索引")
            except Exception as e:
                logger.warning(f"✗ xy_listing_monitor_tasks idx_lmt_category 创建失败: {e}")

            # 为 xy_risk_control_logs 补建 (owner_id, created_at) 复合索引 —— 加速按用户筛选+时间倒序分页
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_risk_control_logs'
                    AND INDEX_NAME = 'idx_rcl_owner_created'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    await conn.execute(text(
                        "ALTER TABLE xy_risk_control_logs ADD"
                    ))
                    logger.info("✓ xy_risk_control_logs: 创建 idx_rcl_owner_created 复合索引")
            except Exception as e:
                logger.warning(f"✗ xy_risk_control_logs idx_rcl_owner_created 创建失败: {e}")

            # 迁移兜底账号表唯一键：从 (owner_id) 改为 (owner_id, category_id)
            # xy_order_fallback_accounts
            try:
                check_old = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_order_fallback_accounts'
                    AND INDEX_NAME = 'uk_ofa_owner'
                """)
                result = await conn.execute(check_old)
                if result.scalar() > 0:
                    # 删除旧唯一键
                    await conn.execute(text(
                        "ALTER TABLE xy_order_fallback_accounts DROP INDEX uk_ofa_owner"
                    ))
                    logger.info("✓ xy_order_fallback_accounts: 删除旧唯一键 uk_ofa_owner")

                # 检查新唯一键是否存在
                check_new = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_order_fallback_accounts'
                    AND INDEX_NAME = 'uk_ofa_owner_category'
                """)
                result = await conn.execute(check_new)
                if result.scalar() == 0:
                    # 添加新唯一键
                    pass  # SQLite: 索引随建表生成, 无需迁移
            except Exception as e:
                logger.warning(f"✗ xy_order_fallback_accounts 唯一键迁移失败: {e}")

            # xy_collect_fallback_accounts
            try:
                check_old = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_collect_fallback_accounts'
                    AND INDEX_NAME = 'uk_cfa_owner'
                """)
                result = await conn.execute(check_old)
                if result.scalar() > 0:
                    # 删除旧唯一键
                    await conn.execute(text(
                        "ALTER TABLE xy_collect_fallback_accounts DROP INDEX uk_cfa_owner"
                    ))
                    logger.info("✓ xy_collect_fallback_accounts: 删除旧唯一键 uk_cfa_owner")

                # 检查新唯一键是否存在
                check_new = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_collect_fallback_accounts'
                    AND INDEX_NAME = 'uk_cfa_owner_category'
                """)
                result = await conn.execute(check_new)
                if result.scalar() == 0:
                    # 添加新唯一键
                    pass  # SQLite: xy_collect_fallback_accounts UNIQUE(owner_id, category_id) 随建表生成
            except Exception as e:
                logger.warning(f"✗ xy_collect_fallback_accounts 唯一键迁移失败: {e}")

            # 为 xy_orders 补建 (account_id, order_no) 唯一约束 —— 防止「定时获取闲鱼订单」
            # 与「获取待发货订单」两个任务并发 upsert 时重复插入同一订单（B方案兜底）。
            # 注意：项目硬性规范禁止删除数据，存在历史重复数据时不自动清理，
            # 仅打印警告并跳过创建，待人工合并后下次启动自检再补建。
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_orders'
                    AND INDEX_NAME = 'uk_order_account_no'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    # 先检测是否存在 (account_id, order_no) 重复数据
                    dup_check = text("""
                        SELECT COUNT(*) FROM (
                            SELECT account_id, order_no
                            FROM xy_orders
                            GROUP BY account_id, order_no
                            HAVING COUNT(*) > 1
                        ) AS dup
                    """)
                    dup_result = await conn.execute(dup_check)
                    dup_groups = dup_result.scalar() or 0
                    if dup_groups > 0:
                        logger.warning(
                            f"✗ xy_orders 存在 {dup_groups} 组 (account_id, order_no) 重复数据，"
                            f"为遵守禁止删除数据规范，暂不创建 uk_order_account_no 唯一约束。"
                            f"请人工合并重复订单后，重启服务自动补建（当前由 Redis 账号锁兜底防并发）"
                        )
                    else:
                        pass  # SQLite: xy_orders UNIQUE(account_id, order_no) 随建表生成
            except Exception as e:
                logger.warning(f"✗ xy_orders uk_order_account_no 创建失败: {e}")

            # 为 xy_accounts 补建 account_id 全局唯一约束 —— 闲鱼账号ID全局唯一，
            # 杜绝不同用户绑定同一账号、以及并发创建导致的重复（业务大量代码仅按
            # account_id 查询并取 first，依赖其全局唯一）。
            # 注意：项目硬性规范禁止删除数据，存在历史重复数据时不自动清理，
            # 仅打印警告并跳过创建，待人工合并后下次启动自检再补建。
            try:
                check = text("""
                    SELECT COUNT(*) FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'xy_accounts'
                    AND INDEX_NAME = 'uk_account_id'
                """)
                result = await conn.execute(check)
                if result.scalar() == 0:
                    # 先检测是否存在 account_id 重复数据（全局，不区分 owner_id）
                    dup_check = text("""
                        SELECT COUNT(*) FROM (
                            SELECT account_id
                            FROM xy_accounts
                            GROUP BY account_id
                            HAVING COUNT(*) > 1
                        ) AS dup
                    """)
                    dup_result = await conn.execute(dup_check)
                    dup_groups = dup_result.scalar() or 0
                    if dup_groups > 0:
                        logger.warning(
                            f"✗ xy_accounts 存在 {dup_groups} 组 account_id 重复数据，"
                            f"为遵守禁止删除数据规范，暂不创建 uk_account_id 唯一约束。"
                            f"请人工合并/处理重复账号后，重启服务自动补建"
                        )
                    else:
                        pass  # SQLite: xy_accounts UNIQUE(account_id) 随建表生成
            except Exception as e:
                logger.warning(f"✗ xy_accounts uk_account_id 创建失败: {e}")


    async def create_default_admin(self):
        """创建默认管理员用户 (admin/admin123)"""
        logger.info("检查默认管理员用户...")
        
        try:
            async with async_session_maker() as session:
                # 检查是否已存在admin用户
                result = await session.execute(
                    text("SELECT id FROM xy_users WHERE username = 'admin' LIMIT 1")
                )
                existing = result.fetchone()
                
                if existing:
                    logger.info("✓ 管理员用户已存在，跳过创建")
                    return
                
                # 使用 passlib 创建密码哈希
                password_hash = get_password_hash("admin123")
                
                # 插入管理员用户
                await session.execute(
                    text("""
                        INSERT INTO xy_users (username, email, password_hash, status, role, created_at, updated_at)
                        VALUES ('admin', 'admin@example.com', :password_hash, 'ACTIVE', 'ADMIN', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """),
                    {"password_hash": password_hash}
                )
                await session.commit()
                
                logger.info("✓ 默认管理员用户创建成功")
                logger.info("  用户名: admin")
                logger.info("  密码: admin123")
                
        except IntegrityError:
            logger.info("✓ 管理员用户已存在，跳过创建")
        except Exception as e:
            logger.error(f"✗ 创建管理员用户失败: {e}")
    
    async def init_system_settings(self):
        """初始化系统设置"""
        logger.info("初始化系统设置...")
        
        try:
            async with async_session_maker() as session:
                for key, value, description in self.DEFAULT_SETTINGS:
                    try:
                        await session.execute(
                            text("""
                                INSERT OR IGNORE INTO xy_system_settings (key, value, description, updated_at)
                                VALUES (:key, :value, :description, CURRENT_TIMESTAMP)
                            """),
                            {"key": key, "value": value, "description": description}
                        )
                    except Exception as e:
                        logger.warning(f"设置 {key} 插入失败: {e}")
                
                await session.commit()
                logger.info(f"✓ 系统设置初始化完成，共 {len(self.DEFAULT_SETTINGS)} 项")
                
        except Exception as e:
            logger.error(f"✗ 初始化系统设置失败: {e}")

    async def init_scheduled_tasks(self):
        """初始化定时任务配置"""
        logger.info("初始化定时任务配置...")
        
        try:
            async with async_session_maker() as session:
                for task_code, task_name, interval_seconds, enabled, description in self.DEFAULT_SCHEDULED_TASKS:
                    try:
                        await session.execute(
                            text("""
                                INSERT OR IGNORE INTO xy_scheduled_tasks 
                                (task_code, task_name, interval_seconds, enabled, description, created_at, updated_at)
                                VALUES (:task_code, :task_name, :interval_seconds, :enabled, :description, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            """),
                            {
                                "task_code": task_code,
                                "task_name": task_name,
                                "interval_seconds": interval_seconds,
                                "enabled": enabled,
                                "description": description
                            }
                        )
                    except Exception as e:
                        logger.warning(f"定时任务 {task_code} 插入失败: {e}")
                
                await session.commit()
                logger.info(f"✓ 定时任务配置初始化完成，共 {len(self.DEFAULT_SCHEDULED_TASKS)} 项")
                
        except Exception as e:
            logger.error(f"✗ 初始化定时任务配置失败: {e}")

    async def init_publish_addresses(self):
        """初始化随机地址默认数据"""
        logger.info("初始化随机地址默认数据...")

        try:
            default_addresses = build_default_publish_addresses()
            async with async_session_maker() as session:
                removed_conditions = []
                removed_params: dict[str, str] = {}
                for index, prefix in enumerate(REMOVED_PUBLISH_ADDRESS_PREFIXES):
                    param_key = f"removed_prefix_{index}"
                    removed_conditions.append(f"search_keyword LIKE :{param_key}")
                    removed_params[param_key] = f"{prefix}%"

                if removed_conditions:
                    await session.execute(
                        text(
                            f"""
                                UPDATE xy_publish_addresses
                                SET is_enabled = 0,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE account_id IS NULL
                                  AND remark = '系统初始化默认地址'
                                  AND ({' OR '.join(removed_conditions)})
                            """
                        ),
                        removed_params
                    )

                existing_result = await session.execute(
                    text("""
                        SELECT search_keyword
                        FROM xy_publish_addresses
                        WHERE account_id IS NULL
                    """)
                )
                existing_keywords = {
                    str(search_keyword).strip()
                    for search_keyword in existing_result.scalars().all()
                    if search_keyword and str(search_keyword).strip()
                }

                insert_rows = [
                    {
                        "name": address,
                        "search_keyword": address,
                        "sort_order": index,
                        "remark": "系统初始化默认地址",
                    }
                    for index, address in enumerate(default_addresses, start=1)
                    if address not in existing_keywords
                ]

                if insert_rows:
                    await session.execute(
                        text("""
                            INSERT INTO xy_publish_addresses (
                                name,
                                search_keyword,
                                expected_text,
                                account_id,
                                weight,
                                sort_order,
                                is_enabled,
                                use_count,
                                last_used_at,
                                created_by,
                                remark,
                                created_at,
                                updated_at
                            ) VALUES (
                                :name,
                                :search_keyword,
                                NULL,
                                NULL,
                                1,
                                :sort_order,
                                1,
                                0,
                                NULL,
                                NULL,
                                :remark,
                                CURRENT_TIMESTAMP,
                                CURRENT_TIMESTAMP
                            )
                        """),
                        insert_rows
                    )
                    await session.commit()

                logger.info(
                    f"✓ 随机地址默认数据初始化完成，默认 {len(default_addresses)} 条，本次新增 {len(insert_rows)} 条"
                )

        except Exception as e:
            logger.warning(f"✗ 初始化随机地址默认数据失败（不影响系统运行）: {e}")

    async def init_redis_platform_day(self):
        """初始化Redis中的平台日"""
        logger.info("初始化Redis平台日...")
        
        try:
            from common.db.redis_client import get_redis_client
            
            redis_client = await get_redis_client()
            platform_day_key = "platform:day"
            
            # 检查Redis中是否已存在平台日
            existing_day = await redis_client.get(platform_day_key)
            
            if existing_day:
                logger.info(f"✓ Redis平台日已存在: {existing_day}")
            else:
                # 设置当前日期为平台日
                current_day = get_beijing_now_naive().strftime("%Y-%m-%d")
                await redis_client.set(platform_day_key, current_day)
                logger.info(f"✓ Redis平台日初始化完成: {current_day}")
                
        except Exception as e:
            logger.warning(f"✗ 初始化Redis平台日失败（不影响系统运行）: {e}")


    async def migrate_card_item_relations(self):
        """
        迁移卡券商品关联数据（仅在关联表为空时执行一次）
        
        将 xy_cards 表中 item_id 不为空的记录迁移到 xy_card_item_relations 关联表。
        关联表已有数据时跳过，避免重复迁移。
        """
        try:
            async with async_session_maker() as session:
                # 检查关联表中已有数据量
                existing_result = await session.execute(
                    text("SELECT COUNT(*) FROM xy_card_item_relations")
                )
                existing = existing_result.scalar()
                
                if existing > 0:
                    logger.debug(f"✓ 卡券商品关联表已有 {existing} 条数据，跳过迁移")
                    return
                
                # 统计需要迁移的数据量
                count_result = await session.execute(
                    text("""
                        SELECT COUNT(*) FROM xy_cards 
                        WHERE item_id IS NOT NULL AND item_id != ''
                    """)
                )
                total = count_result.scalar()
                
                if total == 0:
                    logger.info("✓ 无需迁移卡券商品关联数据")
                    return
                
                # 首次迁移：从 xy_cards 导入到关联表
                migrate_result = await session.execute(
                    text("""
                        INSERT OR IGNORE INTO xy_card_item_relations (user_id, card_id, item_id, dock_record_id, created_at, updated_at)
                        SELECT user_id, id, item_id, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        FROM xy_cards
                        WHERE item_id IS NOT NULL AND item_id != ''
                    """)
                )
                await session.commit()
                
                migrated = migrate_result.rowcount
                logger.info(f"✓ 卡券商品关联数据首次迁移完成：源数据 {total} 条，本次迁移 {migrated} 条")
                
        except Exception as e:
            logger.warning(f"✗ 卡券商品关联数据迁移失败（不影响系统运行）: {e}")

    async def migrate_delivery_block_rules(self):
        """迁移旧禁止发货设置到规则配置表

        对于已开启 delivery_disabled=1 但 xy_delivery_block_rules 表中无记录的账号，
        自动将旧配置迁移为一条 buyer_credit_zero 规则。
        迁移后旧字段保留不删除，但不再使用。
        """
        try:
            async with async_session_maker() as session:
                # 查找已开启旧禁止发货开关的账号
                result = await session.execute(
                    text("""
                        SELECT a.account_id, a.delivery_disabled_reason,
                               a.auto_close_order, a.delivery_only_card_after_close,
                               a.delivery_disabled_excluded_items
                        FROM xy_accounts a
                        WHERE a.delivery_disabled = 1
                        AND NOT EXISTS (
                            SELECT 1 FROM xy_delivery_block_rules r
                            WHERE r.account_id = a.account_id
                        )
                    """)
                )
                rows = result.fetchall()

                if not rows:
                    logger.info("✓ 无需迁移禁止发货规则数据")
                    return

                migrated_count = 0
                for row in rows:
                    account_id = row[0]
                    reason = row[1]
                    auto_close = row[2]
                    only_card = row[3]
                    excluded_items = row[4]

                    # 归一化 excluded_items（可能是 TEXT 字符串或 list）
                    excluded_json = None
                    if excluded_items:
                        if isinstance(excluded_items, str):
                            excluded_json = excluded_items  # 已经是 TEXT 字符串
                        elif isinstance(excluded_items, list):
                            import json as _json
                            excluded_json = _json.dumps(excluded_items, ensure_ascii=False)

                    try:
                        await session.execute(
                            text("""
                                INSERT OR IGNORE INTO xy_delivery_block_rules
                                (account_id, rule_code, enabled, priority, block_reason,
                                 auto_close_order, only_card_after_close, excluded_item_ids, config)
                                VALUES
                                (:account_id, 'buyer_credit_zero', 1, 10, :block_reason,
                                 :auto_close, :only_card, :excluded_ids, '{"threshold": 0}')
                            """),
                            {
                                "account_id": account_id,
                                "block_reason": reason,
                                "auto_close": 1 if auto_close else 0,
                                "only_card": 1 if only_card else 0,
                                "excluded_ids": excluded_json,
                            }
                        )
                        migrated_count += 1
                    except Exception as row_err:
                        logger.warning(
                            f"✗ 迁移账号 {account_id} 禁止发货规则失败: {row_err}"
                        )

                await session.commit()
                logger.info(
                    f"✓ 禁止发货规则迁移完成：共 {len(rows)} 个账号，成功迁移 {migrated_count} 条"
                )

        except Exception as e:
            logger.warning(f"✗ 禁止发货规则迁移失败（不影响系统运行）: {e}")

    async def backfill_user_secret_keys(self):
        """为历史用户回填分销秘钥

        存量用户在新增 secret_key 列后值为 NULL，这里在服务启动自检时
        统一为它们生成 32 位随机秘钥（全局唯一），避免依赖用户访问页面才懒生成。
        逐个用户生成并校验唯一性，遇到唯一约束冲突时重试，最多 10 次。
        不影响已有秘钥的用户。
        """
        try:
            async with async_session_maker() as session:
                # 查询所有秘钥为空的用户ID
                result = await session.execute(
                    text("SELECT id FROM xy_users WHERE secret_key IS NULL OR secret_key = ''")
                )
                user_ids = [row[0] for row in result.fetchall()]

                if not user_ids:
                    logger.info("✓ 无需回填分销秘钥")
                    return

                # 预加载已有秘钥，减少唯一性冲突的数据库往返
                existing_result = await session.execute(
                    text("SELECT secret_key FROM xy_users WHERE secret_key IS NOT NULL AND secret_key != ''")
                )
                used_keys = {row[0] for row in existing_result.fetchall()}

                backfilled = 0
                for user_id in user_ids:
                    # 生成不与已知秘钥重复的新秘钥
                    new_key = None
                    for _ in range(10):
                        candidate = generate_secret_key()
                        if candidate not in used_keys:
                            new_key = candidate
                            break
                    if not new_key:
                        logger.warning(f"✗ 用户 {user_id} 分销秘钥生成失败（多次重复），跳过")
                        continue

                    try:
                        await session.execute(
                            text("UPDATE xy_users SET secret_key = :key WHERE id = :uid"),
                            {"key": new_key, "uid": user_id}
                        )
                        used_keys.add(new_key)
                        backfilled += 1
                    except Exception as row_err:
                        logger.warning(f"✗ 回填用户 {user_id} 分销秘钥失败: {row_err}")

                await session.commit()
                logger.info(
                    f"✓ 分销秘钥回填完成：共 {len(user_ids)} 个历史用户，成功回填 {backfilled} 个"
                )

        except Exception as e:
            logger.warning(f"✗ 分销秘钥回填失败（不影响系统运行）: {e}")


async def init_database():
    """初始化数据库（供外部调用）"""
    initializer = DatabaseInitializer()
    await initializer.init_all()


# 如果直接运行此脚本
if __name__ == "__main__":
    import asyncio
    asyncio.run(init_database())
