"""自动续售表的幂等建表与字段迁移。"""
from __future__ import annotations

from datetime import datetime

from loguru import logger
from sqlalchemy import text


AUTO_RELIST_TABLE_DDL = {
    "xy_auto_relist_rules": """
        CREATE TABLE IF NOT EXISTS xy_auto_relist_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            material_id INTEGER NOT NULL,
            account_id VARCHAR(80) NOT NULL,
            current_item_id VARCHAR(64) NOT NULL,
            card_id INTEGER NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 0,
            delay_seconds INT NOT NULL DEFAULT 60,
            status VARCHAR(24) NOT NULL DEFAULT 'disabled',
            version INTEGER NOT NULL DEFAULT 0,
            retry_count INT NOT NULL DEFAULT 0,
            next_retry_at DATETIME NULL,
            last_order_no VARCHAR(64) NULL,
            last_order_id INTEGER NULL,
            last_order_updated_at DATETIME NULL,
            last_old_item_id VARCHAR(64) NULL,
            last_new_item_id VARCHAR(64) NULL,
            last_error TEXT NULL,
            paused_reason VARCHAR(500) NULL,
            last_relisted_at DATETIME NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(material_id)
        )""",
    "xy_auto_relist_events": """
        CREATE TABLE IF NOT EXISTS xy_auto_relist_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            rule_id INTEGER NOT NULL,
            material_id INTEGER NOT NULL,
            account_id VARCHAR(80) NOT NULL,
            order_no VARCHAR(64) NOT NULL,
            old_item_id VARCHAR(64) NOT NULL,
            new_item_id VARCHAR(64) NULL,
            status VARCHAR(24) NOT NULL DEFAULT 'pending',
            attempt_count INT NOT NULL DEFAULT 0,
            next_retry_at DATETIME NULL,
            error_message TEXT NULL,
            claim_token VARCHAR(64) NULL,
            claimed_at DATETIME NULL,
            lease_expires_at DATETIME NULL,
            publish_request_id VARCHAR(100) NULL,
            publish_state VARCHAR(20) NOT NULL DEFAULT 'not_started',
            publish_item_id VARCHAR(64) NULL,
            last_checked_at DATETIME NULL,
            result_unknown INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(rule_id, order_no),
            UNIQUE(publish_request_id)
        )""",
    "xy_relist_association_migrations": """
        CREATE TABLE IF NOT EXISTS xy_relist_association_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            step VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            attempt_count INT NOT NULL DEFAULT 0,
            error_message VARCHAR(1000) NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_id, step)
        )""",
}

AUTO_RELIST_COLUMNS = {
    "xy_auto_relist_rules": {
        "user_id": "INTEGER NOT NULL DEFAULT 0",
        "material_id": "INTEGER NOT NULL DEFAULT 0",
        "account_id": "VARCHAR(80) NOT NULL DEFAULT ''",
        "current_item_id": "VARCHAR(64) NOT NULL DEFAULT ''",
        "card_id": "INTEGER NOT NULL DEFAULT 0",
        "enabled": "INTEGER NOT NULL DEFAULT 0",
        "delay_seconds": "INT NOT NULL DEFAULT 60",
        "status": "VARCHAR(24) NOT NULL DEFAULT 'disabled'",
        "retry_count": "INT NOT NULL DEFAULT 0",
        "next_retry_at": "DATETIME NULL",
        "last_order_no": "VARCHAR(64) NULL",
        "version": "INTEGER NOT NULL DEFAULT 0",
        "last_order_id": "INTEGER NULL",
        "last_order_updated_at": "DATETIME NULL",
        "last_old_item_id": "VARCHAR(64) NULL",
        "last_new_item_id": "VARCHAR(64) NULL",
        "last_error": "TEXT NULL",
        "paused_reason": "VARCHAR(500) NULL",
        "last_relisted_at": "DATETIME NULL",
        "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
    },
    "xy_auto_relist_events": {
        "user_id": "INTEGER NOT NULL DEFAULT 0",
        "rule_id": "INTEGER NOT NULL DEFAULT 0",
        "material_id": "INTEGER NOT NULL DEFAULT 0",
        "account_id": "VARCHAR(80) NOT NULL DEFAULT ''",
        "order_no": "VARCHAR(64) NOT NULL DEFAULT ''",
        "old_item_id": "VARCHAR(64) NOT NULL DEFAULT ''",
        "new_item_id": "VARCHAR(64) NULL",
        "status": "VARCHAR(24) NOT NULL DEFAULT 'pending'",
        "attempt_count": "INT NOT NULL DEFAULT 0",
        "next_retry_at": "DATETIME NULL",
        "error_message": "TEXT NULL",
        "claim_token": "VARCHAR(64) NULL",
        "claimed_at": "DATETIME NULL",
        "lease_expires_at": "DATETIME NULL",
        "publish_request_id": "VARCHAR(100) NULL",
        "publish_state": "VARCHAR(20) NOT NULL DEFAULT 'not_started'",
        "publish_item_id": "VARCHAR(64) NULL",
        "last_checked_at": "DATETIME NULL",
        "result_unknown": "INTEGER NOT NULL DEFAULT 0",
        "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
    },
    "xy_relist_association_migrations": {
        "event_id": "INTEGER NOT NULL DEFAULT 0",
        "user_id": "INTEGER NOT NULL DEFAULT 0",
        "step": "VARCHAR(50) NOT NULL DEFAULT ''",
        "status": "VARCHAR(20) NOT NULL DEFAULT 'pending'",
        "attempt_count": "INT NOT NULL DEFAULT 0",
        "error_message": "VARCHAR(1000) NULL",
        "created_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
    },
}

AUTO_RELIST_INDEXES = {
    "xy_auto_relist_rules": {
        "uk_auto_relist_material": "UNIQUE (material_id)",
        "idx_auto_relist_user_enabled_due": "(user_id, enabled, next_retry_at)",
        "idx_auto_relist_account_item": "(account_id, current_item_id)",
    },
    "xy_auto_relist_events": {
        "uk_auto_relist_rule_order": "UNIQUE (rule_id, order_no)",
        "uk_auto_relist_publish_request": "UNIQUE (publish_request_id)",
        "idx_auto_relist_event_due": "(status, next_retry_at, lease_expires_at)",
        "idx_auto_relist_event_user_created": "(user_id, created_at)",
    },
    "xy_relist_association_migrations": {
        "uk_relist_migration_event_step": "UNIQUE (event_id, step)",
        "idx_relist_migration_event_status": "(event_id, status)",
    },
    "xy_publish_logs": {
        "uk_publish_request_id": "UNIQUE (publish_request_id)",
    },
}

UNIQUE_INDEX_COLUMNS = {
    ("xy_auto_relist_rules", "uk_auto_relist_material"): ("material_id",),
    ("xy_auto_relist_events", "uk_auto_relist_rule_order"): ("rule_id", "order_no"),
    ("xy_auto_relist_events", "uk_auto_relist_publish_request"): ("publish_request_id",),
    ("xy_relist_association_migrations", "uk_relist_migration_event_step"): ("event_id", "step"),
    ("xy_publish_logs", "uk_publish_request_id"): ("publish_request_id",),
}


async def _column_exists(conn, table_name: str, column_name: str) -> bool:
    """跨库判列存在: SQLite用PRAGMA, MySQL用information_schema。"""
    from common.core.config import get_settings
    if get_settings().async_database_url.startswith("sqlite"):
        from sqlalchemy import text as _t
        res = await conn.execute(_t(f"PRAGMA table_info({table_name})"))
        return any(row[1] == column_name for row in res.fetchall())
    res = await conn.execute(
        text("""SELECT COUNT(*) FROM information_schema.COLUMNS
               WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
               AND COLUMN_NAME = :column_name"""),
        {"table_name": table_name, "column_name": column_name},
    )
    return bool(res.scalar())


async def ensure_auto_relist_schema(conn, current_time: datetime) -> None:
    """创建自动续售表并幂等补齐字段、索引和发布日志关联字段。

    Args:
        conn: 数据库连接。
        current_time: 应用侧传入的北京时间（无时区 datetime），用于恢复过期租约。
    """
    for table_name, ddl in AUTO_RELIST_TABLE_DDL.items():
        await conn.execute(text(ddl))

    for table_name, columns in AUTO_RELIST_COLUMNS.items():
        for column_name, column_definition in columns.items():
            if not await _column_exists(conn, table_name, column_name):
                await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"))
                logger.info("自动续售表补齐字段: {}.{}", table_name, column_name)

    # 兼容早期方案使用 owner_id 命名的表结构。新增的 user_id 保持当前代码统一口径，
    # 只做回填，不删除旧字段或改写已有非空 user_id。
    for table_name in ("xy_auto_relist_rules", "xy_auto_relist_events"):
        if await _column_exists(conn, table_name, 'owner_id'):
            await conn.execute(
                text(
                    f"UPDATE {table_name} SET user_id = owner_id "
                    "WHERE (user_id = 0 OR user_id IS NULL) AND owner_id IS NOT NULL"
                )
            )

    publish_columns = {
        "publish_request_id": "VARCHAR(100) NULL",
        "source_event_id": "INTEGER NULL",
    }
    for column_name, column_definition in publish_columns.items():
        if not await _column_exists(conn, 'xy_publish_logs', column_name):
            await conn.execute(text(f"ALTER TABLE xy_publish_logs ADD COLUMN {column_name} {column_definition}"))
            logger.info("发布日志补齐字段: {}", column_name)

    for table_name, indexes in AUTO_RELIST_INDEXES.items():
        for index_name, index_definition in indexes.items():
            from common.core.config import get_settings as _gs
            if _gs().async_database_url.startswith("sqlite"):
                res = await conn.execute(text(f"PRAGMA index_list({table_name})"))
                exists_flag = any(row[1] == index_name for row in res.fetchall())
            else:
                _e = await conn.execute(
                    text("""SELECT COUNT(*) FROM information_schema.STATISTICS
                           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
                           AND INDEX_NAME = :index_name"""),
                    {"table_name": table_name, "index_name": index_name}
                )
                exists_flag = bool(_e.scalar())
            if not exists_flag:
                if index_definition.startswith("UNIQUE"):
                    columns = UNIQUE_INDEX_COLUMNS[(table_name, index_name)]
                    group_columns = ", ".join(f"{column}" for column in columns)
                    non_null = " AND ".join(f"{column} IS NOT NULL" for column in columns)
                    duplicate_result = await conn.execute(
                        text(
                            f"""SELECT COUNT(*) FROM (
                                SELECT {group_columns}
                                FROM {table_name}
                                WHERE {non_null}
                                GROUP BY {group_columns}
                                HAVING COUNT(*) > 1
                            ) AS duplicate_groups"""
                        )
                    )
                    duplicate_groups = int(duplicate_result.scalar() or 0)
                    if duplicate_groups:
                        message = (
                            f"表 {table_name} 存在 {duplicate_groups} 组重复数据，无法安全创建唯一约束 {index_name}；"
                            "为保护历史数据，已停止自动续售调度，请先人工备份并处理重复记录"
                        )
                        logger.error(message)
                        raise RuntimeError(message)
                from common.core.config import get_settings as _gs
                if _gs().async_database_url.startswith("sqlite"):
                    uniq = "UNIQUE " if index_definition.startswith("UNIQUE") else ""
                    definition = index_definition.replace("UNIQUE ", "", 1) if index_definition.startswith("UNIQUE") else index_definition
                    await conn.execute(text(f"CREATE {uniq}INDEX {index_name} ON {table_name} {definition}"))
                else:
                    add_clause = "ADD UNIQUE KEY" if index_definition.startswith("UNIQUE") else "ADD KEY"
                    definition = index_definition.replace("UNIQUE ", "", 1) if index_definition.startswith("UNIQUE") else index_definition
                    await conn.execute(text(f"ALTER TABLE {table_name} {add_clause} {index_name} {definition}"))

    # 仅恢复租约已过期（或历史记录没有租约）的发布事件，不能把仍在执行中的
    # worker 误标为未知结果。未知事件后续必须人工对账，禁止盲目再次上架。
    await conn.execute(text("""UPDATE xy_auto_relist_events
        SET result_unknown = 1,
            status = 'unknown',
            publish_state = 'unknown',
            claim_token = NULL,
            claimed_at = NULL,
            lease_expires_at = NULL
        WHERE status = 'publishing'
          AND (result_unknown = 0 OR result_unknown IS NULL)
          AND (publish_state IS NULL OR publish_state <> 'succeeded')
          AND (lease_expires_at IS NULL OR lease_expires_at < :current_time)"""),
        {"current_time": current_time}
    )
