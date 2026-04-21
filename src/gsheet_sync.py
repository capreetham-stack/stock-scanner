"""
Google Sheets sync utility.
Creates a new worksheet per day and writes scanner output rows.
"""

from __future__ import annotations

import os
import json
import re
import logging
import datetime as dt
from typing import Any

import gspread
from gspread.exceptions import APIError, SpreadsheetNotFound
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class GoogleSheetSync:
    def __init__(self, spreadsheet_url: str, creds_path: str):
        self.spreadsheet_url = spreadsheet_url
        self.creds_path = creds_path

    @staticmethod
    def _extract_sheet_key(url_or_key: str) -> str:
        if "/spreadsheets/d/" not in url_or_key:
            return url_or_key.strip()
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url_or_key)
        if not m:
            raise ValueError("Invalid Google Sheet URL")
        return m.group(1)

    def _client(self) -> gspread.Client:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        if not os.path.exists(self.creds_path):
            raise FileNotFoundError(f"Google credentials file not found: {self.creds_path}")
        creds = Credentials.from_service_account_file(self.creds_path, scopes=scopes)
        return gspread.authorize(creds)

    def _open_sheet(self, client: gspread.Client):
        try:
            if "/spreadsheets/d/" in self.spreadsheet_url:
                return client.open_by_url(self.spreadsheet_url)
            sheet_key = self._extract_sheet_key(self.spreadsheet_url)
            return client.open_by_key(sheet_key)
        except PermissionError as exc:
            raise RuntimeError(
                "Google Sheet permission denied. Share the sheet with the service-account "
                "email as Editor."
            ) from exc
        except SpreadsheetNotFound as exc:
            raise RuntimeError(
                "Spreadsheet not found or not shared with the service account. "
                "Share the sheet with the service-account email as Editor."
            ) from exc
        except APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", "unknown")
            body = getattr(getattr(exc, "response", None), "text", "")
            raise RuntimeError(f"Google Sheets API error {status}: {body}") from exc

    @staticmethod
    def _sheet_cell_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=True)
        return str(value)

    @staticmethod
    def _fmt_pct(value: Any) -> str:
        if value in (None, ""):
            return "N/A"
        try:
            number = float(value)
            return f"{number:+.2f}%"
        except Exception:
            return str(value)

    @classmethod
    def _format_morning_row(cls, rank: int, row: dict[str, Any]) -> dict[str, Any]:
        trend_summary = (
            f"7D {cls._fmt_pct(row.get('chg_7d_pct'))} | "
            f"30D {cls._fmt_pct(row.get('chg_30d_pct'))} | "
            f"3M {cls._fmt_pct(row.get('chg_90d_pct'))}"
        )
        technical_summary = (
            f"RSI {row.get('rsi', '')} | MACD {row.get('macd_hist', '')} | "
            f"ADX {row.get('adx', '')} | Vol {row.get('vol_ratio', '')}x | "
            f"ST {row.get('supertrend', '')}"
        )
        trade_plan = (
            f"Entry {row.get('entry', '')} | SL {row.get('stop_loss', '')} | "
            f"TGT {row.get('target', '')} | R/R {row.get('reward_risk', '')}"
        )
        zone_summary = (
            f"Demand {row.get('demand_zone', 'N/A')} | "
            f"Supply {row.get('supply_zone', 'N/A')}"
        )
        return {
            "Rank": rank,
            "Symbol": row.get("symbol", ""),
            "Decision": row.get("buy_heading", ""),
            "Why Buy": row.get("why_buy", ""),
            "Cautions": row.get("cautions_summary", ""),
            "Score": row.get("score", ""),
            "Current Price": row.get("current_price", ""),
            "Gap %": cls._fmt_pct(row.get("gap_pct", "")),
            "Trend Check": trend_summary,
            "Trade Plan": trade_plan,
            "Technical Snapshot": technical_summary,
            "Zones": zone_summary,
            "Pattern": row.get("pattern", ""),
            "Buy/Sell Ratio": row.get("buy_sell_ratio", ""),
            "Reasons Detail": row.get("reasons", ""),
            "Warnings Detail": row.get("warnings", ""),
        }

    def sync_daily(self, result: dict[str, Any], prefix: str = "SCAN") -> str:
        client = self._client()
        sh = self._open_sheet(client)

        IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
        if prefix == "HOURLY":
            date_title = dt.datetime.now(IST).strftime("%Y-%m-%d_%H00")
        else:
            date_title = dt.datetime.now(IST).strftime("%Y-%m-%d")
            
        ws_title = f"{prefix}_{date_title}"

        existing = {ws.title for ws in sh.worksheets()}
        final_title = ws_title
        idx = 1
        while final_title in existing:
            idx += 1
            final_title = f"{ws_title}_{idx}"

        if not result.get("buy_list"):
            logger.info("No buy_list rows found. Inserting placeholder row.")
            dummy = {"symbol": "NONE", "buy_heading": "No stocks met criteria"}
            rows = [self._format_morning_row(1, dummy)]
        else:
            rows = [self._format_morning_row(idx, s.to_dict()) for idx, s in enumerate(result["buy_list"], 1)]
        headers = list(rows[0].keys())

        ws = sh.add_worksheet(title=final_title, rows=max(100, len(rows) + 5), cols=max(20, len(headers) + 2))
        values = [headers] + [
            [self._sheet_cell_value(r.get(h, "")) for h in headers]
            for r in rows
        ]
        ws.update(range_name="A1", values=values, value_input_option="RAW")

        # Freeze header row for mobile/web readability.
        ws.freeze(rows=1)

        logger.info("Google Sheet rows written: %d", len(rows))

        logger.info("Google Sheet sync complete: worksheet '%s'", final_title)
        return final_title
