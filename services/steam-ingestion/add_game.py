import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


BASE_DIR = Path(__file__).resolve().parent
WATCHLIST_FILE = BASE_DIR / "watchlist.json"

STEAM_SEARCH_URL = "https://store.steampowered.com/api/storesearch/"
HTTP_TIMEOUT_SECONDS = 30


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [add-game] %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_ALERT_KEYWORDS = [
    "security",
    "exploit",
    "cheat",
    "matchmaking",
    "ranked",
    "balance",
    "patch",
    "bug",
    "fix",
]


def load_watchlist() -> Dict[str, Any]:
    if not WATCHLIST_FILE.exists():
        return {"games": []}

    with WATCHLIST_FILE.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if "games" not in data or not isinstance(data["games"], list):
        raise ValueError("watchlist.json must contain a top-level 'games' array")

    return data


def save_watchlist(data: Dict[str, Any]) -> None:
    with WATCHLIST_FILE.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
        file.write("\n")

def search_steam_apps(query: str, limit: int) -> List[Dict[str, Any]]:
    logger.info("Searching Steam store for: %s", query)

    params = {
        "term": query,
        "l": "english",
        "cc": "US",
    }

    response = requests.get(
        STEAM_SEARCH_URL,
        params=params,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    data = response.json()
    items = data.get("items", [])

    if not isinstance(items, list):
        raise ValueError("Unexpected Steam store search response shape")

    results = []

    for item in items:
        app_id = item.get("id")
        name = str(item.get("name", "")).strip()

        if app_id is None or not name:
            continue

        results.append(
            {
                "app_id": int(app_id),
                "name": name,
            }
        )

        if len(results) >= limit:
            break

    return results

def app_already_exists(watchlist: Dict[str, Any], app_id: int) -> bool:
    for game in watchlist["games"]:
        if int(game.get("app_id")) == app_id:
            return True

    return False


def add_game_to_watchlist(
    watchlist: Dict[str, Any],
    app_id: int,
    name: str,
    enabled: bool,
    alert_keywords: List[str],
) -> None:
    if app_already_exists(watchlist, app_id):
        logger.info("Game already exists in watchlist: app_id=%s name=%s", app_id, name)
        return

    watchlist["games"].append(
        {
            "app_id": app_id,
            "name": name,
            "enabled": enabled,
            "alert_keywords": alert_keywords,
        }
    )

    watchlist["games"].sort(key=lambda game: str(game.get("name", "")).lower())


def choose_match(matches: List[Dict[str, Any]], auto_select: bool) -> Optional[Dict[str, Any]]:
    if not matches:
        return None

    if auto_select and len(matches) == 1:
        return matches[0]

    print()
    print("Matching Steam apps:")
    print()

    for index, match in enumerate(matches, start=1):
        print(f"{index}. {match['name']} | app_id={match['app_id']}")

    print()
    choice = input("Choose a number to add, or press Enter to cancel: ").strip()

    if not choice:
        return None

    if not choice.isdigit():
        raise ValueError("Choice must be a number")

    choice_number = int(choice)

    if choice_number < 1 or choice_number > len(matches):
        raise ValueError("Choice is out of range")

    return matches[choice_number - 1]


def parse_keywords(value: Optional[str]) -> List[str]:
    if not value:
        return DEFAULT_ALERT_KEYWORDS

    keywords = [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ]

    return keywords or DEFAULT_ALERT_KEYWORDS


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Search Steam apps and add a game to the Steam ingestion watchlist."
    )

    parser.add_argument(
        "query",
        help="Game name to search for, for example: 'Counter-Strike 2'",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of search results to show. Default: 10",
    )

    parser.add_argument(
        "--disabled",
        action="store_true",
        help="Add the game as disabled instead of enabled.",
    )

    parser.add_argument(
        "--keywords",
        default=None,
        help="Comma-separated alert keywords. Example: security,exploit,ranked,balance",
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Automatically select the only match if exactly one match is found.",
    )

    args = parser.parse_args()

    matches = search_steam_apps(args.query, args.limit)

    if not matches:
        print(f"No Steam apps found matching: {args.query}")
        return 1

    selected = choose_match(matches, auto_select=args.yes)

    if selected is None:
        print("Cancelled. No game added.")
        return 0

    watchlist = load_watchlist()

    add_game_to_watchlist(
        watchlist=watchlist,
        app_id=selected["app_id"],
        name=selected["name"],
        enabled=not args.disabled,
        alert_keywords=parse_keywords(args.keywords),
    )

    save_watchlist(watchlist)

    print()
    print("Added game to watchlist:")
    print(f"- name: {selected['name']}")
    print(f"- app_id: {selected['app_id']}")
    print(f"- file: {WATCHLIST_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())