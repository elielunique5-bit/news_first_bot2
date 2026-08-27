"""
Bot d'analyse des annonces économiques - Stratégie news-first (v3)
====================================================================
- Fuseau horaire : TOUTES les heures affichées sont converties en
  heure de Kinshasa (Africa/Kinshasa), quelle que soit la source du feed.
- SEUIL_ALERTE : lecture protégée (env_int), ne plante plus si la
  variable GitHub est vide ou absente.
- Deux modes d'exécution, pilotés par RUN_MODE (fixé par le workflow) :
    RUN_MODE=briefing -> envoie le récap complet du biais cumulé
                          (normalement 1x/jour, ~6h Kinshasa)
    RUN_MODE=watch     -> ne fait QUE vérifier les publications fraîches
                          (tourne toutes les 15 min, pas de spam)
- Anti-doublon :
    - state["alerted"]           -> events déjà notifiés individuellement
    - state["last_briefing_date"] -> empêche d'envoyer 2 briefings le même jour
      même si le cron se déclenche deux fois ou qu'on relance manuellement.

Secrets requis dans le repo GitHub :
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID

Variables optionnelles (Settings > Secrets and variables > Actions > Variables) :
    SEUIL_ALERTE (def: 5)
    ENVOYER_MEME_SANS_ALERTE (def: true)
"""

import os
import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------

TZ = ZoneInfo("Africa/Kinshasa")                    # fuseau d'affichage cible
SOURCE_TZ_FALLBACK = ZoneInfo("America/New_York")   # fuseau du feed si pas d'offset explicite

MARKETS = {
    "USD": "New York",
    "EUR": "Londres",
    "GBP": "Londres",
    "JPY": "Tokyo / Hong Kong",
    "CNY": "Hong Kong",
    "HKD": "Hong Kong",
    "AUD": "Sydney / Hong Kong",
    "CHF": "Londres / Zurich",
}

ASSET_MAP = {
    "USD": ["DXY", "XAUUSD", "indices US", "BTC/ETH (indirect)"],
    "EUR": ["EURUSD", "DXY (inverse)"],
    "GBP": ["GBPUSD", "GBPJPY"],
    "JPY": ["USDJPY", "GBPJPY", "XAUJPY"],
    "CNY": ["indices asiatiques", "AUDUSD (proxy Chine)"],
    "HKD": ["indices Hong Kong", "USDHKD"],
    "AUD": ["AUDUSD", "AUDJPY"],
    "CHF": ["USDCHF", "XAUUSD (refuge)"],
}

NIVEAU_1_KEYWORDS = [
    "interest rate", "rate decision", "fomc", "cpi", "core cpi",
    "non-farm", "nfp", "gdp", "press conference",
    "monetary policy statement", "boj", "ecb", "boe", "pboc", "rba",
]

NIVEAU_2_KEYWORDS = [
    "pmi", "retail sales", "jobless claims", "unemployment", "ppi",
    "trade balance", "industrial production", "consumer confidence",
    "speech", "speaks", "housing", "durable goods",
]

FOREX_FACTORY_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
STATE_FILE = "state/sent_events.json"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

RUN_MODE = os.environ.get("RUN_MODE", "briefing").strip().lower()  # "briefing" ou "watch"

PUBLISH_WINDOW_MIN = 20   # tolérance pour détecter "vient de sortir"
WATCH_HOURS = 36          # fenêtre d'anticipation avant publication


def env_int(name, default):
    """Lit une variable d'env entière, en gérant le cas vide/absent (bug corrigé)."""
    val = os.environ.get(name, "")
    if val is None or str(val).strip() == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default


def env_bool(name, default):
    val = os.environ.get(name, "")
    if val is None or str(val).strip() == "":
        return default
    return str(val).strip().lower() == "true"


SEUIL_ALERTE = env_int("SEUIL_ALERTE", 5)
ENVOYER_MEME_SANS_ALERTE = env_bool("ENVOYER_MEME_SANS_ALERTE", True)


# ---------------------------------------------------------------
# RÉCUPÉRATION ET PARSING
# ---------------------------------------------------------------

def fetch_calendar():
    try:
        resp = requests.get(FOREX_FACTORY_URL, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Erreur de récupération du calendrier: {e}")
        return []


def classify_event(title):
    t = title.lower()
    if any(kw in t for kw in NIVEAU_1_KEYWORDS):
        return 1
    if any(kw in t for kw in NIVEAU_2_KEYWORDS):
        return 2
    return None


def parse_datetime(raw_date):
    """Parse la date du feed et la convertit TOUJOURS en heure Kinshasa."""
    dt = None
    try:
        dt = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        try:
            dt_naive = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S")
            dt = dt_naive.replace(tzinfo=SOURCE_TZ_FALLBACK)
        except ValueError:
            return None
    return dt.astimezone(TZ)


def parse_num(v):
    """Nettoie '0.9%', '9.5K', '-100.8B' etc. vers un float."""
    if v is None or v == "":
        return None
    s = str(v).strip().replace("%", "").replace(",", "")
    mult = 1
    if s.endswith("K"):
        mult, s = 1e3, s[:-1]
    elif s.endswith("M"):
        mult, s = 1e6, s[:-1]
    elif s.endswith("B"):
        mult, s = 1e9, s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return None


def parse_events(raw_events):
    parsed = []
    for e in raw_events:
        impact = (e.get("impact") or "").lower()
        currency = e.get("country", "")
        title = e.get("title", "")

        if impact != "high":
            continue
        if currency not in MARKETS:
            continue

        niveau = classify_event(title)
        if niveau is None:
            niveau = 2

        dt = parse_datetime(e.get("date", ""))  # <- toujours en heure Kinshasa

        parsed.append({
            "id": f"{currency}_{title}_{e.get('date','')}",
            "titre": title,
            "devise": currency,
            "place": MARKETS.get(currency, "?"),
            "niveau": niveau,
            "datetime": dt,
            "actual": e.get("actual"),
            "forecast": e.get("forecast"),
            "previous": e.get("previous"),
        })
    return parsed


# ---------------------------------------------------------------
# TABLEAU DE SCÉNARIOS (neutre, sans thèse personnelle)
# ---------------------------------------------------------------

def build_scenario_table(event):
    f, p = parse_num(event["forecast"]), parse_num(event["previous"])
    if f is None or p is None:
        return None

    ecart = f - p
    step = abs(ecart) if abs(ecart) > 1e-9 else max(abs(f) * 0.5, 0.1)
    trend_up = ecart >= 0

    if trend_up:
        forte = f + step * 0.5
        inverse = p - step * 0.3
    else:
        forte = f - step * 0.5
        inverse = p + step * 0.3

    return {"trend_up": trend_up, "consensus": f, "forte": forte, "inverse": inverse}


def classify_actual(event, table):
    a = parse_num(event["actual"])
    if a is None or table is None:
        return None
    f = table["consensus"]
    tol = max(abs(f) * 0.08, 0.05)
    if abs(a - f) <= tol:
        return "Conforme au consensus"
    if (table["trend_up"] and a > f) or (not table["trend_up"] and a < f):
        return "Confirmation forte"
    if (table["trend_up"] and a <= table["inverse"]) or (not table["trend_up"] and a >= table["inverse"]):
        return "Surprise inverse"
    return "Entre consensus et confirmation"


def format_scenario_table(table, unite=""):
    if table is None:
        return "  (pas de scénario chiffré disponible pour cet event)"
    fleche = "↑" if table["trend_up"] else "↓"
    return "\n".join([
        f"  1) Conforme au consensus     : ~{table['consensus']:.2f}{unite}",
        f"  2) Confirmation forte {fleche}      : au-delà de {table['forte']:.2f}{unite}",
        f"  3) Surprise inverse           : retour vers/au-delà de {table['inverse']:.2f}{unite}",
    ])


# ---------------------------------------------------------------
# ANALYSE GLOBALE (score de biais cumulé)
# ---------------------------------------------------------------

def determine_direction(event):
    a, f = parse_num(event.get("actual")), parse_num(event.get("forecast"))
    if a is None or f is None:
        return "en attente de publication"
    if a > f:
        return "au-dessus des attentes"
    elif a < f:
        return "en-dessous des attentes"
    return "conforme aux attentes"


def build_bias_score(events, days_window=4):
    now = datetime.now(TZ)
    cutoff = now - timedelta(days=days_window)

    scores = defaultdict(int)
    details = defaultdict(list)

    for e in events:
        if e["datetime"] is None or not (cutoff <= e["datetime"] <= now):
            continue
        weight = 3 if e["niveau"] == 1 else 1
        direction = determine_direction(e)
        if direction == "au-dessus des attentes":
            scores[e["devise"]] += weight
        elif direction == "en-dessous des attentes":
            scores[e["devise"]] -= weight
        details[e["devise"]].append((e["titre"], direction, e["niveau"]))

    return scores, details


def upcoming_events(events, hours_ahead=WATCH_HOURS):
    now = datetime.now(TZ)
    limit = now + timedelta(hours=hours_ahead)
    up = [e for e in events if e["datetime"] and now <= e["datetime"] <= limit and not e.get("actual")]
    up.sort(key=lambda x: x["datetime"])
    return up


def just_published(events, state, window_min=PUBLISH_WINDOW_MIN):
    """Events dont l'actual est rempli, pas encore alertés."""
    now = datetime.now(TZ)
    fresh = []
    for e in events:
        if not e.get("actual") or e["datetime"] is None:
            continue
        if e["id"] in state.get("alerted", []):
            continue
        delta_min = abs((now - e["datetime"]).total_seconds()) / 60
        if delta_min <= window_min or e["datetime"] <= now:
            fresh.append(e)
    return fresh


# ---------------------------------------------------------------
# ÉTAT (anti-doublon : publications individuelles + briefing quotidien)
# ---------------------------------------------------------------

def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"alerted": [], "last_briefing_date": ""}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    state["alerted"] = state.get("alerted", [])[-300:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------
# MESSAGES TELEGRAM
# ---------------------------------------------------------------

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID manquant. Message non envoyé:")
        print(message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [message[i:i + 4000] for i in range(0, len(message), 4000)]
    for chunk in chunks:
        resp = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",
        })
        if resp.status_code != 200:
            print(f"Erreur envoi Telegram: {resp.status_code} - {resp.text}")


def build_publication_alert(event):
    table = build_scenario_table(event)
    resultat = classify_actual(event, table)

    lines = [f"🔔 <b>PUBLICATION — {event['titre']} ({event['devise']})</b>"]
    lines.append(f"Heure: {event['datetime'].strftime('%a %d/%m %Hh%M')} (Kinshasa)")
    lines.append(f"Actual: {event.get('actual','n/a')} | Forecast: {event.get('forecast','n/a')} | Previous: {event.get('previous','n/a')}")
    lines.append("")
    lines.append("<b>Scénarios (référence, calculés avant publication)</b>")
    lines.append(format_scenario_table(table))
    if resultat:
        lines.append(f"\n<b>→ Scénario réalisé : {resultat}</b>")
    lines.append(f"\n<b>Actifs ciblés :</b> {', '.join(ASSET_MAP.get(event['devise'], ['-']))}")
    return "\n".join(lines)


def build_daily_briefing(events):
    scores, details = build_bias_score(events)
    fortes = {d: s for d, s in scores.items() if abs(s) >= SEUIL_ALERTE}
    faibles = {d: s for d, s in scores.items() if abs(s) < SEUIL_ALERTE}

    lines = []
    if fortes:
        lines.append(f"<b>🔴 BIAIS FORT (seuil {SEUIL_ALERTE} dépassé)</b>")
        for devise, score in sorted(fortes.items(), key=lambda x: -abs(x[1])):
            tendance = "HAUSSIER" if score > 0 else "BAISSIER"
            lines.append(f"\n<b>{devise}</b> ({MARKETS.get(devise)}) — score {score:+d} → {tendance} CONFIRMÉ")
            lines.append(f"Actifs: {', '.join(ASSET_MAP.get(devise, ['-']))}")
            for titre, direction, niveau in details[devise]:
                lines.append(f"  [N{niveau}] {titre} → {direction}")
    else:
        lines.append(f"<b>Aucun biais n'a dépassé le seuil ({SEUIL_ALERTE}) sur les 4 derniers jours.</b>")
        lines.append("Pas de conviction suffisante pour trader sur base du narratif — patience.")

    if faibles:
        lines.append(f"\n<b>Biais sous le seuil (à surveiller)</b>")
        for devise, score in sorted(faibles.items(), key=lambda x: -abs(x[1])):
            tendance = "haussier" if score > 0 else "baissier" if score < 0 else "neutre"
            lines.append(f"  {devise}: {score:+d} ({tendance})")

    lines.append(f"\n<b>À VENIR — prochaines {WATCH_HOURS}h (heure Kinshasa)</b>")
    up = upcoming_events(events)
    if not up:
        lines.append("Rien de prévu à fort impact, pas encore publié.")
    else:
        for e in up:
            table = build_scenario_table(e)
            date_str = e["datetime"].strftime("%a %d/%m %Hh%M")
            marqueur = " ⚠ devise déjà en biais fort" if e["devise"] in fortes else ""
            lines.append(f"\n[N{e['niveau']}] {e['titre']} ({e['devise']} - {e['place']}){marqueur}")
            lines.append(f"Prévu: {date_str} | Forecast: {e.get('forecast','n/a')} | Previous: {e.get('previous','n/a')}")
            lines.append(format_scenario_table(table))
            lines.append(f"Actifs à surveiller: {', '.join(ASSET_MAP.get(e['devise'], ['-']))}")

    return "\n".join(lines)


# ---------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------

def main():
    print(f"Mode d'exécution : {RUN_MODE}")
    print("Récupération du calendrier économique...")
    raw = fetch_calendar()
    if not raw:
        if RUN_MODE == "briefing":
            send_telegram("Bot news-first: impossible de récupérer le calendrier aujourd'hui.")
        return

    events = parse_events(raw)
    state = load_state()

    fresh = just_published(events, state)
    for e in fresh:
        msg = build_publication_alert(e)
        print(msg)
        send_telegram(msg)
        state.setdefault("alerted", []).append(e["id"])

    if RUN_MODE == "briefing":
        today_str = datetime.now(TZ).strftime("%Y-%m-%d")
        if state.get("last_briefing_date") == today_str:
            print("Briefing déjà envoyé aujourd'hui — on ne renvoie pas de doublon.")
        else:
            briefing = build_daily_briefing(events)
            print(briefing)
            if ENVOYER_MEME_SANS_ALERTE or "🔴" in briefing:
                send_telegram(briefing)
            state["last_briefing_date"] = today_str
    else:
        print("Mode watch : pas de briefing complet, uniquement les publications fraîches ci-dessus.")

    save_state(state)


if __name__ == "__main__":
    main()
