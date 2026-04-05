import asyncio
import os
import re
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeDefault, Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, LabeledPrice
from telegram.error import BadRequest
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes, PreCheckoutQueryHandler,
)
from rapidfuzz import process, fuzz
import database as db

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
PAYMENT_TOKEN = os.getenv("PAYMENT_TOKEN", "")  # YooKassa / Sberbank token from BotFather
# Admins from .env are permanent super-admins; additional admins stored in DB
_ENV_ADMINS: set[int] = set(
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
)
_db_admins: set[int] = set()


def refresh_admins() -> None:
    global _db_admins
    _db_admins = db.get_admin_ids()


def is_admin(user_id: int) -> bool:
    return user_id in _ENV_ADMINS or user_id in _db_admins


# ---------------------------------------------------------------------------
# Subscription plans
# ---------------------------------------------------------------------------

_PLAN_META = {
    "1m": {"label": "1 месяц",   "months": 1, "discount": 0},
    "3m": {"label": "3 месяца",  "months": 3, "discount": 10},
    "6m": {"label": "6 месяцев", "months": 6, "discount": 15},
}

# Conversation states for /subprice
SP_PICK_PLAN, SP_ENTER_RUB, SP_ENTER_STARS = range(90, 93)


def _get_plans() -> dict:
    """Returns merged plan dict with current DB prices."""
    prices = db.get_plan_prices()
    result = {}
    for key, meta in _PLAN_META.items():
        rub, stars = prices[key]
        result[key] = {**meta, "price_rub": rub, "price_stars": stars}
    return result


def _fmt_price(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _show_plans(msg) -> None:
    plans = _get_plans()
    p1 = plans["1m"]

    lines = ["🏠 *Подписка на базу ЖК*\n"]
    for key, p in plans.items():
        label   = p["label"]
        rub     = _fmt_price(p["price_rub"])
        stars   = _fmt_price(p["price_stars"])
        disc    = p["discount"]
        base_rub = _fmt_price(p["months"] * p1["price_rub"])

        if disc:
            lines.append(
                f"*{label}* — {rub} ₽  |  {stars} ⭐\n"
                f"   ~~{base_rub} ₽~~ → скидка {disc}% 🔥"
            )
        else:
            lines.append(f"*{label}* — {rub} ₽  |  {stars} ⭐")

    lines.append("\nВыберите тариф 👇")

    keyboard = []
    for key, p in plans.items():
        disc_tag = f" 🔥 -{p['discount']}%" if p["discount"] else ""
        keyboard.append([InlineKeyboardButton(
            f"{p['label']} — {_fmt_price(p['price_rub'])} ₽{disc_tag}",
            callback_data=f"sub|{key}",
        )])

    await msg.reply_text("\n".join(lines), parse_mode="Markdown",
                         reply_markup=InlineKeyboardMarkup(keyboard))


async def _require_sub(update: Update) -> bool:
    """Returns True if user may proceed; sends paywall message otherwise."""
    user_id = update.effective_user.id
    if is_admin(user_id) or db.get_active_subscription(user_id):
        return True
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Оформить подписку", callback_data="sub_plans"),
    ]])
    text = "🔒 Для доступа к базе ЖК необходима подписка.\n\nНажмите кнопку ниже, чтобы выбрать тариф."
    if update.message:
        await update.message.reply_text(text, reply_markup=keyboard)
    elif update.callback_query:
        await update.callback_query.message.reply_text(text, reply_markup=keyboard)
    return False


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

_index: dict[str, dict] = {}


def refresh_index() -> None:
    global _index
    _index = db.build_search_index(db.get_all_apartments())


def find_apartment(query: str) -> dict | None:
    q = query.strip().upper()
    for prefix in ("ЖК ", "ЖК. ", "ЖК-"):
        if q.startswith(prefix):
            q = q[len(prefix):]
            break
    if not _index:
        return None
    result = process.extractOne(q, list(_index.keys()), scorer=fuzz.WRatio, score_cutoff=60)
    return _index[result[0]] if result else None


def is_valid_url(url: str) -> bool:
    return bool(url) and url.startswith(("http://", "https://"))


def slugify(name: str) -> str:
    slug = name.lower()
    slug = re.sub(r"[^a-zа-я0-9]", "_", slug)
    slug = re.sub(r"_+", "_", slug)
    return slug.strip("_")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt(n: float) -> str:
    return f"{int(n):,}".replace(",", " ")


def parse_layouts(text: str) -> list[dict] | None:
    result = []
    for part in text.split(","):
        tokens = part.strip().rsplit(None, 1)
        if len(tokens) != 2:
            return None
        name, area_s = tokens
        try:
            result.append({"name": name.strip(), "area": float(area_s.replace(",", "."))})
        except ValueError:
            return None
    return result or None


def parse_kv_list(text: str) -> list[tuple[float, float]] | None:
    result = []
    for part in text.split(","):
        part = part.strip()
        if ":" not in part:
            return None
        a, b = part.split(":", 1)
        try:
            result.append((float(a.strip()), float(b.strip())))
        except ValueError:
            return None
    return result or None


def parse_floor_prices(text: str) -> tuple[int, list | None]:
    """
    Accepts:  "150000"  OR  "1-5:150000, 6-10:160000"
    Returns:  (min_price, floor_prices_list_or_None)
              (0, None) on parse error
    """
    text = text.strip()
    if ":" in text:
        parts = [p.strip() for p in text.split(",")]
        floor_prices = []
        for part in parts:
            if ":" not in part:
                return 0, None
            rng, price_s = part.rsplit(":", 1)
            price_s = price_s.strip().replace(" ", "")
            if not price_s.isdigit():
                return 0, None
            floor_prices.append({"range": rng.strip(), "price": int(price_s)})
        if not floor_prices:
            return 0, None
        return min(fp["price"] for fp in floor_prices), floor_prices
    else:
        raw = text.replace(" ", "")
        if not raw.isdigit():
            return 0, None
        return int(raw), None


def find_discount_per_sqm(discounts: list[dict], down_pct: float) -> float:
    """Returns rub/m² price reduction applicable for given down_pct."""
    applicable = 0.0
    for d in sorted(discounts, key=lambda x: x["from_pct"]):
        if down_pct >= d["from_pct"]:
            applicable = d.get("discount_per_sqm", 0)
    return applicable


def find_discount_for_amount(discounts: list[dict], down_amount: float,
                              area: float, base_price_per_sqm: float) -> float:
    """Returns rub/m² discount applicable when paying a fixed ruble amount as down payment.
    Correctly checks against the DISCOUNTED total (not original price)."""
    best = 0.0
    for d in sorted(discounts, key=lambda x: x["from_pct"]):
        disc = d.get("discount_per_sqm", 0)
        threshold_pct = d["from_pct"]
        discounted_total = area * (base_price_per_sqm - disc)
        min_required = discounted_total * threshold_pct / 100
        if down_amount >= min_required:
            best = disc
    return best


def _skip_btn(callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data=callback)]])


# ---------------------------------------------------------------------------
# User handlers
# ---------------------------------------------------------------------------

async def start(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Добро пожаловать!\n\n"
        "Напишите название ЖК — я найду его даже с опечаткой.\n\n"
        "Например: АН НУР, Классика, анур...\n\n"
        "Для доступа к базе нужна подписка — /subscribe"
    )


# ---------------------------------------------------------------------------
# Subscription handlers
# ---------------------------------------------------------------------------

async def subscribe_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_admin(user_id):
        await update.message.reply_text("У вас бесплатный доступ как у администратора.")
        return
    sub = db.get_active_subscription(user_id)
    if sub:
        expires = datetime.fromisoformat(sub["expires_at"]).strftime("%d.%m.%Y")
        await update.message.reply_text(
            f"Подписка активна (тариф «{sub['plan']}»).\n"
            f"Действует до: *{expires}*",
            parse_mode="Markdown",
        )
        return
    await _show_plans(update.message)


async def sub_plans_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    await _show_plans(cq.message)


async def sub_plan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """User selected a plan — show payment method choice."""
    cq = update.callback_query
    await cq.answer()
    plan_key = cq.data.split("|", 1)[1]
    plans = _get_plans()
    plan = plans.get(plan_key)
    if not plan:
        return
    rub   = _fmt_price(plan["price_rub"])
    stars = _fmt_price(plan["price_stars"])
    text  = f"Выбран тариф: *{plan['label']}*\n\nВыберите способ оплаты:"
    keyboard = [[InlineKeyboardButton(f"Telegram Stars — {stars} ⭐", callback_data=f"subpay|{plan_key}|stars")]]
    if PAYMENT_TOKEN:
        keyboard.insert(0, [InlineKeyboardButton(f"Картой (RUB) — {rub} ₽", callback_data=f"subpay|{plan_key}|rub")])
    await cq.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def sub_pay_method_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send invoice based on chosen plan + payment method."""
    cq = update.callback_query
    await cq.answer()
    _, plan_key, method = cq.data.split("|")
    plans = _get_plans()
    plan = plans.get(plan_key)
    if not plan:
        return
    if method == "rub":
        await context.bot.send_invoice(
            chat_id=cq.from_user.id,
            title=f"Подписка — {plan['label']}",
            description=f"Доступ к базе ЖК на {plan['label']}",
            payload=f"sub|{plan_key}|rub",
            provider_token=PAYMENT_TOKEN,
            currency="RUB",
            prices=[LabeledPrice(label=plan["label"], amount=plan["price_rub"] * 100)],
        )
    else:
        await context.bot.send_invoice(
            chat_id=cq.from_user.id,
            title=f"Подписка — {plan['label']}",
            description=f"Доступ к базе ЖК на {plan['label']}",
            payload=f"sub|{plan_key}|stars",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=plan["label"], amount=plan["price_stars"])],
        )


async def precheckout_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.pre_checkout_query.answer(ok=True)


async def successful_payment_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    payment = update.message.successful_payment
    parts    = payment.invoice_payload.split("|")
    plan_key = parts[1] if len(parts) > 1 else "1m"
    plans    = _get_plans()
    plan     = plans.get(plan_key, plans["1m"])
    user_id  = update.effective_user.id
    expires_at = datetime.utcnow() + timedelta(days=30 * plan["months"])
    db.save_subscription(user_id, plan["label"], expires_at.isoformat(), payment.telegram_payment_charge_id)
    await update.message.reply_text(
        f"Оплата прошла!\n"
        f"Подписка *{plan['label']}* активна до *{expires_at.strftime('%d.%m.%Y')}*.",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# Admin: subscription management
# ---------------------------------------------------------------------------

async def subscribers_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    subs = db.get_all_active_subscriptions()
    if not subs:
        await update.message.reply_text("Нет активных подписчиков.")
        return
    lines = ["*Активные подписчики:*\n"]
    for s in subs:
        exp = datetime.fromisoformat(s["expires_at"]).strftime("%d.%m.%Y")
        lines.append(f"• ID `{s['user_id']}` — {s['plan']} — до {exp}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def delsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args or not args[0].lstrip("-").isdigit():
        await update.message.reply_text("Использование: /delsub <user_id>")
        return
    user_id = int(args[0])
    db.delete_subscription(user_id)
    await update.message.reply_text(f"Подписка пользователя `{user_id}` удалена.", parse_mode="Markdown")


async def addsub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2 or not args[0].lstrip("-").isdigit() or not args[1].isdigit():
        await update.message.reply_text("Использование: /addsub <user_id> <дней>")
        return
    user_id, days = int(args[0]), int(args[1])
    db.extend_subscription(user_id, days)
    sub = db.get_active_subscription(user_id)
    exp = datetime.fromisoformat(sub["expires_at"]).strftime("%d.%m.%Y") if sub else "?"
    await update.message.reply_text(
        f"Подписка пользователя `{user_id}` продлена на {days} дн.\nАктивна до: *{exp}*",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# Admin: /subprice — edit plan prices
# ---------------------------------------------------------------------------

async def subprice_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        return ConversationHandler.END
    plans = _get_plans()
    lines = ["*Текущие цены тарифов:*\n"]
    for key, p in plans.items():
        lines.append(f"• {p['label']}: {_fmt_price(p['price_rub'])} ₽ / {_fmt_price(p['price_stars'])} ⭐")
    lines.append("\nВыберите тариф для изменения:")
    keyboard = [[InlineKeyboardButton(p["label"], callback_data=f"sp|{key}")] for key, p in plans.items()]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="sp|cancel")])
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                    reply_markup=InlineKeyboardMarkup(keyboard))
    return SP_PICK_PLAN


async def sp_pick_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "sp|cancel":
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    plan_key = cq.data.split("|", 1)[1]
    plans = _get_plans()
    if plan_key not in plans:
        return ConversationHandler.END
    context.user_data["sp_plan_key"] = plan_key
    await cq.edit_message_text(
        f"Тариф: *{plans[plan_key]['label']}*\n\nВведите новую цену в рублях:",
        parse_mode="Markdown",
    )
    return SP_ENTER_RUB


async def sp_enter_rub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().replace(" ", "")
    if not text.isdigit():
        await update.message.reply_text("Введите целое число (цена в рублях):")
        return SP_ENTER_RUB
    context.user_data["sp_price_rub"] = int(text)
    await update.message.reply_text(
        "Введите цену в Telegram Stars,\n"
        "или отправьте *=* чтобы Stars = рублям:",
        parse_mode="Markdown",
    )
    return SP_ENTER_STARS


async def sp_enter_stars(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().replace(" ", "")
    if text == "=":
        price_stars = context.user_data["sp_price_rub"]
    elif text.isdigit():
        price_stars = int(text)
    else:
        await update.message.reply_text("Введите целое число или *=*:", parse_mode="Markdown")
        return SP_ENTER_STARS
    plan_key   = context.user_data["sp_plan_key"]
    price_rub  = context.user_data["sp_price_rub"]
    db.update_plan_prices(plan_key, price_rub, price_stars)
    plans = _get_plans()
    await update.message.reply_text(
        f"Цена тарифа *{plans[plan_key]['label']}* обновлена:\n"
        f"• Карта: *{_fmt_price(price_rub)} ₽*\n"
        f"• Stars: *{_fmt_price(price_stars)} ⭐*",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def subprice_cancel(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


async def _send_apt_card(msg, apt: dict, admin_user: bool) -> None:
    """Send one apartment card as a photo (or text fallback)."""
    floor_prices = apt.get("floor_prices")
    if floor_prices and len(floor_prices) > 1:
        price_lines = "\n".join(f"  {fp['range']} эт.: {fmt(fp['price'])} руб/м²" for fp in floor_prices)
        price_display = f"от {fmt(apt['price_per_sqm'])} руб/м²\n{price_lines}"
    else:
        price_display = f"{fmt(apt['price_per_sqm'])} руб/м²"

    inst_price = apt.get("installment_price_per_sqm")
    inst_line  = f"\nВ рассрочку: {fmt(inst_price)} руб/м²" if inst_price else ""
    caption = f"*{apt['name']}*\n\nАдрес: {apt['address']}\nЦена за м²: {price_display}{inst_line}"
    if apt.get("description"):
        caption += f"\n\n{apt['description']}"

    keyboard = []
    file_ids = apt.get("photos_file_ids") or []
    has_url  = is_valid_url(apt.get("photos_url", ""))
    if file_ids:
        keyboard.append([InlineKeyboardButton("Все фото 📷", callback_data=f"aptphotos|{apt['id']}")])
    elif has_url:
        keyboard.append([InlineKeyboardButton("Все фото",   url=apt["photos_url"])])
    if is_valid_url(apt["layouts_url"]):
        keyboard.append([InlineKeyboardButton("Планировки", url=apt["layouts_url"])])
    if is_valid_url(apt["chess_url"]):
        keyboard.append([InlineKeyboardButton("Шахматка",   url=apt["chess_url"])])
    keyboard.append([InlineKeyboardButton("Условия рассрочки", callback_data=f"inst|{apt['id']}")])
    if db.get_calculator(apt["id"]):
        keyboard.append([InlineKeyboardButton("Калькулятор рассрочки", callback_data=f"calc|{apt['id']}")])

    markup = InlineKeyboardMarkup(keyboard)
    try:
        await msg.reply_photo(
            photo=apt["main_photo"], caption=caption, parse_mode="Markdown", reply_markup=markup,
        )
    except BadRequest:
        hint = "\n\n_(фото недоступно — обновите через /edit)_" if admin_user else ""
        await msg.reply_text(caption + hint, parse_mode="Markdown", reply_markup=markup)


async def search_apartment(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_sub(update):
        return
    query = update.message.text.strip()
    apt = find_apartment(query)
    if not apt:
        await update.message.reply_text(f'ЖК по запросу "{query}" не найден.\nПопробуйте уточнить название.')
        return
    await _send_apt_card(update.message, apt, is_admin(update.effective_user.id))


async def installment_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    apt_id = cq.data.split("|", 1)[1]
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not apt:
        await cq.message.reply_text("Информация не найдена.")
        return
    text = f"*Условия рассрочки — {apt['name']}*\n\n{apt.get('installment_text', 'Уточняйте у менеджера.')}"
    await cq.message.reply_text(text, parse_mode="Markdown")


async def aptphotos_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    apt_id  = cq.data.split("|", 1)[1]
    apt     = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not apt:
        await cq.message.reply_text("Информация не найдена.")
        return
    file_ids = apt.get("photos_file_ids") or []
    url      = apt.get("photos_url", "")
    if file_ids:
        media = [InputMediaPhoto(fid) for fid in file_ids]
        await cq.message.reply_media_group(media)
    if is_valid_url(url):
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Ссылка на галерею 🔗", url=url)]])
        await cq.message.reply_text("Дополнительная ссылка:", reply_markup=kb)
    elif not file_ids:
        await cq.message.reply_text("Фото галереи не добавлены.")


# ---------------------------------------------------------------------------
# Admin: /list
# ---------------------------------------------------------------------------

async def list_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_sub(update):
        return
    apts = db.get_all_apartments()
    if not apts:
        await update.message.reply_text("Список ЖК пуст.")
        return
    admin = is_admin(update.effective_user.id)
    lines = []
    for i, a in enumerate(apts, 1):
        if admin:
            has_calc = "🧮" if db.get_calculator(a["id"]) else "  "
            lines.append(f"{i}. {has_calc} *{a['name']}* — от {fmt(a['price_per_sqm'])} руб/м²")
        else:
            lines.append(f"{i}. *{a['name']}* — от {fmt(a['price_per_sqm'])} руб/м²")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ---------------------------------------------------------------------------
# Admin: /addadmin  /removeadmin  /listadmins
# ---------------------------------------------------------------------------

_ADMIN_COMMANDS = [
    BotCommand("start",           "Начать / помощь"),
    BotCommand("subscribe",       "Статус подписки"),
    BotCommand("post",            "Добавить новый ЖК"),
    BotCommand("edit",            "Редактировать ЖК"),
    BotCommand("delete",          "Удалить ЖК"),
    BotCommand("calc",            "Настроить калькулятор"),
    BotCommand("setcount",        "Изменить количество квартир в планировке"),
    BotCommand("list",            "Список всех ЖК"),
    BotCommand("browse",          "Подбор ЖК по районам"),
    BotCommand("adddistrict",     "Добавить район"),
    BotCommand("removedistrict",  "Удалить район"),
    BotCommand("listdistricts",   "Список районов"),
    BotCommand("addadmin",        "Добавить администратора"),
    BotCommand("removeadmin",     "Удалить администратора"),
    BotCommand("listadmins",      "Список администраторов"),
    BotCommand("subscribers",     "Список активных подписчиков"),
    BotCommand("addsub",          "Выдать подписку: /addsub <id> <дней>"),
    BotCommand("delsub",          "Удалить подписку: /delsub <id>"),
    BotCommand("subprice",        "Изменить цены тарифов"),
    BotCommand("cancel",          "Отменить текущее действие"),
]


async def _set_admin_commands(bot, user_id: int) -> None:
    try:
        await bot.set_my_commands(_ADMIN_COMMANDS, scope=BotCommandScopeChat(chat_id=user_id))
    except BadRequest as e:
        logger.warning("Could not set commands for admin %d: %s", user_id, e)


async def addadmin_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in _ENV_ADMINS:
        await update.message.reply_text("Только главный администратор может добавлять других.")
        return
    parts = update.message.text.split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await update.message.reply_text("Использование: /addadmin 123456789\n\nЧтобы узнать свой ID — напишите боту @userinfobot")
        return
    user_id = int(parts[1])
    db.add_admin(user_id)
    refresh_admins()
    await _set_admin_commands(_context.bot, user_id)
    await update.message.reply_text(f"Администратор {user_id} добавлен.")


async def removeadmin_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in _ENV_ADMINS:
        await update.message.reply_text("Только главный администратор может удалять других.")
        return
    parts = update.message.text.split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await update.message.reply_text("Использование: /removeadmin 123456789")
        return
    user_id = int(parts[1])
    if user_id in _ENV_ADMINS:
        await update.message.reply_text("Нельзя удалить главного администратора.")
        return
    db.remove_admin(user_id)
    refresh_admins()
    await update.message.reply_text(f"Администратор {user_id} удалён.")


async def listadmins_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    lines = [f"Главные (из .env): {', '.join(str(i) for i in sorted(_ENV_ADMINS)) or 'нет'}"]
    extra = sorted(_db_admins - _ENV_ADMINS)
    lines.append(f"Дополнительные: {', '.join(str(i) for i in extra) or 'нет'}")
    await update.message.reply_text("\n".join(lines))


# ---------------------------------------------------------------------------
# Admin: /adddistrict  /removedistrict  /listdistricts
# ---------------------------------------------------------------------------

async def adddistrict_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    parts = update.message.text.split(None, 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text("Использование: /adddistrict Название района")
        return
    name = parts[1].strip()
    db.add_district(name)
    await update.message.reply_text(f"Район «{name}» добавлен.")


async def removedistrict_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    districts = db.get_all_districts()
    if not districts:
        await update.message.reply_text("Нет районов.")
        return
    keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"rmd|{d['id']}")] for d in districts]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="rmd_cancel")])
    await update.message.reply_text("Выберите район для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))


async def removedistrict_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "rmd_cancel":
        await cq.edit_message_text("Отменено.")
        return
    dist_id = int(cq.data.split("|", 1)[1])
    districts = db.get_all_districts()
    name = next((d["name"] for d in districts if d["id"] == dist_id), str(dist_id))
    db.remove_district(dist_id)
    await cq.edit_message_text(f"Район «{name}» удалён.")


async def listdistricts_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    districts = db.get_all_districts()
    if not districts:
        await update.message.reply_text("Нет районов.")
        return
    lines = [f"{i}. {d['name']}" for i, d in enumerate(districts, 1)]
    await update.message.reply_text("\n".join(lines))


# ---------------------------------------------------------------------------
# User + Admin: /browse — browse apartments by district
# ---------------------------------------------------------------------------

async def browse_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await _require_sub(update):
        return
    districts = db.get_all_districts()
    if not districts:
        await update.message.reply_text("Районы ещё не добавлены.")
        return
    keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"br_d|{d['id']}")] for d in districts]
    await update.message.reply_text("Выберите район:", reply_markup=InlineKeyboardMarkup(keyboard))


async def browse_district_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    dist_id = int(cq.data.split("|", 1)[1])
    districts = db.get_all_districts()
    dist_name = next((d["name"] for d in districts if d["id"] == dist_id), "Район")
    apts = db.get_apartments_by_district(dist_id)
    if not apts:
        await cq.edit_message_text(f"В районе «{dist_name}» нет ЖК.")
        return
    keyboard = [[InlineKeyboardButton(a["name"], callback_data=f"br_a|{a['id']}")] for a in apts]
    keyboard.append([InlineKeyboardButton("Назад к районам", callback_data="br_back")])
    await cq.edit_message_text(
        f"ЖК в районе «{dist_name}» — {len(apts)} шт.\n\nВыберите ЖК:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def browse_back_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    districts = db.get_all_districts()
    keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"br_d|{d['id']}")] for d in districts]
    await cq.edit_message_text("Выберите район:", reply_markup=InlineKeyboardMarkup(keyboard))


async def browse_apt_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    apt_id = cq.data.split("|", 1)[1]
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not apt:
        await cq.message.reply_text("ЖК не найден.")
        return
    await _send_apt_card(cq.message, apt, is_admin(cq.from_user.id))


# ---------------------------------------------------------------------------
# Admin: /delete
# ---------------------------------------------------------------------------

async def delete_cmd(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return
    apts = db.get_all_apartments()
    if not apts:
        await update.message.reply_text("Список ЖК пуст.")
        return
    keyboard = [[InlineKeyboardButton(a["name"], callback_data=f"del_ask|{a['id']}")] for a in apts]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="del_cancel")])
    await update.message.reply_text("Выберите ЖК для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))


async def del_ask_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    apt_id = cq.data.split("|", 1)[1]
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not apt:
        await cq.edit_message_text("ЖК не найден.")
        return
    keyboard = [
        [InlineKeyboardButton("Да, удалить", callback_data=f"del_yes|{apt_id}")],
        [InlineKeyboardButton("Отмена",       callback_data="del_cancel")],
    ]
    await cq.edit_message_text(
        f"Удалить ЖК *{apt['name']}*?\nЭто действие необратимо.",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def del_yes_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    apt_id = cq.data.split("|", 1)[1]
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    name = apt["name"] if apt else apt_id
    db.delete_apartment(apt_id)
    refresh_index()
    await cq.edit_message_text(f"ЖК *{name}* удалён.", parse_mode="Markdown")


async def del_cancel_callback(update: Update, _context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    await cq.edit_message_text("Удаление отменено.")


# ---------------------------------------------------------------------------
# Admin: /edit
# ---------------------------------------------------------------------------

EDIT_PICK_APT, EDIT_PICK_FIELD, EDIT_VALUE, EDIT_AFTER, EDIT_DISTRICT = range(20, 25)

EDITABLE_FIELDS = {
    "name":             "Название",
    "aliases":          "Псевдонимы (через запятую)",
    "district":         "Район",
    "address":          "Адрес",
    "price_per_sqm":             "Цена за м² (или по этажам)",
    "installment_price_per_sqm": "Цена за м² в рассрочку",
    "description":               "Описание",
    "main_photo":       "Главное фото",
    "photos_file_ids":  "Фото галереи (до 10 фото)",
    "photos_url":       "Ссылка — все фото",
    "layouts_url":      "Ссылка — планировки",
    "chess_url":        "Ссылка — шахматка",
    "installment_text": "Условия рассрочки",
}


async def edit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    apts = db.get_all_apartments()
    if not apts:
        await update.message.reply_text("Список ЖК пуст.")
        return ConversationHandler.END
    context.user_data.clear()
    keyboard = [[InlineKeyboardButton(a["name"], callback_data=f"ea|{a['id']}")] for a in apts]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="ecx")])
    await update.message.reply_text("Выберите ЖК:", reply_markup=InlineKeyboardMarkup(keyboard))
    return EDIT_PICK_APT


async def edit_pick_apt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "ecx":
        context.user_data.clear()
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    context.user_data["edit_apt_id"] = cq.data.split("|", 1)[1]
    return await _show_field_picker(cq, context)


async def _show_field_picker(cq, context) -> int:
    keyboard = [[InlineKeyboardButton(label, callback_data=f"ef|{key}")] for key, label in EDITABLE_FIELDS.items()]
    keyboard.append([InlineKeyboardButton("Готово", callback_data="ecx")])
    await cq.edit_message_text("Какое поле изменить?", reply_markup=InlineKeyboardMarkup(keyboard))
    return EDIT_PICK_FIELD


async def edit_pick_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "ecx":
        context.user_data.clear()
        await cq.edit_message_text("Редактирование завершено.")
        return ConversationHandler.END
    field = cq.data.split("|", 1)[1]
    context.user_data["edit_field"] = field
    label = EDITABLE_FIELDS[field]
    if field == "district":
        districts = db.get_all_districts()
        if not districts:
            await cq.edit_message_text("Нет районов. Сначала добавьте районы командой /adddistrict.")
            return EDIT_PICK_FIELD
        keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"ed_dist|{d['id']}")] for d in districts]
        keyboard.append([InlineKeyboardButton("Убрать район", callback_data="ed_dist|0")])
        keyboard.append([InlineKeyboardButton("Назад", callback_data="ed_dist_back")])
        await cq.edit_message_text("Выберите район:", reply_markup=InlineKeyboardMarkup(keyboard))
        return EDIT_DISTRICT
    if field == "photos_file_ids":
        context.user_data["edit_photos_list"] = []
        kb = [
            [InlineKeyboardButton("Готово (0 фото)", callback_data="edit_photos_done")],
            [InlineKeyboardButton("Очистить галерею", callback_data="edit_photos_clear")],
        ]
        await cq.edit_message_text(
            "Отправляйте фото по одному (до 10).\n"
            "Когда закончите — нажмите Готово.\n"
            "Очистить — удалить все существующие фото галереи.",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return EDIT_VALUE
    hints = {
        "main_photo":                "\nНажмите 📎 → Фото или видео → выберите фото из галереи",
        "aliases":                   "\n(через запятую, или - чтобы очистить)",
        "price_per_sqm":             "\nОдна цена: 150000\nПо этажам: 1-5:150000, 6-10:160000",
        "installment_price_per_sqm": "\nНапример: 165000\nВведите - чтобы убрать",
        "installment_text": (
            "\n\nЭто текстовое поле — пишите в свободной форме.\n"
            "Пример:\nРассрочка на 24 месяца.\nПервый взнос от 30%.\n"
            "Ежемесячный платёж без наценки."
        ),
    }
    await cq.edit_message_text(f"Введите новое значение для «{label}»:{hints.get(field, '')}")
    return EDIT_VALUE


async def edit_photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    import json as _json
    apt_id     = context.user_data["edit_apt_id"]
    file_ids   = [] if cq.data == "edit_photos_clear" else context.user_data.get("edit_photos_list", [])
    db.update_apartment_field(apt_id, "photos_file_ids", _json.dumps(file_ids, ensure_ascii=False))
    label = EDITABLE_FIELDS["photos_file_ids"]
    kb = [[
        InlineKeyboardButton("Изменить ещё поле", callback_data="edit_more"),
        InlineKeyboardButton("Готово",            callback_data="edit_done"),
    ]]
    await cq.edit_message_text(
        f"«{label}» обновлено ({len(file_ids)} фото).",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return EDIT_AFTER


async def edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    apt_id = context.user_data["edit_apt_id"]
    field  = context.user_data["edit_field"]

    if field == "photos_file_ids":
        if not update.message.photo:
            await update.message.reply_text("Нужно отправить фото.")
            return EDIT_VALUE
        file_ids = context.user_data.setdefault("edit_photos_list", [])
        if len(file_ids) >= 10:
            await update.message.reply_text("Максимум 10 фото. Нажмите Готово.")
            return EDIT_VALUE
        file_ids.append(update.message.photo[-1].file_id)
        n = len(file_ids)
        kb = [
            [InlineKeyboardButton(f"Готово ({n}/10 фото)", callback_data="edit_photos_done")],
            [InlineKeyboardButton("Очистить галерею",      callback_data="edit_photos_clear")],
        ]
        await update.message.reply_text(
            f"Фото {n}/10 добавлено. Отправляйте ещё или нажмите Готово:",
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return EDIT_VALUE

    if field == "main_photo":
        if not update.message.photo:
            await update.message.reply_text("Нужно отправить фото.\nНажмите 📎 → Фото или видео → выберите фото")
            return EDIT_VALUE
        value = update.message.photo[-1].file_id
        db.update_apartment_field(apt_id, field, value)
    elif field == "aliases":
        text = update.message.text.strip()
        aliases = [] if text == "-" else [a.strip().upper() for a in text.split(",") if a.strip()]
        db.update_apartment_aliases(apt_id, aliases)
        refresh_index()
    elif field == "installment_price_per_sqm":
        raw = update.message.text.strip().replace(" ", "").replace(",", "")
        if raw == "-" or raw == "0":
            db.update_apartment_field(apt_id, field, None)
        elif not raw.isdigit() or int(raw) <= 0:
            await update.message.reply_text("Введите целое число (например: 165000) или - чтобы убрать:")
            return EDIT_VALUE
        else:
            db.update_apartment_field(apt_id, field, int(raw))
    elif field == "price_per_sqm":
        min_price, floor_prices = parse_floor_prices(update.message.text.strip())
        if not min_price:
            await update.message.reply_text("Неверный формат.\nОдна цена: 150000\nПо этажам: 1-5:150000, 6-10:160000")
            return EDIT_VALUE
        db.update_apartment_field(apt_id, "price_per_sqm", min_price)
        # Store floor_prices as JSON directly
        import json as _json
        with db._get_conn() as conn:
            conn.execute(
                "UPDATE apartments SET floor_prices = ? WHERE id = ?",
                (_json.dumps(floor_prices, ensure_ascii=False) if floor_prices else None, apt_id)
            )
            conn.commit()
    else:
        value = update.message.text.strip()
        db.update_apartment_field(apt_id, field, value)
        if field == "name":
            refresh_index()

    label = EDITABLE_FIELDS[field]
    keyboard = [[
        InlineKeyboardButton("Изменить ещё поле", callback_data="edit_more"),
        InlineKeyboardButton("Готово",            callback_data="edit_done"),
    ]]
    await update.message.reply_text(
        f"«{label}» обновлено.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return EDIT_AFTER


async def edit_after(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "edit_done":
        context.user_data.clear()
        await cq.edit_message_text("Редактирование завершено.")
        return ConversationHandler.END
    # edit_more — show field picker again
    return await _show_field_picker(cq, context)


async def edit_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "ed_dist_back":
        return await _show_field_picker(cq, context)
    apt_id = context.user_data["edit_apt_id"]
    dist_id_str = cq.data.split("|", 1)[1]
    dist_id = int(dist_id_str)
    db.update_apartment_field(apt_id, "district_id", dist_id if dist_id else None)
    label = "убран" if not dist_id else next(
        (d["name"] for d in db.get_all_districts() if d["id"] == dist_id), str(dist_id)
    )
    keyboard = [[
        InlineKeyboardButton("Изменить ещё поле", callback_data="edit_more"),
        InlineKeyboardButton("Готово",            callback_data="edit_done"),
    ]]
    await cq.edit_message_text(
        f"Район обновлён: {label}",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return EDIT_AFTER


async def edit_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Редактирование отменено.")
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Admin: /post
# ---------------------------------------------------------------------------

(
    S_NAME, S_ALIASES, S_DISTRICT, S_ADDRESS, S_PRICE, S_INST_PRICE,
    S_DESC, S_PHOTO, S_PHOTOS, S_LAYOUTS,
    S_CHESS, S_INST, S_CONFIRM,
) = range(13)

SKIP_CB = "post_skip"


async def post_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    context.user_data.clear()
    await update.message.reply_text("Добавление нового ЖК.\n\nШаг 1/12 — Введите название ЖК:\n\n/cancel — отменить")
    return S_NAME


async def s_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip().upper()
    context.user_data["name"] = name
    context.user_data["id"]   = slugify(name)
    await update.message.reply_text(
        f"Название: *{name}*\n\n"
        "Шаг 2/12 — Альтернативные названия через запятую (АНУР, АННУР и т.д.)\n"
        "Или нажмите Пропустить:",
        parse_mode="Markdown",
        reply_markup=_skip_btn("post_skip_aliases"),
    )
    return S_ALIASES


async def s_aliases(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    context.user_data["aliases"] = [a.strip().upper() for a in text.split(",") if a.strip()]
    return await _ask_district(update.effective_chat)


async def s_aliases_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    context.user_data["aliases"] = []
    await cq.edit_message_text("Псевдонимы: нет")
    return await _ask_district(update.effective_chat)


async def _ask_district(chat) -> int:
    districts = db.get_all_districts()
    if not districts:
        await chat.send_message("Шаг 3/12 — Район не назначен (нет районов в базе).\n\nШаг 4/12 — Введите адрес ЖК:")
        return S_ADDRESS
    keyboard = [[InlineKeyboardButton(d["name"], callback_data=f"pd|{d['id']}")] for d in districts]
    keyboard.append([InlineKeyboardButton("Пропустить", callback_data="pd|0")])
    await chat.send_message(
        "Шаг 3/12 — Выберите район ЖК:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return S_DISTRICT


async def s_district(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    dist_id = int(cq.data.split("|", 1)[1])
    context.user_data["district_id"] = dist_id if dist_id else None
    label = next((d["name"] for d in db.get_all_districts() if d["id"] == dist_id), None)
    await cq.edit_message_text(f"Район: {label or 'не выбран'}")
    await update.effective_chat.send_message("Шаг 4/12 — Введите адрес ЖК:")
    return S_ADDRESS


async def s_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["address"] = update.message.text.strip()
    await update.message.reply_text(
        "Шаг 5/12 — Цена за м².\n\n"
        "Одна цена для всех этажей: 150000\n"
        "Разные цены по этажам: 1-5:150000, 6-10:160000, 11+:170000"
    )
    return S_PRICE


async def s_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    min_price, floor_prices = parse_floor_prices(update.message.text.strip())
    if not min_price:
        await update.message.reply_text(
            "Неверный формат.\n"
            "Одна цена: 150000\n"
            "По этажам: 1-5:150000, 6-10:160000"
        )
        return S_PRICE
    context.user_data["price_per_sqm"] = min_price
    context.user_data["floor_prices"]  = floor_prices
    await update.message.reply_text(
        "Шаг 6/12 — Цена за м² в рассрочку.\n\n"
        "Если цена в рассрочку отличается от наличной — введите её.\n"
        "Например: 165000\n\n"
        "Если одинаковая — нажмите Пропустить:",
        reply_markup=_skip_btn("post_skip_inst_price"),
    )
    return S_INST_PRICE


async def s_inst_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip().replace(" ", "").replace(",", "")
    if not raw.isdigit() or int(raw) <= 0:
        await update.message.reply_text("Введите целое число (например: 165000):")
        return S_INST_PRICE
    context.user_data["installment_price_per_sqm"] = int(raw)
    await update.message.reply_text(
        "Шаг 7/12 — Краткое описание ЖК.\nИли нажмите Пропустить:",
        reply_markup=_skip_btn("post_skip_desc"),
    )
    return S_DESC


async def s_inst_price_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    context.user_data["installment_price_per_sqm"] = None
    await cq.edit_message_text("Цена в рассрочку: нет")
    await update.effective_chat.send_message(
        "Шаг 7/12 — Краткое описание ЖК.\nИли нажмите Пропустить:",
        reply_markup=_skip_btn("post_skip_desc"),
    )
    return S_DESC


async def s_desc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["description"] = update.message.text.strip()
    await _ask_photo(update.message)
    return S_PHOTO


async def s_desc_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    context.user_data["description"] = ""
    await cq.edit_message_text("Описание: нет")
    await update.effective_chat.send_message(
        "Шаг 8/12 — Загрузите главное фото ЖК из галереи:\n\nНажмите 📎 → Фото или видео → выберите фото"
    )
    return S_PHOTO


async def _ask_photo(msg) -> None:
    await msg.reply_text(
        "Шаг 8/12 — Загрузите главное фото ЖК из галереи:\n\nНажмите 📎 → Фото или видео → выберите фото"
    )


async def s_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message.photo:
        await update.message.reply_text("Нужно отправить фото.\nНажмите 📎 → Фото или видео → выберите фото")
        return S_PHOTO
    context.user_data["main_photo"] = update.message.photo[-1].file_id
    context.user_data.setdefault("photos_file_ids", [])
    context.user_data.setdefault("photos_url", "")
    await update.message.reply_text(
        "Шаг 9/12 — Галерея фото\n\n"
        "Отправьте до 10 фото, ссылку (Google Drive / Яндекс Диск) или всё вместе.\n"
        "Нажмите Пропустить, если галерея не нужна.",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("Пропустить", callback_data="photos_skip"),
        ]]),
    )
    return S_PHOTOS


async def s_photos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles photo messages and URL text in the gallery collection step."""
    if update.message.photo:
        file_ids = context.user_data.setdefault("photos_file_ids", [])
        if len(file_ids) >= 10:
            await update.message.reply_text("Максимум 10 фото уже добавлено. Нажмите Готово.")
            return S_PHOTOS
        file_ids.append(update.message.photo[-1].file_id)
    else:
        context.user_data["photos_url"] = update.message.text.strip()

    n   = len(context.user_data.get("photos_file_ids", []))
    url = context.user_data.get("photos_url", "")
    parts = []
    if n:   parts.append(f"{n}/10 фото")
    if url: parts.append("ссылка ✓")
    status = ", ".join(parts)

    kb = [
        [InlineKeyboardButton(f"Готово ({status})", callback_data="photos_done")],
        [InlineKeyboardButton("Пропустить",          callback_data="photos_skip")],
    ]
    await update.message.reply_text(
        "Отправляйте ещё фото или ссылку, или нажмите Готово:",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return S_PHOTOS


async def s_photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "photos_skip":
        context.user_data["photos_file_ids"] = []
        context.user_data["photos_url"] = ""
    await cq.edit_message_text("Галерея сохранена.")
    await update.effective_chat.send_message("Шаг 10/12 — Ссылка на планировки:")
    return S_LAYOUTS


async def s_layouts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["layouts_url"] = update.message.text.strip()
    await update.message.reply_text("Шаг 11/12 — Ссылка на шахматку:")
    return S_CHESS


async def s_chess(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["chess_url"] = update.message.text.strip()
    await update.message.reply_text(
        "Шаг 12/12 — Условия рассрочки.\n\n"
        "Это текстовое поле — пишите в свободной форме.\n"
        "Пример:\nРассрочка на 24 месяца.\nПервый взнос от 30%.\n"
        "Ежемесячный платёж без наценки."
    )
    return S_INST


async def s_inst(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["installment_text"] = update.message.text.strip()
    d = context.user_data
    aliases_str = ", ".join(d.get("aliases", [])) or "нет"
    fp = d.get("floor_prices")
    if fp:
        price_str = "по этажам:\n" + "\n".join(f"  {x['range']} эт. — {fmt(x['price'])} руб/м²" for x in fp)
    else:
        price_str = f"{fmt(d['price_per_sqm'])} руб/м²"
    summary = (
        "Проверьте данные:\n\n"
        f"Название:   {d['name']}\n"
        f"Псевдонимы: {aliases_str}\n"
        f"Адрес:      {d['address']}\n"
        f"Цена за м²: {price_str}\n"
        f"Описание:   {d.get('description') or 'нет'}\n"
    )
    keyboard = [[
        InlineKeyboardButton("Сохранить",  callback_data="post_save"),
        InlineKeyboardButton("Отменить",   callback_data="post_discard"),
    ]]
    await update.message.reply_text(summary, reply_markup=InlineKeyboardMarkup(keyboard))
    return S_CONFIRM


async def s_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "post_discard":
        context.user_data.clear()
        await cq.edit_message_text("Добавление отменено.")
        return ConversationHandler.END
    apt_data = dict(context.user_data)
    db.save_apartment(apt_data)
    refresh_index()
    apt_id = apt_data["id"]
    context.user_data.clear()
    keyboard = [[
        InlineKeyboardButton("Настроить калькулятор", callback_data=f"setup_calc|{apt_id}"),
        InlineKeyboardButton("Пропустить",             callback_data="setup_calc_skip"),
    ]]
    await cq.edit_message_text(
        f"ЖК *{apt_data['name']}* добавлен!\n\nДобавить калькулятор рассрочки?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return ConversationHandler.END


async def post_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Admin: /calc  — calculator setup ConversationHandler
# ---------------------------------------------------------------------------

CS_PICK_APT, CS_ASK_ACTION, CS_FLOOR_LABEL, CS_FLOOR_PRICE, \
CS_LAYOUT_NAME, CS_MORE, CS_MIN_DOWN, CS_DISCOUNTS, CS_TERMS = range(30, 39)
CS_LAYOUT_AREA, CS_LAYOUT_MORE, CS_LAYOUT_COUNT = 39, 40, 41
CS_MANDATORY, CS_INST_BASE = 42, 43

CE_PICK_FIELD, CE_PRICE_GROUP, CE_PRICE_VALUE, CE_COUNT_GROUP, \
CE_COUNT_LAYOUT, CE_COUNT_VALUE, CE_MIN_DOWN, CE_DISCOUNTS, \
CE_TERMS, CE_AFTER = range(70, 80)
CE_MANDATORY_VALUE, CE_INST_BASE_EDIT = 80, 81


async def calc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    apts = db.get_all_apartments()
    if not apts:
        await update.message.reply_text("Список ЖК пуст.")
        return ConversationHandler.END
    context.user_data.clear()
    keyboard = [[InlineKeyboardButton(a["name"], callback_data=f"cpa|{a['id']}")] for a in apts]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="csx")])
    await update.message.reply_text("Выберите ЖК для настройки калькулятора:", reply_markup=InlineKeyboardMarkup(keyboard))
    return CS_PICK_APT


async def _cs_start_setup(cq, apt_id: str, apt_name: str, context) -> int:
    context.user_data["cs_apt_id"]   = apt_id
    context.user_data["cs_apt_name"] = apt_name
    context.user_data["cs_groups"]   = []
    await cq.edit_message_text(
        f"Настройка калькулятора — *{apt_name}*\n\nШаг 1 — Введите диапазон этажей (например: 1-5):",
        parse_mode="Markdown",
    )
    return CS_FLOOR_LABEL


async def cs_pick_apt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "csx":
        context.user_data.clear()
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    apt_id = cq.data.split("|", 1)[1]
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not apt:
        await cq.edit_message_text("ЖК не найден.")
        return ConversationHandler.END
    existing = db.get_calculator(apt_id)
    if existing:
        keyboard = [
            [InlineKeyboardButton("Редактировать", callback_data=f"csa_edit|{apt_id}")],
            [InlineKeyboardButton("Удалить",        callback_data=f"csa_del|{apt_id}")],
            [InlineKeyboardButton("Отмена",          callback_data="csx")],
        ]
        await cq.edit_message_text(
            f"У ЖК *{apt['name']}* уже есть калькулятор. Что сделать?",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return CS_ASK_ACTION
    return await _cs_start_setup(cq, apt_id, apt["name"], context)


async def cs_ask_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "csx":
        context.user_data.clear()
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    action, apt_id = cq.data.split("|", 1)
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if action == "csa_del":
        db.delete_calculator(apt_id)
        context.user_data.clear()
        await cq.edit_message_text(f"Калькулятор ЖК *{apt['name']}* удалён.", parse_mode="Markdown")
        return ConversationHandler.END
    # csa_edit — open dashboard editor
    calc = db.get_calculator(apt_id)
    context.user_data["cs_apt_id"]  = apt_id
    context.user_data["ce_apt_name"] = apt["name"]
    context.user_data["ce_calc"]    = calc
    return await _ce_edit_menu(cq, context)


async def setup_calc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "setup_calc_skip":
        await cq.edit_message_text("Калькулятор не добавлен. Используйте /calc чтобы добавить его позже.")
        return
    apt_id = cq.data.split("|", 1)[1]
    apt = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not apt:
        await cq.edit_message_text("ЖК не найден.")
        return
    await cq.edit_message_text(
        f"Используйте команду /calc чтобы настроить калькулятор для *{apt['name']}*.",
        parse_mode="Markdown",
    )


async def cs_floor_label(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["cs_cur_label"] = update.message.text.strip()
    await update.message.reply_text("Цена за м² на этих этажах (только цифры):")
    return CS_FLOOR_PRICE


async def cs_floor_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip().replace(" ", "").replace(",", "")
    if not raw.isdigit():
        await update.message.reply_text("Введите только цифры:")
        return CS_FLOOR_PRICE
    context.user_data["cs_cur_price"]   = int(raw)
    context.user_data["cs_cur_layouts"] = []
    await update.message.reply_text("Введите название первой планировки (например: 1-комн, Студия, 2-комн):")
    return CS_LAYOUT_NAME


async def cs_layout_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["cs_cur_layout_name"] = update.message.text.strip()
    await update.message.reply_text("Введите площадь в м² (например: 45.5):")
    return CS_LAYOUT_AREA


async def cs_layout_area(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip().replace(",", ".")
    try:
        area = float(raw)
        if area <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Введите корректное число (например: 45.5):")
        return CS_LAYOUT_AREA
    name = context.user_data.pop("cs_cur_layout_name")
    context.user_data["cs_cur_layout_pending"] = {"name": name, "area": area}
    await update.message.reply_text(
        "Сколько квартир этой планировки на этаже? (необязательно)\n\nИли нажмите Пропустить:",
        reply_markup=_skip_btn("cs_skip_count"),
    )
    return CS_LAYOUT_COUNT


async def cs_layout_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.pop("cs_cur_layout_pending")
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        await cq.edit_message_text("Количество: не указано")
    else:
        raw = update.message.text.strip().replace(" ", "")
        if not raw.isdigit() or int(raw) <= 0:
            context.user_data["cs_cur_layout_pending"] = pending
            await update.message.reply_text(
                "Введите целое число больше 0 или нажмите Пропустить:",
                reply_markup=_skip_btn("cs_skip_count"),
            )
            return CS_LAYOUT_COUNT
        pending["count"] = int(raw)
    context.user_data["cs_cur_layouts"].append(pending)
    added = context.user_data["cs_cur_layouts"]
    lines = "\n".join(
        f"  {l['name']} — {l['area']} м²" + (f" ({l['count']} кв.)" if l.get("count") else "")
        for l in added
    )
    keyboard = [
        [InlineKeyboardButton("Добавить ещё планировку", callback_data="cl_more")],
        [InlineKeyboardButton("Готово с планировками",   callback_data="cl_done")],
    ]
    await update.effective_chat.send_message(
        f"Добавлено: {len(added)}\n{lines}\n\nДобавить ещё?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return CS_LAYOUT_MORE


async def cs_layout_more(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "cl_more":
        await cq.edit_message_text("Введите название следующей планировки:")
        return CS_LAYOUT_NAME
    group = {
        "label":         context.user_data.pop("cs_cur_label"),
        "price_per_sqm": context.user_data.pop("cs_cur_price"),
        "layouts":       context.user_data.pop("cs_cur_layouts"),
    }
    context.user_data["cs_groups"].append(group)
    n = len(context.user_data["cs_groups"])
    layout_lines = "\n".join(
        f"  {l['name']} — {l['area']} м²" + (f" ({l['count']} кв.)" if l.get("count") else "")
        for l in group["layouts"]
    )
    keyboard = [
        [InlineKeyboardButton("Добавить ещё группу этажей", callback_data="cs_more")],
        [InlineKeyboardButton("Готово, продолжить",          callback_data="cs_done")],
    ]
    await cq.edit_message_text(
        f"Группа {n} сохранена:\n{group['label']} — {fmt(group['price_per_sqm'])} руб/м²\n{layout_lines}\n\n"
        "Добавить ещё группу этажей (другой диапазон с другой ценой)?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return CS_MORE


async def cs_more(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "cs_more":
        await cq.edit_message_text("Введите диапазон следующей группы этажей (например: 6-10):")
        return CS_FLOOR_LABEL
    await cq.edit_message_text("Введите минимальный первоначальный взнос (%, целое число, например: 30):")
    return CS_MIN_DOWN


async def cs_min_down(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip().replace("%", "")
    if not raw.isdigit() or not (0 <= int(raw) < 100):
        await update.message.reply_text("Введите целое число от 0 до 99:\n(0 — если взнос не требуется, только обязательный платёж)")
        return CS_MIN_DOWN
    context.user_data["cs_min_down"] = int(raw)
    await update.message.reply_text(
        "Снижение цены за м² при первоначальном взносе.\n\n"
        "Смысл: чем больше взнос — тем дешевле цена за м².\n\n"
        "Формат: взнос%:скидка_руб/м²\n"
        "Пример: 30:5000, 50:8000, 70:10000\n\n"
        "Это значит:\n"
        "• взнос от 30% → цена снижается на 5 000 руб/м²\n"
        "• взнос от 50% → цена снижается на 8 000 руб/м²\n"
        "• взнос от 70% → цена снижается на 10 000 руб/м²\n\n"
        "Введите свои значения или нажмите Пропустить:",
        reply_markup=_skip_btn("cs_skip_discounts"),
    )
    return CS_DISCOUNTS


def _terms_prompt() -> str:
    return (
        "Сроки рассрочки с наценкой.\n\n"
        "Формат: месяцев:наценка%\n"
        "Пример: 12:0, 24:5, 36:10\n\n"
        "Это значит:\n"
        "• 12 месяцев → без наценки\n"
        "• 24 месяца → наценка 5%\n"
        "• 36 месяцев → наценка 10%\n\n"
        "Введите свои значения:"
    )


async def cs_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        context.user_data["cs_discounts"] = []
        await cq.edit_message_text("Снижение цены: нет")
        await update.effective_chat.send_message(_terms_prompt())
        return CS_TERMS
    text = update.message.text.strip()
    pairs = parse_kv_list(text)
    if not pairs:
        await update.message.reply_text("Неверный формат. Пример: 30:5000, 50:8000\nПопробуйте снова:")
        return CS_DISCOUNTS
    context.user_data["cs_discounts"] = [{"from_pct": a, "discount_per_sqm": b} for a, b in pairs]
    await update.message.reply_text(_terms_prompt())
    return CS_TERMS


async def cs_terms(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pairs = parse_kv_list(update.message.text.strip())
    if not pairs:
        await update.message.reply_text("Неверный формат. Пример: 12:0, 24:5, 36:10\nПопробуйте снова:")
        return CS_TERMS
    terms = sorted([{"months": int(a), "markup_pct": b} for a, b in pairs], key=lambda x: x["months"])
    context.user_data["cs_terms"] = terms
    await update.message.reply_text(
        "Обязательный платёж (руб/м²).\n\n"
        "Фиксированная сумма за каждый квадратный метр, которая засчитывается как первоначальный взнос.\n"
        "То есть это и есть взнос — просто не в процентах, а в рублях за м².\n\n"
        "Пример: 2000 руб/м² × 40 м² = 80 000 руб взнос.\n\n"
        "Если обязательного платежа нет — нажмите Пропустить:",
        reply_markup=_skip_btn("cs_skip_mandatory"),
    )
    return CS_MANDATORY


async def cs_mandatory(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        context.user_data["cs_mandatory_per_sqm"] = 0
        await cq.edit_message_text("Обязательный платёж: нет")
    else:
        raw = update.message.text.strip().replace(" ", "")
        if not raw.isdigit():
            await update.message.reply_text(
                "Введите целое число (руб/м²) или нажмите Пропустить:",
                reply_markup=_skip_btn("cs_skip_mandatory"),
            )
            return CS_MANDATORY
        context.user_data["cs_mandatory_per_sqm"] = int(raw)
    keyboard = [[
        InlineKeyboardButton("На остаток после взноса", callback_data="csib|remaining"),
        InlineKeyboardButton("На всю сумму квартиры",  callback_data="csib|full"),
    ]]
    await update.effective_chat.send_message(
        "Как начисляется рассрочка?\n\n"
        "На остаток: рассрочка = стоимость − взнос\n"
        "На всю сумму: рассрочка = вся стоимость, взнос оплачивается отдельно",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return CS_INST_BASE


async def cs_inst_base(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    on_remaining = cq.data.split("|")[1] == "remaining"
    d = context.user_data
    calc_data = {
        "floor_groups":             d["cs_groups"],
        "min_down_pct":             d["cs_min_down"],
        "discounts":                d.get("cs_discounts", []),
        "terms":                    d["cs_terms"],
        "mandatory_per_sqm":        d.get("cs_mandatory_per_sqm", 0),
        "installment_on_remaining": on_remaining,
    }
    apt_id   = d["cs_apt_id"]
    apt_name = d["cs_apt_name"]
    db.save_calculator(apt_id, calc_data)
    context.user_data.clear()
    groups_summary = "\n".join(
        f"  {g['label']}: {fmt(g['price_per_sqm'])} руб/м², {len(g['layouts'])} планировок"
        for g in calc_data["floor_groups"]
    )
    terms_summary = ", ".join(f"{t['months']} мес. (+{t['markup_pct']}%)" for t in calc_data["terms"])
    mand = calc_data["mandatory_per_sqm"]
    mand_str = f"{fmt(mand)} руб/м²" if mand else "нет"
    base_str = "на остаток после взноса" if on_remaining else "на всю сумму"
    await cq.edit_message_text(
        f"Калькулятор для *{apt_name}* сохранён!\n\n"
        f"Группы этажей:\n{groups_summary}\n"
        f"Мин. взнос: {calc_data['min_down_pct']}%\n"
        f"Обяз. платёж: {mand_str}\n"
        f"Рассрочка: {base_str}\n"
        f"Сроки: {terms_summary}",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def calc_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Настройка отменена.")
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Admin: Calculator editor (CE) — dashboard with current values
# ---------------------------------------------------------------------------

def _ce_save(context) -> None:
    db.save_calculator(context.user_data["cs_apt_id"], context.user_data["ce_calc"])


def _ce_menu_text_and_kb(context) -> tuple[str, InlineKeyboardMarkup]:
    calc = context.user_data["ce_calc"]
    apt_name = context.user_data.get("ce_apt_name", "")

    # Build summary text
    groups = calc.get("floor_groups", [])
    groups_lines = "\n".join(
        f"  {g['label']}: {fmt(g['price_per_sqm'])} руб/м²"
        + (f", {sum(l.get('count', 0) for l in g['layouts'])} кв." if any(l.get('count') for l in g['layouts']) else "")
        for g in groups
    )
    disc = calc.get("discounts", [])
    disc_str = ", ".join(f"{d['from_pct']}%→−{fmt(d.get('discount_per_sqm',0))}" for d in disc) if disc else "нет"
    terms = calc.get("terms", [])
    terms_str = ", ".join(f"{t['months']}м({t['markup_pct']}%)" for t in terms) if terms else "нет"
    mand = calc.get("mandatory_per_sqm", 0)
    mand_str = f"{fmt(mand)} руб/м²" if mand else "нет"
    base_str = "на остаток" if calc.get("installment_on_remaining", True) else "на всю сумму"

    text = (
        f"*Калькулятор — {apt_name}*\n\n"
        f"Группы этажей (цены):\n{groups_lines}\n\n"
        f"Мин. взнос:       {calc['min_down_pct']}%\n"
        f"Обяз. платёж:     {mand_str}\n"
        f"Метод рассрочки:  {base_str}\n"
        f"Снижение цены:    {disc_str}\n"
        f"Сроки рассрочки:  {terms_str}"
    )

    keyboard = [
        [InlineKeyboardButton("✏️ Цены по группам этажей",    callback_data="ce_f|price")],
        [InlineKeyboardButton("✏️ Количество квартир",         callback_data="ce_f|count")],
        [InlineKeyboardButton(f"✏️ Мин. взнос ({calc['min_down_pct']}%)", callback_data="ce_f|mindown")],
        [InlineKeyboardButton(f"✏️ Снижение цены ({disc_str[:30]})", callback_data="ce_f|discounts")],
        [InlineKeyboardButton(f"✏️ Сроки ({terms_str[:35]})", callback_data="ce_f|terms")],
        [InlineKeyboardButton(f"✏️ Обяз. платёж ({mand_str})", callback_data="ce_f|mandatory")],
        [InlineKeyboardButton(f"✏️ Метод рассрочки ({base_str})", callback_data="ce_f|instbase")],
        [InlineKeyboardButton("✅ Готово",                      callback_data="ce_f|done")],
    ]
    return text, InlineKeyboardMarkup(keyboard)


async def _ce_edit_menu(cq, context) -> int:
    """Edit existing message to show the dashboard."""
    text, kb = _ce_menu_text_and_kb(context)
    await cq.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    return CE_PICK_FIELD


async def _ce_send_menu(chat, context) -> int:
    """Send a new message with the dashboard (used after text input)."""
    text, kb = _ce_menu_text_and_kb(context)
    await chat.send_message(text, parse_mode="Markdown", reply_markup=kb)
    return CE_PICK_FIELD


async def ce_pick_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    field = cq.data.split("|", 1)[1]
    calc = context.user_data["ce_calc"]

    if field == "done":
        context.user_data.clear()
        await cq.edit_message_text("Редактирование калькулятора завершено.")
        return ConversationHandler.END

    if field == "price":
        groups = calc["floor_groups"]
        keyboard = [
            [InlineKeyboardButton(f"{g['label']} — {fmt(g['price_per_sqm'])} руб/м²", callback_data=f"cepg|{i}")]
            for i, g in enumerate(groups)
        ]
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="ce_back")])
        await cq.edit_message_text("Выберите группу этажей:", reply_markup=InlineKeyboardMarkup(keyboard))
        return CE_PRICE_GROUP

    if field == "count":
        groups = calc["floor_groups"]
        keyboard = [
            [InlineKeyboardButton(g["label"], callback_data=f"cecg|{i}")]
            for i, g in enumerate(groups)
        ]
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="ce_back")])
        await cq.edit_message_text("Выберите группу этажей:", reply_markup=InlineKeyboardMarkup(keyboard))
        return CE_COUNT_GROUP

    if field == "mindown":
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="ce_back_mindown")]])
        await cq.edit_message_text(
            f"Мин. взнос сейчас: *{calc['min_down_pct']}%*\n\nВведите новое значение (%):",
            parse_mode="Markdown", reply_markup=kb,
        )
        return CE_MIN_DOWN

    if field == "discounts":
        disc = calc.get("discounts", [])
        cur = ", ".join(f"{d['from_pct']}:{d.get('discount_per_sqm', 0)}" for d in disc) or "нет"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Убрать снижение", callback_data="ce_skip_discounts"),
            InlineKeyboardButton("◀️ Назад",         callback_data="ce_back_discounts"),
        ]])
        await cq.edit_message_text(
            f"Снижение цены сейчас: *{cur}*\n\n"
            "Формат: взнос%:снижение_руб/м²\nПример: 30:5000, 50:8000",
            parse_mode="Markdown", reply_markup=kb,
        )
        return CE_DISCOUNTS

    if field == "mandatory":
        cur = calc.get("mandatory_per_sqm", 0)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("Убрать платёж", callback_data="ce_clear_mandatory"),
            InlineKeyboardButton("◀️ Назад",       callback_data="ce_back_mandatory"),
        ]])
        await cq.edit_message_text(
            f"Обяз. платёж сейчас: *{fmt(cur) + ' руб/м²' if cur else 'нет'}*\n\n"
            "Введите новое значение (руб/м²):",
            parse_mode="Markdown", reply_markup=kb,
        )
        return CE_MANDATORY_VALUE

    if field == "instbase":
        cur = calc.get("installment_on_remaining", True)
        cur_label = "на остаток" if cur else "на всю сумму"
        keyboard = [
            [InlineKeyboardButton("✅ На остаток после взноса", callback_data="ceib|remaining")],
            [InlineKeyboardButton("✅ На всю сумму квартиры",  callback_data="ceib|full")],
            [InlineKeyboardButton("◀️ Назад",                   callback_data="ceib|back")],
        ]
        await cq.edit_message_text(
            f"Метод рассрочки сейчас: *{cur_label}*\n\nВыберите:",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return CE_INST_BASE_EDIT

    if field == "terms":
        terms = calc.get("terms", [])
        cur = ", ".join(f"{t['months']}:{t['markup_pct']}" for t in terms) or "нет"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="ce_back_terms")]])
        await cq.edit_message_text(
            f"Сроки сейчас: *{cur}*\n\n"
            "Формат: месяцев:наценка%\nПример: 12:0, 24:5, 36:10",
            parse_mode="Markdown", reply_markup=kb,
        )
        return CE_TERMS

    return await _ce_edit_menu(cq, context)


async def ce_price_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "ce_back":
        return await _ce_edit_menu(cq, context)
    g_idx = int(cq.data.split("|", 1)[1])
    context.user_data["ce_g_idx"] = g_idx
    group = context.user_data["ce_calc"]["floor_groups"][g_idx]
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="ce_back_price_grp")]])
    await cq.edit_message_text(
        f"Группа: *{group['label']}*\nЦена сейчас: {fmt(group['price_per_sqm'])} руб/м²\n\nВведите новую цену:",
        parse_mode="Markdown", reply_markup=kb,
    )
    return CE_PRICE_VALUE


async def ce_price_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        # "◀️ Назад" — back to group picker
        cq = update.callback_query
        await cq.answer()
        groups = context.user_data["ce_calc"]["floor_groups"]
        keyboard = [
            [InlineKeyboardButton(f"{g['label']} — {fmt(g['price_per_sqm'])} руб/м²", callback_data=f"cepg|{i}")]
            for i, g in enumerate(groups)
        ]
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="ce_back")])
        await cq.edit_message_text("Выберите группу этажей:", reply_markup=InlineKeyboardMarkup(keyboard))
        return CE_PRICE_GROUP
    raw = update.message.text.strip().replace(" ", "").replace(",", "")
    if not raw.isdigit() or int(raw) <= 0:
        await update.message.reply_text("Введите целое число:")
        return CE_PRICE_VALUE
    price = int(raw)
    g_idx = context.user_data.pop("ce_g_idx")
    context.user_data["ce_calc"]["floor_groups"][g_idx]["price_per_sqm"] = price
    _ce_save(context)
    return await _ce_send_menu(update.effective_chat, context)


async def ce_count_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "ce_back":
        return await _ce_edit_menu(cq, context)
    g_idx = int(cq.data.split("|", 1)[1])
    context.user_data["ce_g_idx"] = g_idx
    layouts = context.user_data["ce_calc"]["floor_groups"][g_idx]["layouts"]
    keyboard = [
        [InlineKeyboardButton(
            f"{l['name']} {l['area']} м²" + (f" ({l['count']} кв.)" if l.get("count") else ""),
            callback_data=f"cecl|{i}",
        )]
        for i, l in enumerate(layouts)
    ]
    keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="ce_back_grp")])
    await cq.edit_message_text("Выберите планировку:", reply_markup=InlineKeyboardMarkup(keyboard))
    return CE_COUNT_LAYOUT


async def ce_count_layout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "ce_back_grp":
        groups = context.user_data["ce_calc"]["floor_groups"]
        keyboard = [
            [InlineKeyboardButton(g["label"], callback_data=f"cecg|{i}")]
            for i, g in enumerate(groups)
        ]
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="ce_back")])
        await cq.edit_message_text("Выберите группу этажей:", reply_markup=InlineKeyboardMarkup(keyboard))
        return CE_COUNT_GROUP
    l_idx = int(cq.data.split("|", 1)[1])
    context.user_data["ce_l_idx"] = l_idx
    g_idx = context.user_data["ce_g_idx"]
    layout = context.user_data["ce_calc"]["floor_groups"][g_idx]["layouts"][l_idx]
    cur = layout.get("count", "не указано")
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Назад", callback_data="ce_back_cnt_lay")]])
    await cq.edit_message_text(
        f"*{layout['name']}* {layout['area']} м²\nКол-во сейчас: {cur}\n\nВведите новое количество:",
        parse_mode="Markdown", reply_markup=kb,
    )
    return CE_COUNT_VALUE


async def ce_count_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        # "◀️ Назад" — back to layout picker
        cq = update.callback_query
        await cq.answer()
        g_idx = context.user_data["ce_g_idx"]
        layouts = context.user_data["ce_calc"]["floor_groups"][g_idx]["layouts"]
        keyboard = [
            [InlineKeyboardButton(
                f"{l['name']} {l['area']} м²" + (f" ({l['count']} кв.)" if l.get("count") else ""),
                callback_data=f"cecl|{i}",
            )]
            for i, l in enumerate(layouts)
        ]
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="ce_back_grp")])
        await cq.edit_message_text("Выберите планировку:", reply_markup=InlineKeyboardMarkup(keyboard))
        return CE_COUNT_LAYOUT
    raw = update.message.text.strip().replace(" ", "")
    if not raw.isdigit() or int(raw) <= 0:
        await update.message.reply_text("Введите целое число больше 0:")
        return CE_COUNT_VALUE
    count = int(raw)
    g_idx = context.user_data.pop("ce_g_idx")
    l_idx = context.user_data.pop("ce_l_idx")
    context.user_data["ce_calc"]["floor_groups"][g_idx]["layouts"][l_idx]["count"] = count
    _ce_save(context)
    return await _ce_send_menu(update.effective_chat, context)


async def ce_min_down(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        return await _ce_edit_menu(cq, context)
    raw = update.message.text.strip().replace("%", "")
    if not raw.isdigit() or not (0 <= int(raw) < 100):
        await update.message.reply_text("Введите целое число от 0 до 99:")
        return CE_MIN_DOWN
    pct = int(raw)
    context.user_data["ce_calc"]["min_down_pct"] = pct
    _ce_save(context)
    return await _ce_send_menu(update.effective_chat, context)


async def ce_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        if cq.data == "ce_skip_discounts":
            context.user_data["ce_calc"]["discounts"] = []
            _ce_save(context)
            return await _ce_edit_menu(cq, context)
        # ce_back_discounts
        return await _ce_edit_menu(cq, context)
    pairs = parse_kv_list(update.message.text.strip())
    if not pairs:
        await update.message.reply_text("Неверный формат. Пример: 30:5000, 50:8000\nПопробуйте снова:")
        return CE_DISCOUNTS
    context.user_data["ce_calc"]["discounts"] = [{"from_pct": a, "discount_per_sqm": b} for a, b in pairs]
    _ce_save(context)
    return await _ce_send_menu(update.effective_chat, context)


async def ce_terms(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        return await _ce_edit_menu(cq, context)
    pairs = parse_kv_list(update.message.text.strip())
    if not pairs:
        await update.message.reply_text("Неверный формат. Пример: 12:0, 24:5, 36:10\nПопробуйте снова:")
        return CE_TERMS
    terms = sorted([{"months": int(a), "markup_pct": b} for a, b in pairs], key=lambda x: x["months"])
    context.user_data["ce_calc"]["terms"] = terms
    _ce_save(context)
    return await _ce_send_menu(update.effective_chat, context)


async def ce_mandatory_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        cq = update.callback_query
        await cq.answer()
        if cq.data == "ce_clear_mandatory":
            context.user_data["ce_calc"]["mandatory_per_sqm"] = 0
            _ce_save(context)
        return await _ce_edit_menu(cq, context)
    raw = update.message.text.strip().replace(" ", "")
    if not raw.isdigit():
        await update.message.reply_text("Введите целое число:")
        return CE_MANDATORY_VALUE
    val = int(raw)
    context.user_data["ce_calc"]["mandatory_per_sqm"] = val
    _ce_save(context)
    return await _ce_send_menu(update.effective_chat, context)


async def ce_inst_base_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    val = cq.data.split("|")[1]
    if val == "back":
        return await _ce_edit_menu(cq, context)
    on_remaining = val == "remaining"
    context.user_data["ce_calc"]["installment_on_remaining"] = on_remaining
    _ce_save(context)
    return await _ce_edit_menu(cq, context)


# ---------------------------------------------------------------------------
# User: Calculator ConversationHandler
# ---------------------------------------------------------------------------

UC_LAYOUT, UC_DOWN, UC_TERM = range(50, 53)


async def uc_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    apt_id = cq.data.split("|", 1)[1]
    calc   = db.get_calculator(apt_id)
    apt    = next((a for a in db.get_all_apartments() if a["id"] == apt_id), None)
    if not calc or not apt:
        await cq.message.reply_text("Калькулятор недоступен.")
        return ConversationHandler.END
    context.user_data["uc_apt_id"]   = apt_id
    context.user_data["uc_apt_name"] = apt["name"]
    context.user_data["uc_calc"]     = calc
    keyboard = []
    for g_idx, group in enumerate(calc["floor_groups"]):
        for l_idx, layout in enumerate(group["layouts"]):
            label = f"{group['label']} | {layout['name']} {layout['area']} м²"
            if layout.get("count"):
                label += f" ({layout['count']} кв.)"
            keyboard.append([InlineKeyboardButton(label, callback_data=f"ucl|{g_idx}|{l_idx}")])
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="uc_cancel")])
    await cq.message.reply_text(
        f"Калькулятор рассрочки — *{apt['name']}*\n\nВыберите этаж и планировку:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return UC_LAYOUT


def _uc_term_keyboard(calc: dict) -> InlineKeyboardMarkup:
    rows = []
    for t_idx, term in enumerate(calc["terms"]):
        label = f"{term['months']} мес." + (f" (+{term['markup_pct']}%)" if term["markup_pct"] else " (без наценки)")
        rows.append([InlineKeyboardButton(label, callback_data=f"uct|{t_idx}")])
    rows.append([InlineKeyboardButton("Отмена", callback_data="uc_cancel")])
    return InlineKeyboardMarkup(rows)


async def uc_layout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "uc_cancel":
        context.user_data.clear()
        await cq.edit_message_text("Расчёт отменён.")
        return ConversationHandler.END
    _, g_idx, l_idx = cq.data.split("|")
    g_idx, l_idx = int(g_idx), int(l_idx)
    calc   = context.user_data["uc_calc"]
    group  = calc["floor_groups"][g_idx]
    layout = group["layouts"][l_idx]
    context.user_data["uc_group"]  = group
    context.user_data["uc_layout"] = layout

    mand_per_sqm = calc.get("mandatory_per_sqm", 0)

    if mand_per_sqm:
        # Обязательный платёж = взнос (фиксированная сумма за м²)
        total_price  = layout["area"] * group["price_per_sqm"]
        mand_amount  = layout["area"] * mand_per_sqm
        remaining    = total_price - mand_amount
        context.user_data["uc_down_pct"]      = 0
        context.user_data["uc_disc_per_sqm"]  = 0
        context.user_data["uc_price_per_sqm"] = group["price_per_sqm"]
        context.user_data["uc_total_price"]   = total_price
        context.user_data["uc_down_amount"]   = mand_amount
        context.user_data["uc_remaining"]     = remaining
        context.user_data["uc_mand_amount"]   = mand_amount
        context.user_data["uc_mand_mode"]     = True
        on_remaining = calc.get("installment_on_remaining", True)
        base = remaining if on_remaining else total_price
        base_label = "на остаток" if on_remaining else "на всю сумму"
        await cq.edit_message_text(
            f"*{layout['name']}*, {layout['area']} м²\n"
            f"Этаж: {group['label']}\n"
            f"Цена за м²: {fmt(group['price_per_sqm'])} руб\n"
            f"Стоимость квартиры: {fmt(total_price)} руб\n\n"
            f"Обязательный платёж (взнос): {fmt(mand_per_sqm)} руб/м² × {layout['area']} м² = *{fmt(mand_amount)} руб*\n"
            f"Остаток к рассрочке ({base_label}): {fmt(base)} руб\n\n"
            "Выберите срок рассрочки:",
            parse_mode="Markdown",
            reply_markup=_uc_term_keyboard(calc),
        )
        return UC_TERM

    # Обычный режим — взнос в процентах
    apt_price = layout["area"] * group["price_per_sqm"]
    min_pct   = calc["min_down_pct"]
    discounts = calc.get("discounts", [])
    context.user_data["uc_mand_mode"] = False
    disc_text = ""
    if discounts:
        disc_lines = []
        for d in sorted(discounts, key=lambda x: x["from_pct"]):
            disc       = d.get("discount_per_sqm", 0)
            thr        = d["from_pct"]
            disc_price = group["price_per_sqm"] - disc
            disc_total = layout["area"] * disc_price
            down_sum   = disc_total * thr / 100
            disc_lines.append(
                f"  от {thr}% → {fmt(disc_price)} руб/м² "
                f"(взнос {thr}% = *{fmt(down_sum)} руб*)"
            )
        disc_text = "\n\nСнижение цены при взносе:\n" + "\n".join(disc_lines)
    min_note = f"минимум {min_pct}%" if min_pct > 0 else "можно 0%"
    await cq.edit_message_text(
        f"*{layout['name']}*, {layout['area']} м²\n"
        f"Этаж: {group['label']}\n"
        f"Цена за м²: {fmt(group['price_per_sqm'])} руб\n"
        f"Стоимость: {fmt(apt_price)} руб"
        f"{disc_text}\n\n"
        f"Введите первоначальный взнос ({min_note}):\n"
        f"— в процентах: 30\n"
        f"— в рублях: 500000",
        parse_mode="Markdown",
    )
    return UC_DOWN


async def uc_down(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    calc    = context.user_data["uc_calc"]
    group   = context.user_data["uc_group"]
    layout  = context.user_data["uc_layout"]
    min_pct = calc["min_down_pct"]
    discounts = calc.get("discounts", [])

    raw = update.message.text.strip().replace(" ", "").replace(",", ".")
    explicit_pct = raw.endswith("%")
    raw_num = raw.rstrip("%")
    try:
        value = float(raw_num)
    except ValueError:
        await update.message.reply_text("Введите число — процент (30) или сумму в рублях (500000):")
        return UC_DOWN

    # Determine mode: % or rubles
    # If explicit %, or value ≤ 100 → treat as percent; otherwise rubles
    if explicit_pct or value <= 100:
        # Percentage mode
        down_pct = value
        if down_pct < min_pct:
            await update.message.reply_text(f"Минимальный взнос {min_pct}%:")
            return UC_DOWN
        if down_pct >= 100:
            await update.message.reply_text("Процент должен быть меньше 100:")
            return UC_DOWN
        disc_per_sqm  = find_discount_per_sqm(discounts, down_pct)
        price_per_sqm = group["price_per_sqm"] - disc_per_sqm
        total_price   = layout["area"] * price_per_sqm
        down_amount   = total_price * down_pct / 100
        down_label    = f"Взнос {down_pct}%: {fmt(down_amount)} руб"
    else:
        # Rubles mode — find discount based on the absolute amount vs discounted total
        down_amount   = value
        disc_per_sqm  = find_discount_for_amount(discounts, down_amount,
                                                  layout["area"], group["price_per_sqm"])
        price_per_sqm = group["price_per_sqm"] - disc_per_sqm
        total_price   = layout["area"] * price_per_sqm
        down_pct      = down_amount / total_price * 100
        if down_pct < min_pct:
            # Show minimum as rubles from the best possible discounted price
            best_disc    = find_discount_for_amount(discounts, 0, layout["area"], group["price_per_sqm"])
            best_price   = layout["area"] * (group["price_per_sqm"] - best_disc)
            min_amount   = best_price * min_pct / 100
            await update.message.reply_text(
                f"Минимальный взнос {min_pct}% = {fmt(min_amount)} руб. Введите сумму не меньше:"
            )
            return UC_DOWN
        if down_amount >= total_price:
            await update.message.reply_text("Сумма взноса должна быть меньше стоимости квартиры:")
            return UC_DOWN
        down_label = f"Взнос {fmt(down_amount)} руб ({down_pct:.1f}%)"

    remaining  = total_price - down_amount
    mand_amount = layout["area"] * calc.get("mandatory_per_sqm", 0)
    context.user_data["uc_down_pct"]      = down_pct
    context.user_data["uc_disc_per_sqm"]  = disc_per_sqm
    context.user_data["uc_price_per_sqm"] = price_per_sqm
    context.user_data["uc_total_price"]   = total_price
    context.user_data["uc_down_amount"]   = down_amount
    context.user_data["uc_remaining"]     = remaining
    context.user_data["uc_mand_amount"]   = mand_amount
    context.user_data["uc_down_label"]    = down_label

    keyboard = [
        [InlineKeyboardButton(
            f"{t['months']} мес." + (f" (+{t['markup_pct']}%)" if t["markup_pct"] else " (без наценки)"),
            callback_data=f"uct|{i}",
        )]
        for i, t in enumerate(calc["terms"])
    ]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="uc_cancel")])
    disc_line = f"Снижение цены: −{fmt(disc_per_sqm)} руб/м²\nЦена со снижением: {fmt(price_per_sqm)} руб/м²\n" if disc_per_sqm else ""
    mand_line = f"Обяз. платёж: {fmt(mand_amount)} руб\n" if mand_amount else ""
    await update.message.reply_text(
        f"{disc_line}Стоимость квартиры: {fmt(total_price)} руб\n"
        f"{mand_line}"
        f"{down_label}\n"
        f"Остаток: {fmt(remaining)} руб\n\nВыберите срок рассрочки:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return UC_TERM


async def uc_term(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "uc_cancel":
        context.user_data.clear()
        await cq.edit_message_text("Расчёт отменён.")
        return ConversationHandler.END
    t_idx  = int(cq.data.split("|")[1])
    calc   = context.user_data["uc_calc"]
    term   = calc["terms"][t_idx]
    group  = context.user_data["uc_group"]
    layout = context.user_data["uc_layout"]
    down_pct      = context.user_data["uc_down_pct"]
    disc_per_sqm  = context.user_data["uc_disc_per_sqm"]
    price_per_sqm = context.user_data["uc_price_per_sqm"]
    total_price   = context.user_data["uc_total_price"]
    down_amount   = context.user_data["uc_down_amount"]
    remaining     = context.user_data["uc_remaining"]
    mand_amount   = context.user_data["uc_mand_amount"]
    mand_mode     = context.user_data.get("uc_mand_mode", False)
    mand_per_sqm  = calc.get("mandatory_per_sqm", 0)
    on_remaining  = calc.get("installment_on_remaining", True)
    base          = remaining if on_remaining else total_price
    total_debt    = base * (1 + term["markup_pct"] / 100)
    monthly       = total_debt / term["months"]
    disc_line = (
        f"Снижение цены:     −{fmt(disc_per_sqm)} руб/м²\n"
        f"Цена за м²:        {fmt(price_per_sqm)} руб\n"
    ) if disc_per_sqm else f"Цена за м²:        {fmt(price_per_sqm)} руб\n"
    if mand_mode:
        down_line = (
            f"Взнос (обяз. платёж): {fmt(mand_per_sqm)} руб/м² × {layout['area']} м² = {fmt(mand_amount)} руб\n"
        )
    else:
        stored_label = context.user_data.get("uc_down_label", "")
        if stored_label:
            down_line = stored_label + "\n"
        elif down_pct > 0:
            down_line = f"Взнос {down_pct}%:       {fmt(down_amount)} руб\n"
        else:
            down_line = "Без первоначального взноса\n"
    markup_line  = f"Наценка:           {term['markup_pct']}%\n" if term["markup_pct"] else ""
    base_label   = "остаток" if on_remaining else "вся сумма"
    result = (
        f"*Расчёт рассрочки*\n\n"
        f"Квартира:          {layout['name']}, {layout['area']} м²\n"
        f"Этаж:              {group['label']}\n"
        f"{disc_line}"
        f"Стоимость:         {fmt(total_price)} руб\n"
        f"\n{down_line}"
        f"Остаток:           {fmt(remaining)} руб\n"
        f"\nРассрочка на:      {base_label} ({fmt(base)} руб)\n"
        f"Срок:              {term['months']} месяцев\n"
        f"{markup_line}"
        f"Итого по рассрочке: {fmt(total_debt)} руб\n"
        f"\nЕжемесячный платёж: *{fmt(monthly)} руб*"
    )
    await cq.edit_message_text(result, parse_mode="Markdown")
    context.user_data.clear()
    return ConversationHandler.END


async def uc_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Расчёт отменён.")
    else:
        await update.message.reply_text("Расчёт отменён.")
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Admin: /setcount  — update layout apartment count
# ---------------------------------------------------------------------------

SC_PICK_APT, SC_PICK_GROUP, SC_PICK_LAYOUT, SC_ENTER_COUNT = range(60, 64)


async def setcount_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    apts = [a for a in db.get_all_apartments() if db.get_calculator(a["id"])]
    if not apts:
        await update.message.reply_text("Нет ЖК с настроенным калькулятором.")
        return ConversationHandler.END
    context.user_data.clear()
    keyboard = [[InlineKeyboardButton(a["name"], callback_data=f"sc_apt|{a['id']}")] for a in apts]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="sc_cancel")])
    await update.message.reply_text("Выберите ЖК:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SC_PICK_APT


async def sc_pick_apt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "sc_cancel":
        context.user_data.clear()
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    apt_id = cq.data.split("|", 1)[1]
    calc = db.get_calculator(apt_id)
    if not calc:
        await cq.edit_message_text("Калькулятор не найден.")
        return ConversationHandler.END
    context.user_data["sc_apt_id"] = apt_id
    context.user_data["sc_calc"] = calc
    groups = calc["floor_groups"]
    keyboard = [[InlineKeyboardButton(f"{g['label']} эт.", callback_data=f"sc_grp|{i}")] for i, g in enumerate(groups)]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="sc_cancel")])
    await cq.edit_message_text("Выберите группу этажей:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SC_PICK_GROUP


async def sc_pick_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "sc_cancel":
        context.user_data.clear()
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    g_idx = int(cq.data.split("|", 1)[1])
    context.user_data["sc_g_idx"] = g_idx
    calc = context.user_data["sc_calc"]
    layouts = calc["floor_groups"][g_idx]["layouts"]
    keyboard = [
        [InlineKeyboardButton(
            f"{l['name']} {l['area']} м²" + (f" ({l['count']} кв.)" if l.get("count") else ""),
            callback_data=f"sc_lay|{i}",
        )]
        for i, l in enumerate(layouts)
    ]
    keyboard.append([InlineKeyboardButton("Отмена", callback_data="sc_cancel")])
    await cq.edit_message_text("Выберите планировку:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SC_PICK_LAYOUT


async def sc_pick_layout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    cq = update.callback_query
    await cq.answer()
    if cq.data == "sc_cancel":
        context.user_data.clear()
        await cq.edit_message_text("Отменено.")
        return ConversationHandler.END
    l_idx = int(cq.data.split("|", 1)[1])
    context.user_data["sc_l_idx"] = l_idx
    calc = context.user_data["sc_calc"]
    g_idx = context.user_data["sc_g_idx"]
    layout = calc["floor_groups"][g_idx]["layouts"][l_idx]
    cur_count = layout.get("count", "не указано")
    await cq.edit_message_text(
        f"Планировка: {layout['name']} {layout['area']} м²\n"
        f"Текущее количество: {cur_count}\n\n"
        "Введите новое количество квартир:"
    )
    return SC_ENTER_COUNT


async def sc_enter_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip().replace(" ", "")
    if not raw.isdigit() or int(raw) <= 0:
        await update.message.reply_text("Введите целое число больше 0:")
        return SC_ENTER_COUNT
    count = int(raw)
    apt_id = context.user_data["sc_apt_id"]
    calc = context.user_data["sc_calc"]
    g_idx = context.user_data["sc_g_idx"]
    l_idx = context.user_data["sc_l_idx"]
    calc["floor_groups"][g_idx]["layouts"][l_idx]["count"] = count
    db.save_calculator(apt_id, calc)
    layout = calc["floor_groups"][g_idx]["layouts"][l_idx]
    context.user_data.clear()
    await update.message.reply_text(
        f"Обновлено: {layout['name']} {layout['area']} м² — {count} кв."
    )
    return ConversationHandler.END


async def setcount_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в .env файле")

    asyncio.set_event_loop(asyncio.new_event_loop())
    db.init_db()
    refresh_index()
    refresh_admins()
    logger.info("Loaded %d apartments", len(_index))

    async def post_init(application) -> None:
        await application.bot.set_my_commands(
            [
                BotCommand("start",     "Начать / помощь"),
                BotCommand("subscribe", "Оформить подписку"),
                BotCommand("list",     "Список всех ЖК"),
                BotCommand("browse",   "Подбор ЖК по районам"),
            ],
            scope=BotCommandScopeDefault(),
        )
        for admin_id in _ENV_ADMINS | _db_admins:
            await _set_admin_commands(application.bot, admin_id)

    app = Application.builder().token(TOKEN).post_init(post_init).build()

    post_conv = ConversationHandler(
        entry_points=[CommandHandler("post", post_start)],
        states={
            S_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, s_name)],
            S_ALIASES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, s_aliases),
                CallbackQueryHandler(s_aliases_skip, pattern=r"^post_skip_aliases$"),
            ],
            S_DISTRICT: [CallbackQueryHandler(s_district, pattern=r"^pd\|")],
            S_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, s_address)],
            S_PRICE:      [MessageHandler(filters.TEXT & ~filters.COMMAND, s_price)],
            S_INST_PRICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, s_inst_price),
                CallbackQueryHandler(s_inst_price_skip, pattern=r"^post_skip_inst_price$"),
            ],
            S_DESC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, s_desc),
                CallbackQueryHandler(s_desc_skip, pattern=r"^post_skip_desc$"),
            ],
            S_PHOTO:   [MessageHandler(filters.PHOTO | (filters.TEXT & ~filters.COMMAND), s_photo)],
            S_PHOTOS: [
                MessageHandler(filters.PHOTO, s_photos),
                MessageHandler(filters.TEXT & ~filters.COMMAND, s_photos),
                CallbackQueryHandler(s_photos_done, pattern=r"^photos_(done|skip)$"),
            ],
            S_LAYOUTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, s_layouts)],
            S_CHESS:   [MessageHandler(filters.TEXT & ~filters.COMMAND, s_chess)],
            S_INST:    [MessageHandler(filters.TEXT & ~filters.COMMAND, s_inst)],
            S_CONFIRM: [CallbackQueryHandler(s_confirm, pattern=r"^post_(save|discard)$")],
        },
        fallbacks=[CommandHandler("cancel", post_cancel)],
        per_message=False,
    )

    edit_conv = ConversationHandler(
        entry_points=[CommandHandler("edit", edit_cmd)],
        states={
            EDIT_PICK_APT:   [CallbackQueryHandler(edit_pick_apt,   pattern=r"^(ea\||ecx$)")],
            EDIT_PICK_FIELD: [CallbackQueryHandler(edit_pick_field, pattern=r"^(ef\||ecx$)")],
            EDIT_VALUE: [
                CallbackQueryHandler(edit_photos_done, pattern=r"^edit_photos_(done|clear)$"),
                MessageHandler(filters.PHOTO, edit_value),
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_value),
            ],
            EDIT_AFTER:    [CallbackQueryHandler(edit_after,    pattern=r"^edit_(more|done)$")],
            EDIT_DISTRICT: [CallbackQueryHandler(edit_district, pattern=r"^(ed_dist\||ed_dist_back$)")],
        },
        fallbacks=[CommandHandler("cancel", edit_cancel)],
        per_message=False,
    )

    calc_setup_conv = ConversationHandler(
        entry_points=[CommandHandler("calc", calc_cmd)],
        states={
            CS_PICK_APT:     [CallbackQueryHandler(cs_pick_apt,    pattern=r"^(cpa\||csx$)")],
            CS_ASK_ACTION:   [CallbackQueryHandler(cs_ask_action,  pattern=r"^(csa_|csx$)")],
            CS_FLOOR_LABEL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, cs_floor_label)],
            CS_FLOOR_PRICE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, cs_floor_price)],
            CS_LAYOUT_NAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, cs_layout_name)],
            CS_LAYOUT_AREA:  [MessageHandler(filters.TEXT & ~filters.COMMAND, cs_layout_area)],
            CS_LAYOUT_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cs_layout_count),
                CallbackQueryHandler(cs_layout_count, pattern=r"^cs_skip_count$"),
            ],
            CS_LAYOUT_MORE:  [CallbackQueryHandler(cs_layout_more)],
            CS_MORE:         [CallbackQueryHandler(cs_more)],
            CS_MIN_DOWN:     [MessageHandler(filters.TEXT & ~filters.COMMAND, cs_min_down)],
            CS_DISCOUNTS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cs_discounts),
                CallbackQueryHandler(cs_discounts, pattern=r"^cs_skip_discounts$"),
            ],
            CS_TERMS:     [MessageHandler(filters.TEXT & ~filters.COMMAND, cs_terms)],
            CS_MANDATORY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cs_mandatory),
                CallbackQueryHandler(cs_mandatory, pattern=r"^cs_skip_mandatory$"),
            ],
            CS_INST_BASE: [CallbackQueryHandler(cs_inst_base, pattern=r"^csib\|")],
            # Calculator editor states
            CE_PICK_FIELD:  [CallbackQueryHandler(ce_pick_field,   pattern=r"^ce_f\|")],
            CE_PRICE_GROUP: [CallbackQueryHandler(ce_price_group,  pattern=r"^(cepg\||ce_back$)")],
            CE_PRICE_VALUE: [
                CallbackQueryHandler(ce_price_value, pattern=r"^ce_back_price_grp$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ce_price_value),
            ],
            CE_COUNT_GROUP: [CallbackQueryHandler(ce_count_group,  pattern=r"^(cecg\||ce_back$)")],
            CE_COUNT_LAYOUT:[CallbackQueryHandler(ce_count_layout, pattern=r"^(cecl\||ce_back_grp$)")],
            CE_COUNT_VALUE: [
                CallbackQueryHandler(ce_count_value, pattern=r"^ce_back_cnt_lay$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ce_count_value),
            ],
            CE_MIN_DOWN: [
                CallbackQueryHandler(ce_min_down, pattern=r"^ce_back_mindown$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ce_min_down),
            ],
            CE_DISCOUNTS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ce_discounts),
                CallbackQueryHandler(ce_discounts, pattern=r"^ce_(skip_discounts|back_discounts)$"),
            ],
            CE_TERMS: [
                CallbackQueryHandler(ce_terms, pattern=r"^ce_back_terms$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ce_terms),
            ],
            CE_MANDATORY_VALUE: [
                CallbackQueryHandler(ce_mandatory_value, pattern=r"^ce_(clear_mandatory|back_mandatory)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ce_mandatory_value),
            ],
            CE_INST_BASE_EDIT: [CallbackQueryHandler(ce_inst_base_edit, pattern=r"^ceib\|")],
        },
        fallbacks=[CommandHandler("cancel", calc_cancel)],
        per_message=False,
    )

    user_calc_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(uc_start, pattern=r"^calc\|")],
        states={
            UC_LAYOUT: [CallbackQueryHandler(uc_layout)],
            UC_DOWN:   [MessageHandler(filters.TEXT & ~filters.COMMAND, uc_down)],
            UC_TERM:   [CallbackQueryHandler(uc_term)],
        },
        fallbacks=[
            CommandHandler("cancel", uc_cancel),
            CallbackQueryHandler(uc_cancel, pattern=r"^uc_cancel$"),
        ],
        per_message=False,
    )

    setcount_conv = ConversationHandler(
        entry_points=[CommandHandler("setcount", setcount_cmd)],
        states={
            SC_PICK_APT:    [CallbackQueryHandler(sc_pick_apt,    pattern=r"^sc_(apt\||cancel)")],
            SC_PICK_GROUP:  [CallbackQueryHandler(sc_pick_group,  pattern=r"^sc_(grp\||cancel)")],
            SC_PICK_LAYOUT: [CallbackQueryHandler(sc_pick_layout, pattern=r"^sc_(lay\||cancel)")],
            SC_ENTER_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, sc_enter_count)],
        },
        fallbacks=[CommandHandler("cancel", setcount_cancel)],
        per_message=False,
    )

    subprice_conv = ConversationHandler(
        entry_points=[CommandHandler("subprice", subprice_cmd)],
        states={
            SP_PICK_PLAN:   [CallbackQueryHandler(sp_pick_plan,   pattern=r"^sp\|")],
            SP_ENTER_RUB:   [MessageHandler(filters.TEXT & ~filters.COMMAND, sp_enter_rub)],
            SP_ENTER_STARS: [MessageHandler(filters.TEXT & ~filters.COMMAND, sp_enter_stars)],
        },
        fallbacks=[CommandHandler("cancel", subprice_cancel)],
        per_message=False,
    )

    app.add_handler(CommandHandler("start",          start))
    app.add_handler(CommandHandler("subscribe",      subscribe_cmd))
    app.add_handler(CommandHandler("list",           list_cmd))
    app.add_handler(CommandHandler("browse",         browse_cmd))
    app.add_handler(CommandHandler("delete",         delete_cmd))
    app.add_handler(CommandHandler("addadmin",       addadmin_cmd))
    app.add_handler(CommandHandler("removeadmin",    removeadmin_cmd))
    app.add_handler(CommandHandler("listadmins",     listadmins_cmd))
    app.add_handler(CommandHandler("adddistrict",    adddistrict_cmd))
    app.add_handler(CommandHandler("removedistrict", removedistrict_cmd))
    app.add_handler(CommandHandler("listdistricts",  listdistricts_cmd))
    app.add_handler(CommandHandler("subscribers",    subscribers_cmd))
    app.add_handler(CommandHandler("addsub",         addsub_cmd))
    app.add_handler(CommandHandler("delsub",         delsub_cmd))
    app.add_handler(post_conv)
    app.add_handler(edit_conv)
    app.add_handler(calc_setup_conv)
    app.add_handler(setcount_conv)
    app.add_handler(user_calc_conv)
    app.add_handler(subprice_conv)
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))
    app.add_handler(CallbackQueryHandler(sub_plans_callback,    pattern=r"^sub_plans$"))
    app.add_handler(CallbackQueryHandler(sub_plan_callback,     pattern=r"^sub\|"))
    app.add_handler(CallbackQueryHandler(sub_pay_method_callback, pattern=r"^subpay\|"))
    app.add_handler(CallbackQueryHandler(del_ask_callback,    pattern=r"^del_ask\|"))
    app.add_handler(CallbackQueryHandler(del_yes_callback,    pattern=r"^del_yes\|"))
    app.add_handler(CallbackQueryHandler(del_cancel_callback, pattern=r"^del_cancel$"))
    app.add_handler(CallbackQueryHandler(installment_callback,    pattern=r"^inst\|"))
    app.add_handler(CallbackQueryHandler(aptphotos_callback,      pattern=r"^aptphotos\|"))
    app.add_handler(CallbackQueryHandler(setup_calc_callback,     pattern=r"^setup_calc"))
    app.add_handler(CallbackQueryHandler(browse_district_callback, pattern=r"^br_d\|"))
    app.add_handler(CallbackQueryHandler(browse_apt_callback,      pattern=r"^br_a\|"))
    app.add_handler(CallbackQueryHandler(browse_back_callback,     pattern=r"^br_back$"))
    app.add_handler(CallbackQueryHandler(removedistrict_callback,  pattern=r"^(rmd\||rmd_cancel$)"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_apartment))

    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
