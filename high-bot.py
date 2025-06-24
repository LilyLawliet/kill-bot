import discord 
from discord.ext import commands, tasks
import os, asyncio, time, json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from discord.ui import View, Select
from discord import Permissions, PermissionOverwrite

# Load ENV
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")


intents = discord.Intents.all()
bot = commands.Bot(command_prefix='.', intents=intents)
bot.remove_command("help")

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

LB_MESSAGES_FILE = "leaderboard_messages.json"

class UserActivity(Base):
    __tablename__ = "user_activity"
    user_id = Column(String, primary_key=True)
    guild_id = Column(String, primary_key=True)
    messages = Column(Integer, default=0)
    vc_minutes = Column(Integer, default=0)

class UserActivityHistory(Base):
    __tablename__ = "user_activity_history"
    user_id = Column(String, primary_key=True)
    guild_id = Column(String, primary_key=True)
    time_scope = Column(String, primary_key=True)
    messages = Column(Integer, default=0)
    vc_minutes = Column(Integer, default=0)

class VCSession(Base):
    __tablename__ = "vc_sessions"
    user_id = Column(String, primary_key=True)
    guild_id = Column(String, primary_key=True)
    joined_at = Column(Float)


Base.metadata.create_all(engine)

PK_TIMEZONE = timezone.utc

def get_time_keys():
    now = datetime.now(PK_TIMEZONE)
    return {
        "daily": now.strftime("%Y-%m-%d"),
        "weekly": f"{now.year}-W{now.isocalendar()[1]}",
        "monthly": now.strftime("%Y-%m"),
    }

def get_next_reset(scope):
    now = datetime.now(timezone.utc)
    if scope == "daily":
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) - now
    elif scope == "weekly":
        days_until_monday = (7 - now.weekday()) % 7 or 7
        next_monday = now + timedelta(days=days_until_monday)
        return next_monday.replace(hour=0, minute=0, second=0, microsecond=0) - now
    elif scope == "monthly":
        year, month = (now.year + 1, 1) if now.month == 12 else (now.year, now.month + 1)
        next_month = now.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
        return next_month - now

def format_minutes(total_minutes):
    days, remainder = divmod(total_minutes, 1440)
    hours, minutes = divmod(remainder, 60)
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0 or days > 0:
        parts.append(f"{hours}h")
    parts.append(f"{minutes} min")
    return " ".join(parts)

def load_json(file):
    return json.load(open(file)) if os.path.exists(file) else {}

def save_json(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=4)

def update_activity(guild_id, user_id, key, amount):
    session = SessionLocal()
    user_id = str(user_id)
    guild_id = str(guild_id)
    try:
        user = session.query(UserActivity).filter_by(user_id=user_id, guild_id=guild_id).first()
        if not user:
            user = UserActivity(user_id=user_id, guild_id=guild_id)
            session.add(user)
        setattr(user, key, (getattr(user, key) or 0) + amount)

        for scope, t_key in get_time_keys().items():
            history = session.query(UserActivityHistory).filter_by(
                user_id=user_id, guild_id=guild_id, time_scope=t_key
            ).first()
            if not history:
                history = UserActivityHistory(user_id=user_id, guild_id=guild_id, time_scope=t_key)
                session.add(history)
            setattr(history, key, (getattr(history, key) or 0) + amount)

        session.commit()
    finally:
        session.close()

@bot.event
async def on_message(message):
    if not message.author.bot and message.guild:
        update_activity(message.guild.id, message.author.id, "messages", 1)
    await bot.process_commands(message)

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot or not member.guild:
        return
    uid = str(member.id)
    gid = str(member.guild.id)
    session = SessionLocal()
    try:
        if before.channel is None and after.channel:
            vc = session.query(VCSession).filter_by(user_id=uid, guild_id=gid).first()
            if not vc:
                session.add(VCSession(user_id=uid, guild_id=gid, joined_at=time.time()))
            else:
                vc.joined_at = time.time()
        elif before.channel and after.channel is None:
            vc = session.query(VCSession).filter_by(user_id=uid, guild_id=gid).first()
            if vc:
                minutes = int((time.time() - vc.joined_at) / 60)
                session.delete(vc)
                if minutes > 0:
                    update_activity(member.guild.id, uid, "vc_minutes", minutes)
        session.commit()
    finally:
        session.close()

async def build_leaderboard(guild, mode, scope):
    session = SessionLocal()
    try:
        key = get_time_keys().get(scope) if scope != "all" else None
        gid = str(guild.id)
        if key:
            rows = session.query(UserActivityHistory).filter_by(guild_id=gid, time_scope=key).all()
        else:
            rows = session.query(UserActivity).filter_by(guild_id=gid).all()

        entries = []
        for r in rows:
            value = getattr(r, mode)
            if value > 0:
                member = guild.get_member(int(r.user_id))
                if member and not member.bot:
                    entries.append((r.user_id, value))

        entries.sort(key=lambda x: x[1], reverse=True)

        mode_label = "VC" if mode == "vc_minutes" else "Messages"
        display_title = f"{scope.capitalize()} {mode_label} Leaderboard" if scope != "all" else f"All Time {mode_label} Leaderboard"

        embed = discord.Embed(color=0xD8B4E2)
        embed.set_author(name=display_title, icon_url=guild.icon.url if guild.icon else None)

        if not entries:
            embed.description = "*No data available yet.*"
        else:
            lines = []
            for i, (uid, val) in enumerate(entries[:10], 1):
                member = guild.get_member(int(uid))
                mention = member.mention if member else f"<@{uid}>"
                value = format_minutes(val) if mode == "vc_minutes" else f"{val:,}"
                lines.append(f"**#{i}** ‣ {mention}  ━  `{value}`")

            embed.description = "\n".join(lines)

        scope_label = scope.capitalize() + " Stats" if scope != "all" else "All Time Stats"
        embed.set_footer(text=scope_label)
        embed.timestamp = datetime.now(timezone.utc)
        return embed
    finally:
        session.close()


class ScopeDropdown(Select):
    def __init__(self, guild, mode):
        self.guild = guild
        self.mode = mode
        options = [
            discord.SelectOption(label="All Time", value="all"),
            discord.SelectOption(label="Monthly", value="monthly"),
            discord.SelectOption(label="Weekly", value="weekly"),
            discord.SelectOption(label="Daily", value="daily"),
        ]
        super().__init__(placeholder="Select leaderboard scope...", options=options)

    async def callback(self, interaction: discord.Interaction):
        scope = self.values[0]
        embed = await build_leaderboard(self.guild, self.mode, scope)
        await interaction.response.edit_message(embed=embed, view=self.view)

class LeaderboardDropdownView(View):
    def __init__(self, guild, mode):
        super().__init__(timeout=None)
        self.add_item(ScopeDropdown(guild, mode))

@bot.command()
async def stats(ctx, member: discord.Member = None):
    user = member or ctx.author
    uid = str(user.id)
    gid = str(ctx.guild.id)
    session = SessionLocal()

    def get_stat_lines(u):
        lines = []
        now_keys = get_time_keys()

        for scope in ["daily", "weekly", "monthly", "all"]:
            if scope == "all":
                udata = session.query(UserActivity).filter_by(user_id=uid, guild_id=gid).first()
                all_rows = session.query(UserActivity).filter_by(guild_id=gid).all()
            else:
                key = now_keys[scope]
                udata = session.query(UserActivityHistory).filter_by(
                    user_id=uid, guild_id=gid, time_scope=key
                ).first()
                all_rows = session.query(UserActivityHistory).filter_by(
                    guild_id=gid, time_scope=key
                ).all()

            messages = udata.messages if udata else 0
            vc_minutes = udata.vc_minutes if udata else 0

            # Filter out bots and get top ranks for this guild
            msg_sorted = []
            vc_sorted = []
            for r in all_rows:
                member = ctx.guild.get_member(int(r.user_id))
                if not member or member.bot:
                    continue
                if r.messages > 0:
                    msg_sorted.append((r.user_id, r.messages))
                if r.vc_minutes > 0:
                    vc_sorted.append((r.user_id, r.vc_minutes))

            msg_sorted.sort(key=lambda x: x[1], reverse=True)
            vc_sorted.sort(key=lambda x: x[1], reverse=True)

            msg_rank = next((i + 1 for i, (uidx, _) in enumerate(msg_sorted) if uidx == uid), None)
            vc_rank = next((i + 1 for i, (uidx, _) in enumerate(vc_sorted) if uidx == uid), None)

            lines.append((
                scope.capitalize(),
                messages,
                vc_minutes,
                f"**Rank #{msg_rank}**" if msg_rank else "**No Rank**",
                f"**Rank #{vc_rank}**" if vc_rank else "**No Rank**"
            ))
        return lines

    class RankDropdown(discord.ui.Select):
        def __init__(self, stats):
            options = [
                discord.SelectOption(label=scope, value=scope.lower())
                for scope, _, _, _, _ in stats
            ]
            super().__init__(placeholder="Select a timeframe...", options=options)
            self.stats = {
                scope.lower(): (scope, msgs, vc, msg_rank, vc_rank)
                for scope, msgs, vc, msg_rank, vc_rank in stats
            }

        async def callback(self, interaction: discord.Interaction):
            if interaction.user != ctx.author:
                return await interaction.response.send_message(
                    "Only the requester can use this menu.", ephemeral=True
                )

            scope, messages, vc, msg_rank, vc_rank = self.stats[self.values[0]]
            scope_label = f"{scope} Stats" if scope != "All" else "All Time Stats"

            embed = discord.Embed(
                description=(
                    f"**Stats for {user.mention}:**\n"
                    f"**{scope} Messages:** {messages:,} ({msg_rank})\n"
                    f"**{scope} Voice Time:** {format_minutes(vc)} ({vc_rank})"
                ),
                color=0xD8B4E2
            )
            embed.set_footer(text=scope_label)
            embed.timestamp = datetime.now(timezone.utc)
            await interaction.response.edit_message(embed=embed)

    class RankView(discord.ui.View):
        def __init__(self, stats):
            super().__init__(timeout=60)
            self.add_item(RankDropdown(stats))

    try:
        stats = get_stat_lines(user)
        scope, messages, vc, msg_rank, vc_rank = stats[0]
        scope_label = f"{scope} Stats" if scope != "All" else "All Time Stats"

        embed = discord.Embed(
            description=(
                f"**Stats for {user.mention}:**\n"
                f"**{scope} Messages:** {messages:,} ({msg_rank})\n"
                f"**{scope} Voice Time:** {format_minutes(vc)} ({vc_rank})"
            ),
            color=0xD8B4E2
        )
        embed.set_footer(text=scope_label)
        embed.timestamp = datetime.now(timezone.utc)

        await ctx.send(embed=embed, view=RankView(stats))
    finally:
        session.close()


@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    for guild in bot.guilds:
        await ensure_channels_and_messages(guild)
    await update_all_leaderboards_once()
    leaderboard_updater.start()

async def ensure_channels_and_messages(guild):
    config = load_json(LB_MESSAGES_FILE)

    for ch_type in ["msg-lb", "vc-lb"]:
        channel = discord.utils.get(guild.text_channels, name=ch_type)

        # Permission overwrites
        overwrites = {
            guild.default_role: PermissionOverwrite(view_channel=False)
        }

        # Allow view-only access to admins
        for member in guild.members:
            if member.guild_permissions.administrator:
                overwrites[member] = PermissionOverwrite(view_channel=True, read_messages=True, send_messages=False)

        # ✅ Allow full access to the bot itself
        overwrites[guild.me] = PermissionOverwrite(view_channel=True, read_messages=True, send_messages=True)

        # Create channel if not exists
        if not channel:
            channel = await guild.create_text_channel(ch_type, overwrites=overwrites)

        config.setdefault(str(guild.id), {}).setdefault(ch_type, {})

        for scope in ["all", "monthly", "weekly", "daily"]:
            if scope in config[str(guild.id)][ch_type]:
                continue

            mode = "messages" if ch_type == "msg-lb" else "vc_minutes"
            embed = await build_leaderboard(guild, mode, scope)

            try:
                msg = await channel.send(embed=embed)
                config[str(guild.id)][ch_type][scope] = msg.id
            except discord.Forbidden:
                print(f"❌ Bot missing access to send message in {channel.name} of {guild.name}")

    save_json(LB_MESSAGES_FILE, config)

@tasks.loop(minutes=10)
async def leaderboard_updater():
    config = load_json(LB_MESSAGES_FILE)
    for guild in bot.guilds:
        guild_config = config.get(str(guild.id), {})
        for ch_type in ["msg-lb", "vc-lb"]:
            channel = discord.utils.get(guild.text_channels, name=ch_type)
            if not channel:
                continue
            for scope in ["all", "monthly", "weekly", "daily"]:
                try:
                    msg_id = guild_config.get(ch_type, {}).get(scope)
                    if not msg_id:
                        continue
                    msg = await channel.fetch_message(msg_id)
                    mode = "messages" if ch_type == "msg-lb" else "vc_minutes"
                    embed = await build_leaderboard(guild, mode, scope)
                    await msg.edit(embed=embed)
                    await asyncio.sleep(1.5)
                except Exception as e:
                    print(f"❌ Failed to update {ch_type}/{scope} in {guild.name}: {e}")

async def update_all_leaderboards_once():
    config = load_json(LB_MESSAGES_FILE)
    for guild in bot.guilds:
        guild_config = config.get(str(guild.id), {})
        for ch_type in ["msg-lb", "vc-lb"]:
            channel = discord.utils.get(guild.text_channels, name=ch_type)
            if not channel:
                continue
            if "main" in guild_config.get(ch_type, {}):
                try:
                    msg_id = guild_config[ch_type]["main"]
                    msg = await channel.fetch_message(msg_id)
                    mode = "messages" if ch_type == "msg-lb" else "vc_minutes"
                    embed = await build_leaderboard(guild, mode, "all")
                    view = LeaderboardDropdownView(guild, mode)
                    await msg.edit(embed=embed, view=view)
                    await asyncio.sleep(1.5)
                except Exception as e:
                    print(f"❌ Failed to update {ch_type}/main in {guild.name}: {e}")

@bot.event
async def on_guild_join(guild):
    print(f"➕ Joined new guild: {guild.name}")
    await ensure_channels_and_messages(guild)


bot.run(BOT_TOKEN)
