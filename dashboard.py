import discord
from discord.ext import commands
from discord.ui import View, Select, Button
import json
import os
import time

# --- CONFIGURATION ---
ALLIANCES = ["[NVR1] Main", "[NVR2] Shell", "[NVR3] Shell", "[NVR4] Shell", 
             "[NVR5] Shell", "[NVR6] Shell", "[NVR7] Shell", "[NVR8] Shell"]

COOLDOWNS = {
    "bastion": 12 * 3600,       
    "build_buff": 72 * 3600,    
    "storm": 36 * 3600,         
    "fog": 48 * 3600            
}

DATA_FILE = "dashboard_data.json"

# --- DATA MANAGEMENT ---
def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {
        "dashboard_channel_id": None,
        "dashboard_message_id": None,
        "cooldowns": {alliance: {"bastion": 0, "build_buff": 0, "storm": 0, "fog": 0} for alliance in ALLIANCES}
    }

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)

# --- EMBED GENERATOR ---
def generate_dashboard_embed(db):
    embed = discord.Embed(
        title="🛡️ NVR ALLIANCE DASHBOARD",
        description="*Live readiness state for all shell alliances.*\n\u200b",
        color=discord.Color.gold()
    )
    current_time = int(time.time())
    
    for alliance in ALLIANCES:
        timers = db["cooldowns"].get(alliance, {})
        
        # Bastion
        bastion_ready = timers.get("bastion", 0) < current_time
        bastion_str = "🟢 **READY**" if bastion_ready else f"🔴 Cooldown (<t:{timers['bastion']}:R>)"
        
        # Build Buff
        bb_end = timers.get("build_buff", 0)
        if bb_end < current_time:
            bb_str = "🟢 **READY**"
        elif bb_end - current_time > (71 * 3600): 
            bb_str = f"🟠 **ACTIVE** (Ends <t:{bb_end - (71*3600)}:R>)"
        else:
            bb_str = f"🔴 Cooldown (<t:{bb_end}:R>)"

        # Storm Vanguard
        storm_ready = timers.get("storm", 0) < current_time
        storm_str = "🟢 **READY**" if storm_ready else f"🔴 Cooldown (<t:{timers['storm']}:R>)"
        
        # Fog of War
        fog_ready = timers.get("fog", 0) < current_time
        fog_str = "🟢 **READY**" if fog_ready else f"🔴 Cooldown (<t:{timers['fog']}:R>)"
        
        # Formatted into clean double-column blockquotes per alliance
        field_value = (
            f"> 🏰 **Bastion:** {bastion_str}\n"
            f"> 🔨 **Build Buff:** {bb_str}\n"
            f"> ⚡ **Storm Vanguard:** {storm_str}\n"
            f"> 🌫️ **Fog of War:** {fog_str}\n"
        )
        
        embed.add_field(name=f"🛡️ {alliance}", value=field_value, inline=False)
        
    embed.set_footer(text="Dashboard updates automatically when officers trigger actions.")
    return embed


# --- UI COMPONENTS ---
# --- UI COMPONENTS ---
class OfficerPanel(View):
    def __init__(self, bot, db):
        super().__init__(timeout=None)
        self.bot = bot
        self.db = db
        self.selected_alliance = None
        self.last_action = None  # Stores (alliance, action_key, previous_timestamp, action_name)

        options = [discord.SelectOption(label=a, value=a) for a in ALLIANCES]
        self.select = Select(placeholder="1. Select Target Alliance...", options=options, custom_id="select_alliance")
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        self.selected_alliance = self.select.values[0]
        await interaction.response.send_message(f"✅ Selected **{self.selected_alliance}**. Choose an action below.", ephemeral=True)

    async def update_dashboard_message(self):
        channel_id = self.db.get("dashboard_channel_id")
        message_id = self.db.get("dashboard_message_id")
        if channel_id and message_id:
            channel = self.bot.get_channel(channel_id)
            if channel:
                try:
                    msg = await channel.fetch_message(message_id)
                    await msg.edit(embed=generate_dashboard_embed(self.db))
                except discord.NotFound:
                    pass
    LOG_CHANNEL_ID = 1527938722987900978

    async def handle_action(self, interaction: discord.Interaction, action_key: str, action_name: str):
        if not self.selected_alliance:
            await interaction.response.send_message("❌ Please select an alliance from the dropdown menu first!", ephemeral=True)
            return

        current_time = int(time.time())
        if self.selected_alliance not in self.db["cooldowns"]:
            self.db["cooldowns"][self.selected_alliance] = {}
        
        # Store previous value for Undo tracking
        previous_val = self.db["cooldowns"][self.selected_alliance].get(action_key, 0)
        self.last_action = (self.selected_alliance, action_key, previous_val, action_name)

        self.db["cooldowns"][self.selected_alliance][action_key] = current_time + COOLDOWNS[action_key]
        save_data(self.db)
    
        # 1. Private reply so the officer knows it worked without cluttering the channel
        await interaction.response.send_message(f"✅ Triggered **{action_name}** for **{self.selected_alliance}**.", ephemeral=True)
    
        # 2. Update the public dashboard embed
        await self.update_dashboard_message()

        # 3. Send the public log to your audit channel
        log_channel = interaction.guild.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            await log_channel.send(f"✅ **{interaction.user.display_name}** triggered **{action_name}** for **{self.selected_alliance}**.")

    # --- COOLDOWN TRIGGER BUTTONS ---
    @discord.ui.button(label="🏰 Bastion Removed (12h)", style=discord.ButtonStyle.danger, custom_id="btn_bastion", row=1)
    async def btn_bastion(self, interaction: discord.Interaction, button: Button):
        await self.handle_action(interaction, "bastion", "Bastion Removal")

    @discord.ui.button(label="🔨 Build Buff (1h Act/72h CD)", style=discord.ButtonStyle.primary, custom_id="btn_build", row=1)
    async def btn_build(self, interaction: discord.Interaction, button: Button):
        await self.handle_action(interaction, "build_buff", "Build Buff")

    @discord.ui.button(label="⚡ Storm Vanguard (36h)", style=discord.ButtonStyle.primary, custom_id="btn_storm", row=2)
    async def btn_storm(self, interaction: discord.Interaction, button: Button):
        await self.handle_action(interaction, "storm", "Storm Vanguard")

    @discord.ui.button(label="🌫️ Fog of War (48h)", style=discord.ButtonStyle.secondary, custom_id="btn_fog", row=2)
    async def btn_fog(self, interaction: discord.Interaction, button: Button):
        await self.handle_action(interaction, "fog", "Fog of War")

    # --- RESET & UNDO CONTROLS ---
    @discord.ui.button(label="↩️ Undo Last Action", style=discord.ButtonStyle.secondary, custom_id="btn_undo", row=3)
    async def btn_undo(self, interaction: discord.Interaction, button: Button):
        if not self.last_action:
            await interaction.response.send_message("❌ No recent action available to undo!", ephemeral=True)
            return

        alliance, action_key, previous_val, action_name = self.last_action
        self.db["cooldowns"][alliance][action_key] = previous_val
        save_data(self.db)
        self.last_action = None  # Clear undo stack after use

        await interaction.response.send_message(f"↩️ **{interaction.user.display_name}** reversed the last **{action_name}** for **{alliance}**.", ephemeral=False)
        await self.update_dashboard_message()

    @discord.ui.button(label="🔄 Reset Selected Shell", style=discord.ButtonStyle.danger, custom_id="btn_reset_shell", row=3)
    async def btn_reset_shell(self, interaction: discord.Interaction, button: Button):
        if not self.selected_alliance:
            await interaction.response.send_message("❌ Please select an alliance from the dropdown menu first!", ephemeral=True)
            return

        self.db["cooldowns"][self.selected_alliance] = {"bastion": 0, "build_buff": 0, "storm": 0, "fog": 0}
        save_data(self.db)

        await interaction.response.send_message(f"🔄 **{interaction.user.display_name}** reset all cooldowns for **{self.selected_alliance}**.", ephemeral=False)
        await self.update_dashboard_message()

# --- COG SETUP ---
class AllianceDashboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = load_data()

    async def cog_load(self):
        # Natively registers the persistent view so buttons work after bot restarts
        self.bot.add_view(OfficerPanel(self.bot, self.db))

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setup_dashboard(self, ctx):
        """Run this in #alliance-dashboard to spawn the read-only embed."""
        embed = generate_dashboard_embed(self.db)
        msg = await ctx.send(embed=embed)
        
        self.db["dashboard_channel_id"] = msg.channel.id
        self.db["dashboard_message_id"] = msg.id
        save_data(self.db)
        await ctx.message.delete()

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setup_panel(self, ctx):
        """Run this in #officer-control-panel to spawn the interactive buttons."""
        embed = discord.Embed(
            title="🛠️ OFFICER COORDINATION PANEL",
            description="1. Select the alliance from the dropdown.\n2. Click the skill/building button to start the cooldown.",
            color=discord.Color.gold()
        )
        await ctx.send(embed=embed, view=OfficerPanel(self.bot, self.db))
        await ctx.message.delete()

async def setup(bot):
    await bot.add_cog(AllianceDashboard(bot))
