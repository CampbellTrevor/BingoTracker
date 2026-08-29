import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import time
import re
import json
import requests

# --- Page Configuration ---
st.set_page_config(page_title="OSRS Bingo Tracker", layout="wide", page_icon="⚔️")
APP_DIR = Path(__file__).resolve().parent
DEFAULT_CSV_PATH = APP_DIR / "Copy of Copy of Winter Bingo 2026 - Event Log - New Log.csv"
WOM_CACHE_FILE = APP_DIR / "wom_group_cache.json"
WOM_API_BASE_URL = "https://api.wiseoldman.net/v2"
WOM_GROUP_ID = 11794
WOM_MAX_RETRIES = 5
WOM_BASE_BACKOFF_SECONDS = 1.5
WOM_PLAYER_ALIASES = {
    # CSV player name: Wise Old Man player name
    "Iron Thrage": "Thrayge",
    # "Stoke024": "Stoke 024",
    # "CIA PKed JFK": "ExactWOMNameIfDifferent",
}
SUPPORTED_WOM_BOSS_METRICS = {
    "abyssal_sire", "alchemical_hydra", "amoxliatl", "araxxor", "artio",
    "barrows_chests", "bryophyta", "callisto", "calvarion", "cerberus",
    "chambers_of_xeric", "chambers_of_xeric_challenge_mode", "chaos_elemental",
    "chaos_fanatic", "commander_zilyana", "corporeal_beast", "crazy_archaeologist",
    "dagannoth_prime", "dagannoth_rex", "dagannoth_supreme", "deranged_archaeologist",
    "doom_of_mokhaiotl", "duke_sucellus", "general_graardor", "giant_mole",
    "grotesque_guardians", "hespori", "kalphite_queen", "king_black_dragon",
    "kraken", "kreearra", "kril_tsutsaroth", "lunar_chests", "mimic",
    "nex", "nightmare", "obor", "phosanis_nightmare", "royal_titans",
    "scorpia", "skotizo", "sol_heredit", "spindel", "tempoross", "the_hueycoatl",
    "the_leviathan", "the_royal_titans", "the_whisperer", "theatre_of_blood",
    "theatre_of_blood_hard_mode", "thermonuclear_smoke_devil", "tombs_of_amascut",
    "tombs_of_amascut_expert", "tzkal_zuk", "tztok_jad", "vardorvis",
    "venenatis", "vetion", "vorkath", "wintertodt", "yama", "zalcano", "zulrah",
}

# Maps bingo categories to Wise Old Man boss metrics for KC gains.
CATEGORY_TO_WOM_BOSSES = {
    "Dagannoth Kings": ["dagannoth_prime", "dagannoth_rex", "dagannoth_supreme"],
    "Barrows / Moons": ["barrows_chests", "lunar_chests"],
    "Dragons": ["vorkath", "king_black_dragon"],
    "God Wars Dungeon": ["general_graardor", "kreearra", "commander_zilyana", "kril_tsutsaroth", "nex"],
    "Royal Titans": ["the_royal_titans"],
    "Tormented / Demonics": [],
    "Colo / Inferno": ["tzkal_zuk", "sol_heredit"],
    "DT2 Bosses": ["duke_sucellus", "the_leviathan", "the_whisperer", "vardorvis"],
    "Spider / Bear / Skeleton": ["callisto", "artio", "vetion", "calvarion", "venenatis", "spindel"],
    "Slayer Bosses": ["abyssal_sire", "alchemical_hydra", "cerberus", "grotesque_guardians", "kraken", "thermonuclear_smoke_devil"],
    "Zulrah": ["zulrah"],
    "Chambers of Xeric": ["chambers_of_xeric", "chambers_of_xeric_challenge_mode"],
    "Tombs of Amascut": ["tombs_of_amascut", "tombs_of_amascut_expert"],
    "Doom of Mokhaiotl": ["doom_of_mokhaiotl"],
    "Nex": ["nex"],
    "Yama": ["yama"],
    "Nightmare / PNM": ["nightmare", "phosanis_nightmare"],
    "Theatre of Blood": ["theatre_of_blood", "theatre_of_blood_hard_mode"],
    "Zalcano": ["zalcano"],
}

# --- 1. Data Cleaning Engine ---
@st.cache_data
def load_and_clean_data(file):
    try:
        # Load the CSV
        df = pd.read_csv(file)

        required_cols = ['Date', 'Player Name', 'Team', 'Tile', 'Item Received']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            st.error(
                f"Missing required columns: {missing_cols}. "
                f"Found: {df.columns.tolist()}"
            )
            return pd.DataFrame(), False

        # Remove the malformed/test row used by the previous event export.
        df = df[df['Team'] != '-']

        # Point columns are optional for tile-race events. Preserve the previous
        # event's awarded-points behavior when either legacy column is present.
        has_points = 'Awarded Points' in df.columns or 'Points' in df.columns
        if 'Awarded Points' in df.columns and 'Points' in df.columns:
            df['Final_Points'] = df['Awarded Points'].fillna(df['Points'])
        elif 'Awarded Points' in df.columns:
            df['Final_Points'] = df['Awarded Points']
        elif 'Points' in df.columns:
            df['Final_Points'] = df['Points']
        else:
            # Keep a neutral internal column so legacy analysis helpers remain
            # structurally compatible until the tile-race rules are finalized.
            df['Final_Points'] = 0

        target_cols = required_cols + ['Final_Points']

        df = df[target_cols]
        
        # 3. RENAME: Standardize
        df.columns = ['Date', 'Player', 'Team', 'Category', 'Item', 'Points']
        df['Player'] = (
            df['Player']
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r'\s+', '', regex=True)
        )
        
        # 4. FORMAT: Convert types
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
        df['Points'] = pd.to_numeric(df['Points'], errors='coerce').fillna(0)
        df['Quantity'] = 1
        
        return df, has_points
        
    except Exception as e:
        st.error(f"Error processing file: {e}")
        return pd.DataFrame(), False


def _normalize_name(name):
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def _resolve_csv_player_to_wom_key(player_name):
    raw_name = str(player_name).strip()
    alias_target = WOM_PLAYER_ALIASES.get(raw_name)
    if alias_target is None:
        normalized_raw_name = _normalize_name(raw_name)
        for alias_source, alias_value in WOM_PLAYER_ALIASES.items():
            if _normalize_name(alias_source) == normalized_raw_name:
                alias_target = alias_value
                break
    if alias_target:
        return _normalize_name(alias_target)
    return _normalize_name(raw_name)


def _wom_retry_delay_seconds(response, attempt):
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), WOM_BASE_BACKOFF_SECONDS)
        except ValueError:
            pass
    return WOM_BASE_BACKOFF_SECONDS * attempt


def _extract_player_name_from_row(row):
    if not isinstance(row, dict):
        return None

    direct = row.get("username") or row.get("displayName") or row.get("name")
    if direct:
        return str(direct)

    player_obj = row.get("player") or row.get("member")
    if isinstance(player_obj, dict):
        nested = (
            player_obj.get("username")
            or player_obj.get("displayName")
            or player_obj.get("name")
        )
        if nested:
            return str(nested)
    return None


def _extract_rows_from_group_response(response_json):
    if isinstance(response_json, list):
        return response_json
    if not isinstance(response_json, dict):
        return []

    data = response_json.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("records", "entries", "results", "members", "leaderboard"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                return candidate
    for key in ("records", "entries", "results", "members", "leaderboard"):
        candidate = response_json.get(key)
        if isinstance(candidate, list):
            return candidate
    return []


@st.cache_data(ttl=21600)
def _fetch_wom_group_metric_success(group_id, metric_name, start_date_str, end_date_str):
    url = f"{WOM_API_BASE_URL}/groups/{group_id}/gained"
    params = {"metric": metric_name, "startDate": start_date_str, "endDate": end_date_str}

    response = requests.get(url, params=params, timeout=20)
    if response.status_code == 404:
        return {}, f"Group {group_id} not found on Wise Old Man"
    response.raise_for_status()

    rows = _extract_rows_from_group_response(response.json())
    gains_by_player = {}
    for row in rows:
        player_name = _extract_player_name_from_row(row)
        if not player_name:
            continue

        gained_value = row.get("gained")
        if gained_value is None and isinstance(row.get("data"), dict):
            gained_value = row["data"].get("gained")
        if gained_value is None and isinstance(row.get("metric"), dict):
            gained_value = row["metric"].get("gained")

        gains_by_player[_normalize_name(player_name)] = float(gained_value or 0)

    return gains_by_player, None


def fetch_wom_group_metric(group_id, metric_name, start_date_str, end_date_str):
    url = f"{WOM_API_BASE_URL}/groups/{group_id}/gained"
    params = {"metric": metric_name, "startDate": start_date_str, "endDate": end_date_str}

    for attempt in range(1, WOM_MAX_RETRIES + 1):
        try:
            return _fetch_wom_group_metric_success(group_id, metric_name, start_date_str, end_date_str)
        except requests.HTTPError as exc:
            response = exc.response
            if response is not None and response.status_code == 429 and attempt < WOM_MAX_RETRIES:
                time.sleep(_wom_retry_delay_seconds(response, attempt))
                continue
            if response is not None and response.status_code == 429:
                return {}, (
                    f"Rate limited by Wise Old Man after {WOM_MAX_RETRIES} retries "
                    f"for {url}?metric={params['metric']}&startDate={params['startDate']}&endDate={params['endDate']}"
                )
            return {}, f"Wise Old Man request failed: {exc}"
        except requests.RequestException as exc:
            if attempt < WOM_MAX_RETRIES:
                time.sleep(WOM_BASE_BACKOFF_SECONDS * attempt)
                continue
            return {}, f"Wise Old Man request failed: {exc}"

    return {}, "Wise Old Man request failed after retries"


@st.cache_data(ttl=21600)
def prefetch_wom_group_metrics_bundle(group_id, metrics, start_date_str, end_date_str):
    kc_by_metric = {}
    errors = []
    for metric_name in sorted(set(metrics)):
        metric_gains, error_msg = fetch_wom_group_metric(
            group_id,
            metric_name,
            start_date_str,
            end_date_str
        )
        if error_msg:
            errors.append(f"{metric_name}: {error_msg}")
            continue
        kc_by_metric[metric_name] = metric_gains
    return kc_by_metric, errors


@st.cache_data(ttl=300)
def load_wom_group_metrics_from_file(cache_path, group_id, start_date_str, end_date_str, metrics):
    file_path = Path(cache_path)
    if not file_path.exists():
        return {}, [f"WOM cache file not found: {file_path.name}"]

    try:
        payload = json.loads(file_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {}, [f"Failed to read WOM cache file: {exc}"]

    notes = []
    file_group_id = payload.get("group_id")
    file_start = payload.get("start_date")
    file_end = payload.get("end_date")
    file_metrics = payload.get("metrics", {})

    if file_group_id != group_id:
        notes.append(f"WOM cache group_id mismatch (file={file_group_id}, app={group_id})")
    try:
        app_start = pd.to_datetime(start_date_str).date()
        app_end = pd.to_datetime(end_date_str).date()
        cache_start = pd.to_datetime(file_start).date() if file_start else None
        cache_end = pd.to_datetime(file_end).date() if file_end else None
        if cache_start is None or cache_end is None or not (cache_start <= app_start and cache_end >= app_end):
            notes.append(
                f"WOM cache date range mismatch (file={file_start}..{file_end}, app={start_date_str}..{end_date_str})"
            )
    except Exception:
        notes.append(
            f"WOM cache date range mismatch (file={file_start}..{file_end}, app={start_date_str}..{end_date_str})"
        )
    if not isinstance(file_metrics, dict):
        return {}, notes + ["WOM cache format invalid: metrics should be an object"]

    kc_by_metric = {}
    for metric_name in metrics:
        metric_map = file_metrics.get(metric_name)
        if isinstance(metric_map, dict):
            normalized_metric_map = {}
            for player_key, gained_value in metric_map.items():
                try:
                    normalized_metric_map[str(player_key)] = float(gained_value or 0)
                except (TypeError, ValueError):
                    normalized_metric_map[str(player_key)] = 0.0
            kc_by_metric[metric_name] = normalized_metric_map

    missing_metrics = sorted(set(metrics) - set(kc_by_metric.keys()))
    if missing_metrics:
        notes.append("Missing metrics in WOM cache: " + ", ".join(missing_metrics[:12]))

    return kc_by_metric, notes


# --- 2. App Interface ---
def main():
    st.markdown("### Summer Bingo 2026 Dashboard")
    
    # Sidebar
    with st.sidebar:
        st.header("Data Source")
        uploaded_file = st.file_uploader("Optional: Upload a replacement CSV", type=['csv'])

    data_source = uploaded_file if uploaded_file is not None else (DEFAULT_CSV_PATH if DEFAULT_CSV_PATH.exists() else None)

    if data_source is not None:
        df, has_points = load_and_clean_data(data_source)
        
        if not df.empty:
            activity_col = 'Points' if has_points else 'Quantity'
            activity_label = 'Points' if has_points else 'Submissions'

            # --- KPI ROW ---
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Drops", len(df))
            if has_points:
                col2.metric("Total Points", f"{int(df['Points'].sum()):,}")
            else:
                col2.metric("Unique Tiles", f"{df['Category'].nunique():,}")

            player_activity = df.groupby('Player')[activity_col].sum()
            top_player = player_activity.idxmax()
            top_player_activity = player_activity.max()
            col3.metric(
                "MVP Player" if has_points else "Most Active Player",
                top_player,
                f"{int(top_player_activity):,} {activity_label.lower()}"
            )

            team_activity = df.groupby('Team')[activity_col].sum()
            top_team = team_activity.idxmax()
            col4.metric(
                "Leading Team" if has_points else "Most Active Team",
                top_team.split('-')[0],
                None if has_points else f"{int(team_activity.max()):,} submissions"
            )

            st.divider()
            event_start_date = df["Date"].min()
            event_end_date = df["Date"].max()
            event_start_date_str = event_start_date.strftime("%Y-%m-%d")
            event_end_date_str = event_end_date.strftime("%Y-%m-%d")
            prefetch_metrics = sorted(
                {
                    metric
                    for category_metrics in CATEGORY_TO_WOM_BOSSES.values()
                    for metric in category_metrics
                    if metric in SUPPORTED_WOM_BOSS_METRICS
                }
            )
            prefetched_kc_by_metric, _ = load_wom_group_metrics_from_file(
                str(WOM_CACHE_FILE),
                WOM_GROUP_ID,
                event_start_date_str,
                event_end_date_str,
                tuple(prefetch_metrics)
            )

            # --- TABS ---
            tab_leader, tab_player_leaderboard, tab_items, tab_player, tab_rankings, tab_team_rankings, tab_highest_kc, tab_raw = st.tabs([
                "🏆 Leaderboards",
                "📋 Player Leaderboard",
                "📦 Item Stats",
                "🔍 Individual Search",
                "📊 Player Rankings",
                "👥 Team Rankings",
                "⚔️ Highest KC",
                "💾 Cleaned Data"
            ])

            # TAB 1: LEADERBOARDS
            with tab_leader:
                c1, c2 = st.columns(2)
                
                with c1:
                    st.subheader("Team Standings (Official)" if has_points else "Team Activity (Temporary)")
                    if not has_points:
                        st.caption("Submission counts are shown until the tile-race scoring rules are added.")
                    team_df = (
                        df.groupby('Team')[activity_col]
                        .sum()
                        .reset_index()
                        .rename(columns={activity_col: activity_label})
                        .sort_values(activity_label, ascending=False)
                    )
                    team_df.index = range(1, len(team_df) + 1)
                    team_df[activity_label] = team_df[activity_label].apply(
                        lambda x: int(x) if float(x).is_integer() else x
                    )
                    
                    st.dataframe(team_df, use_container_width=True)

                with c2:
                    st.subheader("Top 10 Players")
                    player_df = (
                        df.groupby('Player')[activity_col]
                        .sum()
                        .reset_index()
                        .rename(columns={activity_col: activity_label})
                        .sort_values(activity_label, ascending=False)
                        .head(10)
                    )
                    fig_player = px.bar(
                        player_df,
                        x=activity_label,
                        y='Player',
                        orientation='h',
                        text=activity_label,
                        color=activity_label
                    )
                    fig_player.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig_player, use_container_width=True)

            # TAB 2: PLAYER LEADERBOARD
            with tab_player_leaderboard:
                st.subheader("Player Leaderboard")
                st.caption("All players ranked by submissions; ties are ordered alphabetically.")
                player_leaderboard_df = (
                    df.groupby(['Player', 'Team'], as_index=False)['Quantity']
                    .sum()
                    .rename(columns={'Quantity': 'Submissions'})
                    .sort_values(['Submissions', 'Player'], ascending=[False, True])
                    .reset_index(drop=True)
                )
                player_leaderboard_df.insert(
                    0,
                    "Rank",
                    range(1, len(player_leaderboard_df) + 1)
                )
                st.dataframe(
                    player_leaderboard_df[['Rank', 'Player', 'Team', 'Submissions']],
                    hide_index=True,
                    use_container_width=True
                )

            # TAB 3: ITEM STATS
            with tab_items:
                col_filter, col_chart = st.columns([1, 3])
                
                with col_filter:
                    st.write("### Filters")
                    selected_category = st.selectbox("Filter by Tile/Category", ["All"] + sorted(df['Category'].dropna().unique()))
                
                with col_chart:
                    viz_df = df if selected_category == "All" else df[df['Category'] == selected_category]
                    
                    st.subheader(f"Most Acquired Items ({selected_category})")
                    item_counts = viz_df['Item'].value_counts().reset_index().head(15)
                    item_counts.columns = ['Item', 'Count']
                    
                    fig_items = px.bar(item_counts, x='Count', y='Item', orientation='h', title="Top Drops by Quantity")
                    fig_items.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig_items, use_container_width=True)
                    
                    if has_points:
                        st.write("### High Value Drops")
                        high_value = viz_df[viz_df['Points'] >= 5].sort_values('Date', ascending=False).head(10)
                        st.dataframe(
                            high_value[['Date', 'Player', 'Item', 'Points']],
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.write("### Recent Submissions")
                        recent_submissions = viz_df.sort_values('Date', ascending=False).head(10)
                        st.dataframe(
                            recent_submissions[['Date', 'Player', 'Category', 'Item']],
                            hide_index=True,
                            use_container_width=True
                        )

            # TAB 4: INDIVIDUAL PLAYER
            with tab_player:
                players_list = sorted(df['Player'].unique())
                selected_player = st.selectbox("Select a Player", players_list)
                
                if selected_player:
                    p_data = df[df['Player'] == selected_player]
                    wom_lookup_key = _resolve_csv_player_to_wom_key(selected_player)
                    player_total_kc_gain = sum(
                        prefetched_kc_by_metric.get(metric_name, {}).get(wom_lookup_key, 0.0)
                        for metric_name in prefetch_metrics
                    )
                    player_total_kc_display = (
                        f"{int(player_total_kc_gain):,}"
                        if player_total_kc_gain > 0
                        else "No WoM Data"
                    )
                    
                    pk1, pk2, pk3, pk4 = st.columns(4)
                    pk1.metric("Submissions", len(p_data))
                    if has_points:
                        pk2.metric("Total Points", int(p_data['Points'].sum()))
                    else:
                        pk2.metric("Unique Tiles", p_data['Category'].nunique())
                    pk3.metric("Favorite Tile", p_data['Category'].mode()[0] if not p_data.empty else "N/A")
                    pk4.metric("WoM KC (Event)", player_total_kc_display)
                    
                    st.write(f"### Submission History for {selected_player}")
                    history_cols = ['Date', 'Category', 'Item']
                    if has_points:
                        history_cols.append('Points')
                    st.dataframe(
                        p_data[history_cols].sort_values('Date', ascending=False),
                        use_container_width=True
                    )

            # TAB 5: PLAYER RANKINGS
            with tab_rankings:
                st.subheader("Top Players by Category")
                categories = sorted(df['Category'].dropna().unique())
                if categories:
                    selected_rank_category = st.selectbox(
                        "Choose a Category",
                        categories,
                        key="rank_category"
                    )

                    cat_rank_df = (
                        df[df['Category'] == selected_rank_category]
                        .groupby('Player', as_index=False)[activity_col]
                        .sum()
                        .rename(columns={activity_col: activity_label})
                        .sort_values(activity_label, ascending=False)
                    )
                    cat_rank_df.insert(0, "Rank", range(1, len(cat_rank_df) + 1))
                    st.dataframe(
                        cat_rank_df[['Rank', 'Player', activity_label]],
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.info("No categories found in the uploaded data.")

                st.divider()

                st.subheader("Top Players by Item")
                items = sorted(df['Item'].dropna().unique())
                if items:
                    selected_rank_item = st.selectbox(
                        "Choose an Item",
                        items,
                        key="rank_item"
                    )

                    item_rank_df = (
                        df[df['Item'] == selected_rank_item]
                        .groupby('Player', as_index=False)[activity_col]
                        .sum()
                        .rename(columns={activity_col: activity_label})
                        .sort_values(activity_label, ascending=False)
                    )
                    item_rank_df.insert(0, "Rank", range(1, len(item_rank_df) + 1))
                    st.dataframe(
                        item_rank_df[['Rank', 'Player', activity_label]],
                        hide_index=True,
                        use_container_width=True
                    )
                else:
                    st.info("No items found in the uploaded data.")

            # TAB 6: TEAM RANKINGS
            with tab_team_rankings:
                st.subheader("Top Players by Team")
                teams = sorted(df['Team'].dropna().unique())
                if teams:
                    selected_team = st.selectbox("Choose a Team", teams, key="rank_team")

                    team_player_rank_df = (
                        df[df['Team'] == selected_team]
                        .groupby('Player', as_index=False)[activity_col]
                        .sum()
                        .rename(columns={activity_col: activity_label})
                        .sort_values(activity_label, ascending=False)
                    )
                    team_player_rank_df.insert(0, "Rank", range(1, len(team_player_rank_df) + 1))
                    st.dataframe(
                        team_player_rank_df[['Rank', 'Player', activity_label]],
                        hide_index=True,
                        use_container_width=True
                    )

                    st.divider()
                    st.subheader(f"{selected_team} Item {activity_label} by Category")
                    team_df = df[df['Team'] == selected_team]
                    team_categories = sorted(team_df['Category'].dropna().unique())

                    if team_categories:
                        selected_team_category = st.selectbox(
                            "Choose a Category",
                            team_categories,
                            key="rank_team_category"
                        )

                        team_item_activity_df = (
                            team_df[team_df['Category'] == selected_team_category]
                            .groupby('Item', as_index=False)[activity_col]
                            .sum()
                            .rename(columns={activity_col: activity_label})
                            .sort_values(activity_label, ascending=False)
                        )
                        team_item_activity_df.insert(0, "Rank", range(1, len(team_item_activity_df) + 1))

                        fig_team_items = px.bar(
                            team_item_activity_df.head(20),
                            x=activity_label,
                            y='Item',
                            orientation='h',
                            text=activity_label,
                            color=activity_label,
                            title=f"{selected_team} - {selected_team_category}: {activity_label} by Item"
                        )
                        fig_team_items.update_layout(yaxis={'categoryorder': 'total ascending'})
                        st.plotly_chart(fig_team_items, use_container_width=True)
                        st.dataframe(
                            team_item_activity_df[['Rank', 'Item', activity_label]],
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.info("No categories found for this team.")
                else:
                    st.info("No teams found in the uploaded data.")

            # TAB 7: HIGHEST KC
            with tab_highest_kc:
                st.subheader("Highest KC by Category")
                st.caption(
                    f"Using cached WOM data from {WOM_CACHE_FILE.name} for range "
                    f"{event_start_date_str} to {event_end_date_str}."
                )

                available_kc_categories = sorted(
                    [
                        cat for cat in df["Category"].dropna().unique()
                        if cat in CATEGORY_TO_WOM_BOSSES
                    ]
                )

                if available_kc_categories:
                    selected_kc_category = st.selectbox(
                        "Choose a Category",
                        available_kc_categories,
                        key="highest_kc_category"
                    )
                    selected_kc_metrics = [
                        metric for metric in CATEGORY_TO_WOM_BOSSES[selected_kc_category]
                        if metric in SUPPORTED_WOM_BOSS_METRICS
                    ]

                    if selected_kc_metrics:
                        category_activity_by_player = (
                            df[df["Category"] == selected_kc_category]
                            .groupby("Player", as_index=False)[activity_col]
                            .sum()
                        )

                        kc_rows = []
                        for player in sorted(df["Player"].dropna().unique()):
                            wom_lookup_key = _resolve_csv_player_to_wom_key(player)
                            player_kc_gain = sum(
                                prefetched_kc_by_metric.get(metric_name, {}).get(wom_lookup_key, 0.0)
                                for metric_name in selected_kc_metrics
                            )
                            player_activity_value = float(
                                category_activity_by_player.loc[
                                    category_activity_by_player["Player"] == player,
                                    activity_col
                                ].sum()
                            )
                            kc_rows.append(
                                {
                                    "Player": player,
                                    "KC Gain": round(player_kc_gain, 2),
                                    activity_label: round(player_activity_value, 2),
                                }
                            )

                        kc_df = pd.DataFrame(kc_rows).sort_values(
                            by=["KC Gain", activity_label],
                            ascending=[False, False]
                        ).reset_index(drop=True)
                        kc_df.insert(0, "Rank", range(1, len(kc_df) + 1))

                        fig_kc = px.bar(
                            kc_df.head(20),
                            x="KC Gain",
                            y="Player",
                            orientation="h",
                            text="KC Gain",
                            color="KC Gain",
                            title=f"Top KC Gains - {selected_kc_category}"
                        )
                        fig_kc.update_layout(yaxis={"categoryorder": "total ascending"})
                        st.plotly_chart(fig_kc, use_container_width=True)
                        st.dataframe(kc_df, hide_index=True, use_container_width=True)
                    else:
                        st.info("No supported WOM boss metrics are mapped for this category.")
                else:
                    st.info("No categories available for Highest KC view.")

            # TAB 8: RAW DATA
            with tab_raw:
                if has_points:
                    st.write("Cleaned Data (Using legacy point scoring):")
                    display_cols = ['Date', 'Player', 'Team', 'Category', 'Item', 'Points']
                else:
                    st.write("Cleaned Data (Tile-race preparation; no point columns required):")
                    display_cols = ['Date', 'Player', 'Team', 'Category', 'Item']
                st.dataframe(df[display_cols], use_container_width=True)

    else:
        st.info(f"👋 No CSV available. Add {DEFAULT_CSV_PATH.name} to the app folder or upload a CSV.")

if __name__ == "__main__":
    main()
