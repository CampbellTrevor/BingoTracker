import streamlit as st
import pandas as pd
import plotly.express as px

# --- Page Configuration ---
st.set_page_config(page_title="OSRS Bingo Tracker", layout="wide", page_icon="⚔️")

# --- 1. Data Cleaning Engine ---
@st.cache_data
def load_and_clean_data(file):
    try:
        # Load the CSV
        df = pd.read_csv(file)
        
        # 1. FILTER: Remove the "malformed" test row (Entry #1899)
        df = df[df['Team'] != '-']
        
        # 2. SELECT: We now grab 'Awarded Points' as our primary source
        # We rename 'Awarded Points' to 'Points' for the app to use
        # We keep 'Points' as 'Base_Points' just in case we want to compare later
        
        # Check if 'Awarded Points' exists, otherwise default to 'Points'
        if 'Awarded Points' in df.columns:
            df['Final_Points'] = df['Awarded Points'].fillna(df['Points'])
        else:
            df['Final_Points'] = df['Points']

        target_cols = ['Date', 'Player Name', 'Team', 'Tile', 'Item Received', 'Final_Points']
        
        # Check for missing columns
        if not all(col in df.columns for col in target_cols):
             # Fallback for older CSV versions if names differ
            st.error(f"Missing columns. Found: {df.columns.tolist()}")
            return pd.DataFrame()

        df = df[target_cols]
        
        # 3. RENAME: Standardize
        df.columns = ['Date', 'Player', 'Team', 'Category', 'Item', 'Points']
        
        # 4. FORMAT: Convert types
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
        df['Points'] = pd.to_numeric(df['Points'], errors='coerce').fillna(0)
        df['Quantity'] = 1
        
        return df
        
    except Exception as e:
        st.error(f"Error processing file: {e}")
        return pd.DataFrame()

# --- 2. App Interface ---
def main():
    st.title("⚔️ OSRS Bingo Event Tracker")
    st.markdown("### Winter Bingo 2026 Dashboard")
    
    # Sidebar
    with st.sidebar:
        st.header("Upload Log")
        uploaded_file = st.file_uploader("Upload your 'Event Log' CSV", type=['csv'])
        st.caption("Now using 'Awarded Points' for accurate scoring.")

    if uploaded_file is not None:
        df = load_and_clean_data(uploaded_file)
        
        if not df.empty:
            # --- KPI ROW ---
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Drops", len(df))
            col2.metric("Total Points", f"{int(df['Points'].sum()):,}")
            
            top_player = df.groupby('Player')['Points'].sum().idxmax()
            top_player_score = df.groupby('Player')['Points'].sum().max()
            col3.metric("MVP Player", top_player, f"{int(top_player_score)} pts")
            
            top_team = df.groupby('Team')['Points'].sum().idxmax()
            col4.metric("Leading Team", top_team.split('-')[0]) 

            st.divider()

            # --- TABS ---
            tab_leader, tab_items, tab_player, tab_raw = st.tabs(["🏆 Leaderboards", "📦 Item Stats", "🔍 Individual Search", "💾 Cleaned Data"])

            # TAB 1: LEADERBOARDS
            with tab_leader:
                c1, c2 = st.columns(2)
                
                with c1:
                    st.subheader("Team Standings (Official)")
                    # Group by Team and Sum the CORRECTED points
                    team_df = df.groupby('Team')['Points'].sum().reset_index().sort_values('Points', ascending=False)
                    team_df.index = range(1, len(team_df) + 1)
                    
                    # Format points to be integers if they are whole numbers
                    team_df['Points'] = team_df['Points'].apply(lambda x: int(x) if x.is_integer() else x)
                    
                    st.dataframe(team_df, use_container_width=True)

                with c2:
                    st.subheader("Top 10 Players")
                    player_df = df.groupby('Player')['Points'].sum().reset_index().sort_values('Points', ascending=False).head(10)
                    fig_player = px.bar(player_df, x='Points', y='Player', orientation='h', text='Points', color='Points')
                    fig_player.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig_player, use_container_width=True)

            # TAB 2: ITEM STATS
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
                    
                    st.write("### High Value Drops")
                    high_value = viz_df[viz_df['Points'] >= 5].sort_values('Date', ascending=False).head(10)
                    st.dataframe(high_value[['Date', 'Player', 'Item', 'Points']], hide_index=True, use_container_width=True)

            # TAB 3: INDIVIDUAL PLAYER
            with tab_player:
                players_list = sorted(df['Player'].unique())
                selected_player = st.selectbox("Select a Player", players_list)
                
                if selected_player:
                    p_data = df[df['Player'] == selected_player]
                    
                    pk1, pk2, pk3 = st.columns(3)
                    pk1.metric("Submissions", len(p_data))
                    pk2.metric("Total Points", int(p_data['Points'].sum()))
                    pk3.metric("Favorite Tile", p_data['Category'].mode()[0] if not p_data.empty else "N/A")
                    
                    st.write(f"### Submission History for {selected_player}")
                    st.dataframe(
                        p_data[['Date', 'Category', 'Item', 'Points']].sort_values('Date', ascending=False),
                        use_container_width=True
                    )

            # TAB 4: RAW DATA
            with tab_raw:
                st.write("Cleaned Data (Using 'Awarded Points'):")
                st.dataframe(df, use_container_width=True)

    else:
        st.info("👋 Upload your CSV file to see the corrected scores.")

if __name__ == "__main__":
    main()