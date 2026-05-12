"""
dashboard.py — Real-Time Recommendation System Dashboard
Run: streamlit run /app/dashboard.py --server.port 8501 --server.address 0.0.0.0
"""

import streamlit as st
import pandas as pd
import os
import time

# ── Config ───────────────────────────────────────────────────────────────────
STREAM_PATH = '/data/streaming_output/'
RECS_PATH   = '/data/integrated_recs.parquet'
REFRESH_SEC = 10

st.set_page_config(
    page_title='Recommendation Dashboard',
    page_icon='🛒',
    layout='wide'
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .dash-title {
        font-size: 2.2rem; font-weight: 700;
        text-align: center; padding: 16px 0 4px;
        background: linear-gradient(90deg, #6C63FF, #48CAE4);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .dash-sub { text-align: center; color: #888; font-size: 0.85rem; margin-bottom: 24px; }
    .kpi-card {
        background: #1e2130; border-radius: 12px;
        padding: 18px 20px; margin: 4px; border-left: 4px solid;
    }
    .kpi-card.blue   { border-color: #48CAE4; }
    .kpi-card.purple { border-color: #6C63FF; }
    .kpi-card.green  { border-color: #43D17A; }
    .kpi-card.amber  { border-color: #FFB547; }
    .kpi-label { font-size: 0.78rem; color: #aaa; margin-bottom: 4px; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; color: #fff; }
    .section-header {
        font-size: 1.1rem; font-weight: 600;
        padding: 8px 0 4px; border-bottom: 2px solid; margin-bottom: 12px;
    }
    .section-header.teal   { border-color: #48CAE4; color: #48CAE4; }
    .section-header.purple { border-color: #6C63FF; color: #6C63FF; }
    .section-header.green  { border-color: #43D17A; color: #43D17A; }
    .section-header.amber  { border-color: #FFB547; color: #FFB547; }
    .rec-card {
        background: #1e2130; border-radius: 10px;
        padding: 12px 16px; margin-bottom: 8px; border-left: 4px solid #6C63FF;
        display: flex; justify-content: space-between; align-items: center;
    }
    .rec-rank  { font-size: 1.4rem; font-weight: 700; color: #6C63FF; min-width: 36px; }
    .rec-item  { font-size: 0.95rem; color: #ddd; flex: 1; }
    .rec-score { font-size: 0.85rem; color: #aaa; text-align: right; }
    .rec-badge {
        background: #FFB547; color: #000; border-radius: 20px;
        font-size: 0.7rem; font-weight: 700; padding: 2px 8px; margin-left: 8px;
    }
    .alert-card {
        background: #2a1a1a; border: 1px solid #e53e3e;
        border-radius: 10px; padding: 12px 16px; margin-bottom: 8px;
    }
    .alert-title  { color: #fc8181; font-weight: 700; font-size: 0.95rem; }
    .alert-detail { color: #aaa; font-size: 0.82rem; margin-top: 2px; }
    .trend-row {
        background: #1e2130; border-radius: 8px;
        padding: 10px 14px; margin-bottom: 6px;
        display: flex; align-items: center; gap: 12px;
    }
    .trend-rank  { font-size: 1.1rem; font-weight: 700; min-width: 28px; }
    .trend-bar-bg { flex: 1; background: #2d3147; border-radius: 4px; height: 8px; }
    .trend-bar    { height: 8px; border-radius: 4px; background: linear-gradient(90deg,#48CAE4,#6C63FF); }
    .trend-score  { font-size: 0.85rem; color: #ddd; min-width: 60px; text-align: right; }
    .activity-stat {
        background: #1e2130; border-radius: 10px;
        padding: 14px 18px; margin-bottom: 8px; border-left: 4px solid #43D17A;
    }
    .activity-label { font-size: 0.8rem; color: #aaa; }
    .activity-value { font-size: 1.3rem; font-weight: 700; color: #43D17A; }
    .item-row {
        display:flex; justify-content:space-between;
        background:#1e2130; border-radius:8px;
        padding:8px 14px; margin-bottom:5px;
    }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def load_streaming():
    if not os.path.exists(STREAM_PATH):
        return pd.DataFrame()
    files = [f for f in os.listdir(STREAM_PATH) if f.endswith('.parquet')]
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(os.path.join(STREAM_PATH, f)))
        except Exception:
            continue
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def load_recs():
    if not os.path.exists(RECS_PATH):
        return pd.DataFrame()
    try:
        return pd.read_parquet(RECS_PATH)
    except Exception:
        return pd.DataFrame()

def latest_per_item(df):
    if df.empty or 'window_end' not in df.columns:
        return df
    return (
        df.sort_values('window_end', ascending=False)
          .drop_duplicates(subset='item_id')
          .reset_index(drop=True)
    )

def rating_stars(r):
    return '★' * int(r) + '☆' * (5 - int(r))

def rank_color(rank):
    return ['#FFD700','#C0C0C0','#CD7F32','#6C63FF','#48CAE4'][min(rank-1, 4)]

# ── Load ──────────────────────────────────────────────────────────────────────
streaming_raw = load_streaming()
streaming     = latest_per_item(streaming_raw)
recs          = load_recs()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="dash-title">🛒 Real-Time Recommendation System</div>', unsafe_allow_html=True)
st.markdown(
    f'<div class="dash-sub">E-Commerce · Electronics · '
    f'Auto-refreshes every {REFRESH_SEC}s · {time.strftime("%H:%M:%S")}</div>',
    unsafe_allow_html=True
)

# ── KPI Row ───────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
def kpi(col, color, label, value):
    col.markdown(
        f'<div class="kpi-card {color}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div></div>',
        unsafe_allow_html=True
    )

if not streaming.empty:
    kpi(k1, 'blue',   '📦 Items Tracked',     f"{streaming['item_id'].nunique():,}")
    kpi(k2, 'purple', '⭐ Avg Live Rating',    f"{streaming['avg_rating'].mean():.2f}")
    kpi(k3, 'green',  '🔥 Total Interactions', f"{streaming['interaction_count'].sum():,.0f}")
    kpi(k4, 'amber',  '📈 Top Trending Score', f"{streaming['trending_score'].max():.1f}")
else:
    for col, color, label in [
        (k1,'blue','📦 Items Tracked'),
        (k2,'purple','⭐ Avg Live Rating'),
        (k3,'green','🔥 Total Interactions'),
        (k4,'amber','📈 Top Trending Score')
    ]:
        kpi(col, color, label, '—')
    st.warning('⚠️ No streaming data. Start 02_streaming.ipynb and kafka_producer.py first.')

st.markdown('<br>', unsafe_allow_html=True)

# ── Trending + Alerts ─────────────────────────────────────────────────────────
col_trend, col_alert = st.columns(2)

with col_trend:
    st.markdown('<div class="section-header teal">📈 Trending Items</div>', unsafe_allow_html=True)
    if not streaming.empty:
        top10    = streaming.sort_values('trending_score', ascending=False).head(10).reset_index(drop=True)
        max_score = top10['trending_score'].max() or 1
        rank_colors = ['#FFD700','#C0C0C0','#CD7F32'] + ['#6C63FF'] * 7
        for i, row in top10.iterrows():
            pct = int((row['trending_score'] / max_score) * 100)
            st.markdown(f"""
            <div class="trend-row">
                <span class="trend-rank" style="color:{rank_colors[i]}">#{i+1}</span>
                <span style="color:#ddd;min-width:80px;font-size:0.9rem">ID {int(row['item_id'])}</span>
                <span style="color:#aaa;font-size:0.8rem">{rating_stars(row['avg_rating'])}</span>
                <div class="trend-bar-bg"><div class="trend-bar" style="width:{pct}%"></div></div>
                <span class="trend-score">{row['trending_score']:.1f}</span>
            </div>""", unsafe_allow_html=True)
    else:
        st.info('Waiting for streaming data...')

with col_alert:
    st.markdown('<div class="section-header amber">🚨 Alerts</div>', unsafe_allow_html=True)
    if not streaming.empty:
        alerts = streaming[
            (streaming['avg_rating'] > 4.5) | (streaming['interaction_count'] > 50)
        ].sort_values('trending_score', ascending=False)
        if not alerts.empty:
            for _, row in alerts.head(6).iterrows():
                reasons = []
                if row['avg_rating'] > 4.5:
                    reasons.append(f"⭐ Rating {row['avg_rating']:.2f}")
                if row['interaction_count'] > 50:
                    reasons.append(f"⚡ {int(row['interaction_count'])} interactions")
                st.markdown(f"""
                <div class="alert-card">
                    <div class="alert-title">🔔 Item {int(row['item_id'])} is trending</div>
                    <div class="alert-detail">{' · '.join(reasons)} · Score: {row['trending_score']:.1f}</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.success('✅ No alerts at this time.')
    else:
        st.info('Waiting for streaming data...')

st.markdown('<br>', unsafe_allow_html=True)

# ── Recommendations + User Activity ──────────────────────────────────────────
col_rec, col_user = st.columns(2)

with col_rec:
    st.markdown('<div class="section-header purple">🎯 Recommendations</div>', unsafe_allow_html=True)
    if not recs.empty:
        uid = st.selectbox(
            'Select user ID',
            options=sorted(recs['user_id'].unique())[:200],
            key='uid'
        )
        user_recs = recs[recs['user_id'] == uid].sort_values('final_rank').reset_index(drop=True)
        for _, row in user_recs.iterrows():
            rank    = int(row['final_rank'])
            color   = rank_color(rank)
            als     = row.get('predicted_rating', 0)
            blend   = row.get('blended_score', 0)
            trending = row.get('trending_norm', 0)
            badge   = '<span class="rec-badge">🔥 TRENDING</span>' if trending > 0 else ''
            st.markdown(f"""
            <div class="rec-card" style="border-color:{color}">
                <span class="rec-rank" style="color:{color}">#{rank}</span>
                <div class="rec-item">Item <b>{int(row['item_id'])}</b>{badge}</div>
                <div class="rec-score">ALS {als:.2f} · Blend {blend:.2f}</div>
            </div>""", unsafe_allow_html=True)
    else:
        st.info('Run 03_integration.ipynb first to generate recommendations.')

with col_user:
    st.markdown('<div class="section-header green">👤 User Activity</div>', unsafe_allow_html=True)
    if not streaming_raw.empty and 'interaction_count' in streaming_raw.columns:
        total   = streaming_raw['interaction_count'].sum()
        n_win   = streaming_raw['window_end'].nunique() if 'window_end' in streaming_raw.columns else 1
        avg_win = total / n_win if n_win > 0 else 0

        a1, a2 = st.columns(2)
        a1.markdown(
            f'<div class="activity-stat">'
            f'<div class="activity-label">Total events</div>'
            f'<div class="activity-value">{total:,.0f}</div></div>',
            unsafe_allow_html=True
        )
        a2.markdown(
            f'<div class="activity-stat">'
            f'<div class="activity-label">Avg per window</div>'
            f'<div class="activity-value">{avg_win:.0f}</div></div>',
            unsafe_allow_html=True
        )

        if 'window_end' in streaming_raw.columns:
            activity = (
                streaming_raw.groupby('window_end')['interaction_count']
                .sum().reset_index().sort_values('window_end').tail(20)
            )
            activity.columns = ['Time', 'Interactions']
            activity['Time'] = activity['Time'].astype(str).str[11:19]
            st.markdown(
                '<div style="color:#aaa;font-size:0.8rem;margin:8px 0 4px">'
                'Interactions over time (last 20 windows)</div>',
                unsafe_allow_html=True
            )
            st.line_chart(activity.set_index('Time'), color='#43D17A')

        st.markdown(
            '<div style="color:#aaa;font-size:0.8rem;margin:12px 0 4px">'
            'Most active items</div>',
            unsafe_allow_html=True
        )
        if not streaming.empty:
            top_active = (
                streaming[['item_id','interaction_count']]
                .sort_values('interaction_count', ascending=False).head(5)
            )
            for _, row in top_active.iterrows():
                st.markdown(
                    f'<div class="item-row">'
                    f'<span style="color:#ddd">Item {int(row["item_id"])}</span>'
                    f'<span style="color:#43D17A;font-weight:700">{int(row["interaction_count"])} events</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
    else:
        st.info('Waiting for streaming data...')

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown('<br>', unsafe_allow_html=True)
st.divider()
fc, fb = st.columns([4, 1])
fc.caption(f'Big Data Analytics · Mini Project 3 · Last updated: {time.strftime("%Y-%m-%d %H:%M:%S")}')
fb.button('🔄 Refresh now', on_click=st.rerun, use_container_width=True)

time.sleep(REFRESH_SEC)
st.rerun()