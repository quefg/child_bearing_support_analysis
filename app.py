import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# ================= 0. 定义全局展示列 (严格规范展示标准) =================
FULL_SHOW_COLS = [
    'post_id', 'keyword','user_name', 'is_spammer',
    'user_role','clean_ip','publish_time','content',
    'post_repost_count','post_comment_count','post_like_count','total_engagement',
    'content_repeat_times','post_tool',
    'url','crawl_time',
]

# ================= 1. 页面配置 =================
st.set_page_config(
    page_title="微博生育津贴数据交互看板",
    page_icon="📊",
    layout="wide"
)

# ================= 2. 自定义样式 =================
st.markdown("""
<style>
/* 页面整体留白更适合论文展示 */
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    padding-left: 2rem;
    padding-right: 2rem;
}

/* 🎯 让右侧栏 (第二个 column) 变成悬浮固定 (Sticky) */
[data-testid="column"]:nth-of-type(2) {
    position: sticky;
    top: 4rem;
    height: 85vh;
    overflow-y: auto;
    padding-bottom: 1rem;
}

/* 美化右侧栏的滚动条 */
[data-testid="column"]:nth-of-type(2)::-webkit-scrollbar { width: 6px; }
[data-testid="column"]:nth-of-type(2)::-webkit-scrollbar-thumb { background-color: #D1D5DB; border-radius: 4px; }

/* 标题与组件美化 */
.section-title { font-weight: 700; font-size: 1.2rem; margin-bottom: 0.4rem; color: #111827; }
[data-testid="stMetric"] { background: #FAFAFA; border: 1px solid #EAEAEA; padding: 10px 12px; border-radius: 12px; }
[data-testid="stDataFrame"] { border: 1px solid #ECECEC; border-radius: 12px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ================= 3. Session State 初始化 =================
state_keys = ["sample_kw", "sample_geo", "sample_role", "sample_date", "current_id"]
for key in state_keys:
    if key not in st.session_state:
        st.session_state[key] = None


# ================= 5. 数据加载与清洗 =================
@st.cache_data
def load_all_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    p_path = os.path.join(current_dir,"Dashboard_labeled_post.parquet")
    c_path_parquet = os.path.join(current_dir,"Dashboard_Comments.parquet")

    # --- 1. 读取主表 ---
    df_p = pd.read_parquet(p_path)
    df_p["post_id"] = df_p["post_id"].astype(str)

    numeric_cols = ["post_like_count", "post_comment_count", "post_repost_count"]
    for col in numeric_cols:
        if col in df_p.columns:
            df_p[col] = pd.to_numeric(df_p[col], errors="coerce").fillna(0).astype(int)
        else:
            df_p[col] = 0

    df_p["total_engagement"] = (
            df_p["post_like_count"] +
            df_p["post_comment_count"] +
            df_p["post_repost_count"]
    )

    if "total_eng" in df_p.columns:
        df_p = df_p.drop(columns=["total_eng"])

    if "date" in df_p.columns:
        df_p["date"] = pd.to_datetime(df_p["date"], errors="coerce").dt.date
    else:
        df_p["date"] = pd.NaT

    # --- 2. 读取评论表 ---
    if os.path.exists(c_path_parquet):
        df_c = pd.read_parquet(c_path_parquet)
    else:
        df_c = pd.DataFrame()

    if not df_c.empty:
        if "post_id" in df_c.columns:
            df_c["post_id"] = df_c["post_id"].astype(str)
        if "comment_like_count" in df_c.columns:
            df_c["comment_like_count"] = pd.to_numeric(
                df_c["comment_like_count"], errors="coerce"
            ).fillna(0)

    # --- 3. 🎯 核心修复：从评论表反向提取“真实发帖时间” ---
    if not df_c.empty and "post_time" in df_c.columns and "post_date" in df_c.columns:
        time_mapping = df_c[['post_id', 'post_date', 'post_time']].dropna(subset=['post_time']).drop_duplicates(
            subset=['post_id'])

        time_mapping['real_publish_time'] = pd.to_datetime(
            time_mapping['post_date'].astype(str) + ' ' + time_mapping['post_time'].astype(str),
            errors='coerce'
        )

        df_p = df_p.merge(time_mapping[['post_id', 'real_publish_time']], on='post_id', how='left')

        if 'publish_time' in df_p.columns:
            df_p['publish_time'] = df_p['real_publish_time'].fillna(df_p['publish_time'])
        else:
            df_p['publish_time'] = df_p['real_publish_time']

        df_p = df_p.drop(columns=['real_publish_time'])

    return df_p, df_c

try:
    df_raw, df_comm = load_all_data()
except Exception as e:
    st.error(f"数据加载失败: {e}")
    st.stop()

# ================= 6. 顶部区 =================
st.title("📊 微博舆情深度分析看板")

top_c1, top_c2, top_c3 = st.columns([2.4, 1.2, 1.2])

with top_c1:
    if not df_raw["date"].isna().all():
        min_date, max_date = df_raw["date"].min(), df_raw["date"].max()
        selected_dates = st.date_input("🗓️ 选择日期范围", value=(min_date, max_date))
    else:
        selected_dates = None

with top_c2: st.metric("主表帖子数", f"{len(df_raw):,}")
with top_c3: st.metric("评论数", f"{len(df_comm):,}" if not df_comm.empty else "0")

if selected_dates and isinstance(selected_dates, tuple) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
    df = df_raw[(df_raw["date"] >= start_date) & (df_raw["date"] <= end_date)].copy()
else:
    df = df_raw.copy()

# ================= 7. 左右布局 =================
col_left, col_right = st.columns([6, 4], gap="large")

# ================= 左侧：图表与抽样 =================
with col_left:
    st.markdown("## 📈 舆情宏观分析")

    # ---------- 0. 全网声量演化趋势 ----------
    st.markdown("### 0. “生育补贴”相关话题全网发帖声量演化")
    trend_df = df.groupby("date").size().reset_index(name="post_count")
    if not trend_df.empty:
        fig_trend = px.line(trend_df, x="date", y="post_count", markers=True)
        fig_trend.update_traces(line=dict(color="#D94042", width=2), marker=dict(size=6, color="#D94042"))
        fig_trend.update_layout(xaxis_title="发布日期", yaxis_title="发帖数量 (篇)", plot_bgcolor="white",
                                paper_bgcolor="white", margin=dict(l=20, r=20, t=10, b=10))
        fig_trend.update_xaxes(showgrid=True, gridcolor="#E5E7EB")
        fig_trend.update_yaxes(showgrid=True, gridcolor="#E5E7EB")
        st.plotly_chart(fig_trend, use_container_width=True)

    with st.expander("👉 日期样本抽取与锁定", expanded=False):
        date_options = sorted([d for d in df["date"].unique() if pd.notna(d)])
        if date_options:
            c1, c2 = st.columns([2, 3])
            with c1:
                sel_date = st.selectbox("选择具体日期", options=date_options, key="date_select")
            with c2:
                subset_date = df[df["date"] == sel_date]
                max_eng_date = int(subset_date["total_engagement"].max()) if not subset_date.empty and pd.notna(subset_date["total_engagement"].max()) else 0
                slider_max_date = max(1, max_eng_date) if max_eng_date == 0 else max_eng_date

                eng_range_date = st.slider("该日期热度范围", 0, slider_max_date, (0, slider_max_date), key="date_eng_range")

            if st.button("🎲 抽取并锁定该日样本", key="date_sample_btn"):
                subset = subset_date[subset_date["total_engagement"].between(eng_range_date[0], eng_range_date[1])].copy()
                st.session_state.sample_date = subset.sample(n=min(100, len(subset)), random_state=42) if not subset.empty else None

            if st.session_state.sample_date is not None and not st.session_state.sample_date.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_date)} 条数据")
                # 🛡️ 严格应用 FULL_SHOW_COLS
                cols_to_show = [c for c in FULL_SHOW_COLS if c in st.session_state.sample_date.columns]
                st.dataframe(st.session_state.sample_date[cols_to_show], use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 1. 各关键词下辖数据体量与平均互动热度对比 ----------
    st.markdown("### 1. 各关键词下辖数据体量与平均互动热度对比")
    kw_agg = df.groupby("keyword").agg(post_count=("post_id", "count"),
                                       avg_eng=("total_engagement", "mean")).reset_index()
    kw_agg = kw_agg.sort_values("post_count", ascending=False)

    if not kw_agg.empty:
        fig_kw_dual = make_subplots(specs=[[{"secondary_y": True}]])
        fig_kw_dual.add_trace(go.Bar(x=kw_agg["keyword"], y=kw_agg["post_count"], name="发帖总数 (篇)", marker_color="#7DA8D6"), secondary_y=False)
        fig_kw_dual.add_trace(go.Scatter(x=kw_agg["keyword"], y=kw_agg["avg_eng"], name="平均互动热度", mode="lines+markers", line=dict(color="#DE5358", width=2.5), marker=dict(symbol="square", size=6)), secondary_y=True)

        fig_kw_dual.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=20, r=20, t=20, b=20), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        fig_kw_dual.update_xaxes(tickangle=-45)
        fig_kw_dual.update_yaxes(title_text="发帖总数 (篇)", showgrid=False, secondary_y=False, color="#5B7E9F")
        fig_kw_dual.update_yaxes(title_text="平均单帖互动数", showgrid=False, secondary_y=True, color="#DE5358")
        st.plotly_chart(fig_kw_dual, use_container_width=True)

    with st.expander("👉 关键词样本抽取与锁定", expanded=False):
        kw_options = sorted([x for x in df["keyword"].dropna().unique().tolist()])
        if kw_options:
            c1, c2 = st.columns([2, 3])
            with c1:
                sel_kw = st.selectbox("选择关键词", options=kw_options, key="kw_select")
            with c2:
                subset_kw = df[df["keyword"] == sel_kw]
                max_eng_kw = int(subset_kw["total_engagement"].max()) if not subset_kw.empty and pd.notna(subset_kw["total_engagement"].max()) else 0
                slider_max = max(1, max_eng_kw) if max_eng_kw == 0 else max_eng_kw
                eng_range_kw = st.slider("该词条热度范围", 0, slider_max, (0, slider_max), key="kw_eng_range")

            if st.button("🎲 抽取并锁定关键词样本", key="kw_sample_btn"):
                subset = subset_kw[subset_kw["total_engagement"].between(eng_range_kw[0], eng_range_kw[1])].copy()
                st.session_state.sample_kw = subset.sample(n=min(100, len(subset)), random_state=42) if not subset.empty else None

            if st.session_state.sample_kw is not None and not st.session_state.sample_kw.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_kw)} 条数据")
                # 🛡️ 严格应用 FULL_SHOW_COLS
                cols_to_show = [c for c in FULL_SHOW_COLS if c in st.session_state.sample_kw.columns]
                st.dataframe(st.session_state.sample_kw[cols_to_show], use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 2. 地域分析 ----------
    st.markdown("### 2. 发帖账号地域归属分布（Top 15）")
    if "clean_ip" in df.columns:
        geo_df = df[df["clean_ip"].fillna("") != ""].copy()
        geo_stats = geo_df[geo_df["clean_ip"] != "未知地域"]["clean_ip"].value_counts().head(15).reset_index(name="count")
        geo_stats.rename(columns={"index": "province"}, inplace=True)

        if not geo_stats.empty:
            fig_geo = px.bar(geo_stats.sort_values("count"), x="count", y="clean_ip" if "clean_ip" in geo_stats.columns else "province", orientation="h", text_auto=True, color="count", color_continuous_scale=[[0.0, "#EEF4F7"], [0.5, "#9EB9C2"], [1.0, "#4A6A75"]])
            fig_geo.update_layout(xaxis_title="帖子数量", yaxis_title="地域", plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=20, r=20, t=10, b=10), coloraxis_showscale=False)
            fig_geo.update_xaxes(showgrid=True, gridcolor="#E5E7EB")
            fig_geo.update_yaxes(showgrid=False)
            st.plotly_chart(fig_geo, use_container_width=True)

    with st.expander("👉 地域样本抽取与锁定", expanded=False):
        geo_options = sorted([x for x in df["clean_ip"].dropna().unique().tolist() if str(x).strip() != "" and x != "未知地域"]) if "clean_ip" in df.columns else []
        if geo_options:
            c1, c2 = st.columns([2, 3])
            with c1:
                sel_geo = st.selectbox("选择地域", options=geo_options, key="geo_select")
            with c2:
                subset_geo = df[df["clean_ip"] == sel_geo]
                max_eng_geo = int(subset_geo["total_engagement"].max()) if not subset_geo.empty and pd.notna(subset_geo["total_engagement"].max()) else 0
                slider_max = max(1, max_eng_geo) if max_eng_geo == 0 else max_eng_geo
                eng_range_geo = st.slider("该地域热度范围", 0, slider_max, (0, slider_max), key="geo_eng_range")

            if st.button("🎲 抽取并锁定地域样本", key="geo_sample_btn"):
                subset = subset_geo[subset_geo["total_engagement"].between(eng_range_geo[0], eng_range_geo[1])].copy()
                st.session_state.sample_geo = subset.sample(n=min(100, len(subset)), random_state=42) if not subset.empty else None

            if st.session_state.sample_geo is not None and not st.session_state.sample_geo.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_geo)} 条数据")
                # 🛡️ 严格应用 FULL_SHOW_COLS
                cols_to_show = [c for c in FULL_SHOW_COLS if c in st.session_state.sample_geo.columns]
                st.dataframe(st.session_state.sample_geo[cols_to_show], use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 3. 发帖用户画像占比与异常流量监测 ----------
    st.markdown("### 3. 用户画像分布与异常流量监测")
    if "user_role" in df.columns:
        role_stats = df["user_role"].fillna("未知").value_counts().reset_index(name="count")
        role_color_map = {'普通真实用户': '#5EAC98', '水军/高频通稿号': '#A8C66C', '官方媒体/政务号': '#F2A93B', "未知": "#C7CDD4"}
        role_stats["color_key"] = role_stats["user_role" if "user_role" in role_stats.columns else "index"].apply(lambda x: x if x in role_color_map else "未知")

        fig_role = px.pie(role_stats, values="count", names="user_role" if "user_role" in role_stats.columns else "index", hole=0.3, color="color_key", color_discrete_map=role_color_map)
        fig_role.update_traces(textposition="inside", textinfo="percent", marker=dict(line=dict(color="white", width=1.6)), sort=False)
        fig_role.update_layout(height=280, margin=dict(t=10, b=10, l=10, r=10), showlegend=True, legend=dict(orientation="v", y=0.5, x=1.05))

        pie_col1, pie_col2 = st.columns([1, 1])
        with pie_col1: st.plotly_chart(fig_role, use_container_width=True)

    st.markdown("#### 🤖 异常数据")
    tab1, tab2 = st.tabs(["🏆 内容复读机异常数据", "🔥 零评高热度异常数据"])

    with tab1:
        st.caption("💡 诊断逻辑：内容完全相同的帖子，根据被转发的次数倒序排列，提取 Top 50 的疑似水军通稿。")
        content_counts = df["content"].value_counts()
        repeat_contents = content_counts[content_counts > 1].index
        repeat_df = df[df["content"].isin(repeat_contents)].copy()

        if not repeat_df.empty:
            repeat_top50 = repeat_df.sort_values(by="post_repost_count", ascending=False).head(50)
            # 🛡️ 严格应用 FULL_SHOW_COLS
            cols_to_show = [c for c in FULL_SHOW_COLS if c in repeat_top50.columns]
            st.dataframe(repeat_top50[cols_to_show], use_container_width=True, height=280)
        else:
            st.info("✅ 表现良好，当前数据集中未发现重复发帖异常。")

    with tab2:
        st.caption("💡 诊断逻辑：只有刷赞/转发，但真实评论数为 0 的帖子。利用滑杆调节异常热度下限。")
        zero_comment_df = df[df["post_comment_count"] == 0]
        max_eng_zero = int(zero_comment_df["total_engagement"].max()) if not zero_comment_df.empty and pd.notna(zero_comment_df["total_engagement"].max()) else 0
        slider_max_anomaly = max(max_eng_zero, 10)
        default_val = min(50, slider_max_anomaly)

        anomaly_thresh = st.slider("把【总互动量（点赞+转发）】作为筛选参考 (默认查找 >50 的零评贴)", min_value=10, max_value=slider_max_anomaly, value=default_val, step=10, key="anomaly_slider")
        anomaly_df = zero_comment_df[zero_comment_df["total_engagement"] >= anomaly_thresh]

        if not anomaly_df.empty:
            anomaly_top50 = anomaly_df.sort_values(by="total_engagement", ascending=False).head(50)
            st.warning(f"🚨 在该阈值下抓到了 {len(anomaly_df)} 条异常帖子，下方展示 Top {len(anomaly_top50)}：")
            # 🛡️ 严格应用 FULL_SHOW_COLS
            cols_to_show = [c for c in FULL_SHOW_COLS if c in anomaly_top50.columns]
            st.dataframe(anomaly_top50[cols_to_show], use_container_width=True, height=280)
        else:
            st.success(f"✅ 在互动量 >= {anomaly_thresh} 的条件下，未发现 0 评论的异常高热度帖子。")

    with st.expander("👉 用户画像样本抽取与锁定", expanded=False):
        role_options = sorted([x for x in df["user_role"].dropna().unique().tolist()]) if "user_role" in df.columns else []
        if role_options:
            c1, c2 = st.columns([2, 3])
            with c1:
                sel_role = st.selectbox("选择角色类型", options=role_options, key="role_select")
            with c2:
                subset_role = df[df["user_role"] == sel_role]
                max_eng_role = int(subset_role["total_engagement"].max()) if not subset_role.empty and pd.notna(subset_role["total_engagement"].max()) else 0
                slider_max = max(1, max_eng_role) if max_eng_role == 0 else max_eng_role
                eng_range_role = st.slider("该角色热度范围", 0, slider_max, (0, slider_max), key="role_eng_range")

            if st.button("🎲 抽取并锁定角色样本", key="role_sample_btn"):
                subset = subset_role[subset_role["total_engagement"].between(eng_range_role[0], eng_range_role[1])].copy()
                st.session_state.sample_role = subset.sample(n=min(100, len(subset)), random_state=42) if not subset.empty else None

            if st.session_state.sample_role is not None and not st.session_state.sample_role.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_role)} 条数据")
                # 🛡️ 严格应用 FULL_SHOW_COLS
                cols_to_show = [c for c in FULL_SHOW_COLS if c in st.session_state.sample_role.columns]
                st.dataframe(st.session_state.sample_role[cols_to_show], use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 4. 其他特征探索 (多维交叉分析) ----------
    st.markdown("### 4. 其他特征探索 (多维交叉分析)")

    if "content_length" not in df.columns:
        df["content_length"] = df["content"].fillna("").astype(str).str.len()

    if "publish_time" in df.columns:
        dt_parsed = pd.to_datetime(df["publish_time"], errors="coerce")
        df["hour"] = dt_parsed.dt.hour.fillna(-1).astype(int)
        day_map = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三", "Thursday": "周四", "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}
        df["day_of_week"] = dt_parsed.dt.day_name().map(day_map)
    else:
        df["hour"] = -1
        df["day_of_week"] = "未知"

    has_real_time = df["hour"].nunique() > 1
    role_color_map = {'普通真实用户': '#5EAC98', '水军/高频通稿号': '#A8C66C', '官方媒体/政务号': '#F2A93B', '未知': '#C7CDD4'}

    analysis_options = [
        "1. 🕒 时效与流量热力图：什么时间段发帖最容易火？",
        "2. 📏 长度与互动散点图：长文还是短文转化率高？",
        "3. 📱 发帖工具柱状图：用什么平台发帖？",
        "4. 👻 发布时间雷达图：什么时间段发贴多？",
        "5. 🗺️ 地域话题偏好树状图：各省网友最关心什么？",
        "6. 📝 话痨程度箱线图：哪个省的人帖文最长？",
        "7. 🌪️ 互动心理 3D 聚类图：赞、评、转"
    ]

    sel_analysis = st.selectbox("🎯 请选择要探索的分析场景：", analysis_options)

    # ================= 场景 1：时效与流量热力图 =================
    if sel_analysis.startswith("1"):
        st.caption("💡 洞察：横轴为全天 24 小时，纵轴为星期几。颜色越深代表该时段平均互动（赞+评+转）越高。")
        if not has_real_time:
            st.warning("⚠️ 数据源中的 `publish_time` 日期没有精确到小时，无法绘制 24 小时热力图。")
        else:
            heat_df = df[df["hour"] >= 0].groupby(["hour", "day_of_week"])["total_engagement"].mean().reset_index()
            days_order = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

            fig1 = px.density_heatmap(heat_df, x="hour", y="day_of_week", z="total_engagement", histfunc="avg", category_orders={"day_of_week": days_order}, color_continuous_scale="Blues", nbinsx=24, title="每周各时段平均互动热度分布")
            fig1.update_layout(xaxis=dict(tickmode="array", tickvals=list(range(24)), ticktext=[f"{i}点" for i in range(24)]), xaxis_title="发布时间 (0-23点)", yaxis_title="星期", margin=dict(t=40, b=10))
            st.plotly_chart(fig1, use_container_width=True)

            st.markdown("#### 🎯 提取特定时段的帖子数据")
            c1, c2 = st.columns(2)
            with c1: sel_day = st.selectbox("📅 选择星期", options=days_order)
            with c2: sel_hour = st.selectbox("⏰ 选择小时", options=list(range(24)), format_func=lambda x: f"{x} 点")

            detail_df = df[(df["day_of_week"] == sel_day) & (df["hour"] == sel_hour)].sort_values(by="total_engagement", ascending=False)

            if not detail_df.empty:
                st.success(f"✅ 找到了 {len(detail_df)} 条在 **{sel_day} {sel_hour}点** 发布的帖子（按互动热度排序）：")
                display_df = detail_df.copy()
                if "publish_time" in display_df.columns:
                    display_df["publish_time"] = pd.to_datetime(display_df["publish_time"]).dt.strftime('%Y-%m-%d %H:%M:%S')

                # 🛡️ 严格应用 FULL_SHOW_COLS
                cols_to_show = [c for c in FULL_SHOW_COLS if c in display_df.columns]
                st.dataframe(display_df[cols_to_show], use_container_width=True, height=280)
            else:
                st.info(f"📭 在 **{sel_day} {sel_hour}点** 暂无帖子记录。")

    # ================= 场景 2：长度与互动散点图 =================
    elif sel_analysis.startswith("2"):
        st.caption("💡 洞察：X 轴为正文长度，Y 轴为互动量（对数轴以拉开差距），颜色区分关键词，气泡大小代表评论量。")
        df_len = df[df["content_length"] <= 2000].copy()
        df_sample = df_len.sample(n=min(1500, len(df_len)), random_state=42)

        fig2 = px.scatter(df_sample, x="content_length", y="total_engagement", color="keyword", size="post_comment_count", hover_data=["user_name", "user_role"], opacity=0.7, size_max=40, title="帖子字数长度与互动的黄金区间")
        fig2.update_layout(xaxis_title="正文字符长度", yaxis_title="总互动量 (Log对数)", yaxis_type="log", margin=dict(t=40, b=10), plot_bgcolor="white")
        fig2.update_xaxes(showgrid=True, gridcolor="#ECECEC")
        fig2.update_yaxes(showgrid=True, gridcolor="#ECECEC")
        st.plotly_chart(fig2, use_container_width=True)

    # ================= 场景 3：发帖工具鄙视链 =================
    elif sel_analysis.startswith("3"):
        st.caption("💡 洞察：展示发帖量 Top 15 的发布工具中，真实用户与水军/通稿号的比例构成差异。")
        if "post_tool" in df.columns:
            top_tools = df["post_tool"].value_counts().nlargest(15).index
            tool_df = df[df["post_tool"].isin(top_tools)]

            fig3 = px.histogram(tool_df, y="post_tool", color="user_role", barmode="stack", orientation="h", color_discrete_map=role_color_map, title="发帖工具分布与账号真伪鉴定")
            fig3.update_layout(xaxis_title="发帖频次", yaxis_title="发布工具", yaxis={'categoryorder': 'total ascending'}, margin=dict(t=40, b=10), plot_bgcolor="white")
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.error("❌ 数据中缺少 post_tool 字段。")

    # ================= 场景 4：深夜幽灵雷达图 =================
    elif sel_analysis.startswith("4"):
        st.caption("💡 洞察：通过极坐标雷达图查看各角色在 24 小时的活跃圈。")
        if not has_real_time:
            st.warning("⚠️ 缺乏精确到小时的时间数据，无法绘制时间雷达图。")
        else:
            radar_df = df[df["hour"] >= 0].groupby(["hour", "user_role"]).size().reset_index(name="count")
            radar_df["hour_str"] = radar_df["hour"].astype(str) + "点"
            hours_order = [f"{i}点" for i in range(24)]

            fig4 = px.line_polar(radar_df, r="count", theta="hour_str", color="user_role", line_close=True, color_discrete_map=role_color_map, title="全天发帖活跃度雷达图")
            fig4.update_traces(fill='toself', opacity=0.6)
            fig4.update_layout(polar=dict(radialaxis=dict(visible=True, showticklabels=False, showline=False), angularaxis=dict(categoryorder="array", categoryarray=hours_order, direction="clockwise", rotation=90)), margin=dict(t=60, b=40, l=40, r=40))
            st.plotly_chart(fig4, use_container_width=True)

    # ================= 场景 5：地域话题偏好 (升级为：100% 结构堆叠图) =================
    elif sel_analysis.startswith("5"):
        st.caption("💡 洞察：对比各省网民的【话题偏好结构】。每个省份内部的柱状图代表该省网民在不同话题上的占比，柱子越长代表该话题在该省网民中越受关注。")
        if "clean_ip" in df.columns:
            top_provs = df[df["clean_ip"] != "未知地域"]["clean_ip"].value_counts().nlargest(15).index
            df_prov = df[df["clean_ip"].isin(top_provs)]

            prov_kw_stats = df_prov.groupby(["clean_ip", "keyword"]).size().reset_index(name="count")
            prov_total = prov_kw_stats.groupby("clean_ip")["count"].sum().reset_index(name="total")
            prov_kw_stats = prov_kw_stats.merge(prov_total, on="clean_ip")
            prov_kw_stats["percentage"] = prov_kw_stats["count"] / prov_kw_stats["total"] * 100

            fig5 = px.bar(prov_kw_stats, x="percentage", y="clean_ip", color="keyword", orientation="h", title="各省份地域话题偏好结构 (100% 堆叠比)", labels={"percentage": "话题占比 (%)", "clean_ip": "地域"}, color_discrete_sequence=px.colors.qualitative.Pastel)
            fig5.update_layout(barmode="stack", margin=dict(t=40, l=10, r=10, b=10), plot_bgcolor="white", xaxis=dict(showgrid=True, gridcolor="#F0F0F0"))
            st.plotly_chart(fig5, use_container_width=True)

    # ================= 场景 6：省份话痨程度箱线图 =================
    elif sel_analysis.startswith("6"):
        st.caption("💡 洞察：展示发帖最活跃的 Top 15 地域，各个省份网民写帖子的字数长度分布与离群值。")
        if "clean_ip" in df.columns:
            top_ips = df[df["clean_ip"] != "未知地域"]["clean_ip"].value_counts().nlargest(15).index
            box_df = df[df["clean_ip"].isin(top_ips)].copy()
            box_df = box_df[box_df["content_length"] <= 1500]

            fig6 = px.box(box_df, x="clean_ip", y="content_length", color="clean_ip", hover_data=["post_comment_count", "user_role"], title="各地域网民“话痨”程度与离群习惯")
            fig6.update_layout(xaxis_title="地域 (Top 15)", yaxis_title="发帖正文字数", showlegend=False, margin=dict(t=40, b=10), plot_bgcolor="white")
            fig6.update_xaxes(tickangle=-45, showgrid=False)
            fig6.update_yaxes(showgrid=True, gridcolor="#ECECEC")
            st.plotly_chart(fig6, use_container_width=True)

    # ================= 场景 7：互动心理 3D 聚类漏斗 =================
    elif sel_analysis.startswith("7"):
        st.caption("💡 洞察：查看三类互动的分布倾向。")
        df_3d = df[(df["total_engagement"] > 0) & (df["total_engagement"] < 50000)].copy()
        df_3d = df_3d.sample(n=min(1000, len(df_3d)), random_state=42)

        fig7 = px.scatter_3d(df_3d, x="post_like_count", y="post_comment_count", z="post_repost_count", color="user_role", size="total_engagement", size_max=40, opacity=0.75, hover_data=["keyword", "user_name"], color_discrete_map=role_color_map, title="互动行为倾向 3D ")
        fig7.update_layout(scene=dict(xaxis_title='👍点赞', yaxis_title='💬评论', zaxis_title='🔁转发'), margin=dict(l=0, r=0, b=0, t=40))
        st.plotly_chart(fig7, use_container_width=True)

# ================= 右侧：悬浮查询详情区 (Sticky) =================
with col_right:
    st.markdown("## 🔍 帖子与评论详情")

    search_default = st.session_state.current_id if st.session_state.current_id else ""
    search_id = st.text_input("输入 Post ID", value=search_default, placeholder="从左侧表格复制 Post ID 粘贴到这里")

    if search_id:
        target_post = df_raw[df_raw["post_id"] == search_id.strip()]
        if not target_post.empty:
            post_data = target_post.iloc[0]
            st.success("✅ 检索成功")

            with st.container(border=True):
                st.markdown("#### 📝 帖子详情")
                st.markdown(f"**用户:** `{post_data.get('user_name', '')}` | **地域:** `{post_data.get('clean_ip', '')}` | **工具:** `{post_data.get('post_tool', '')}`")
                st.markdown(f"**内容:**\n {post_data.get('content', '')}")

                m1, m2, m3, m4 = st.columns(4)
                with m1: st.metric("👍 点赞", int(post_data.get("post_like_count", 0)))
                with m2: st.metric("💬 评论", int(post_data.get("post_comment_count", 0)))
                with m3: st.metric("🔁 转发", int(post_data.get("post_repost_count", 0)))
                with m4: st.metric("🔥 互动", int(post_data.get("total_engagement", 0)))

                if pd.notna(post_data.get("url")) and str(post_data.get("url")).strip():
                    st.link_button("🔗 微博原贴", str(post_data.get("url")), use_container_width=True)

            with st.container(border=True):
                st.markdown("#### 💬 评论详情")
                sub_comm = df_comm[df_comm["post_id"] == search_id.strip()] if not df_comm.empty else pd.DataFrame()
                if not sub_comm.empty:
                    # 评论表有独立的列，不需要受制于 FULL_SHOW_COLS
                    cols = [c for c in ["comment_user_name", "comment_content", "comment_like_count"] if c in sub_comm.columns]
                    st.dataframe(sub_comm[cols], use_container_width=True, height=350)
                else:
                    st.info("暂无抓取到该帖对应评论。")
        else:
            st.error("❌ 未找到该 Post ID")