import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

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
state_keys = ["sample_kw", "sample_geo", "sample_role", "current_id"]
for key in state_keys:
    if key not in st.session_state:
        st.session_state[key] = None


# ================= 5. 数据加载与清洗 =================
@st.cache_data
def load_all_data():
    # 自动获取你当前 app.py 所在的绝对文件夹路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 自动拼接出极其稳定的绝对路径
    p_path = os.path.join(current_dir, "crawler_results", "Dashboard_labeled_post.parquet")
    c_path_parquet = os.path.join(current_dir, "crawler_results", "Dashboard_Comments.parquet")
    c_path_csv = os.path.join(current_dir, "crawler_results", "Dashboard_Comments.csv")

    # --- 1. 读取主表 ---
    df_p = pd.read_parquet(p_path)
    df_p["post_id"] = df_p["post_id"].astype(str)

    # 处理互动量数字
    numeric_cols = ["post_like_count", "post_comment_count", "post_repost_count"]
    for col in numeric_cols:
        if col in df_p.columns:
            df_p[col] = pd.to_numeric(df_p[col], errors="coerce").fillna(0).astype(int)
        else:
            df_p[col] = 0

    # 统一总互动量字段
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
    elif os.path.exists(c_path_csv):
        df_c = pd.read_csv(c_path_csv, dtype={"post_id": str})
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
        # 去重：因为一个帖子有多条评论，我们只需要提取一次该帖子的发布时间
        time_mapping = df_c[['post_id', 'post_date', 'post_time']].dropna(subset=['post_time']).drop_duplicates(
            subset=['post_id'])

        # 将日期和时间拼接到一起，转为标准 datetime 格式
        time_mapping['real_publish_time'] = pd.to_datetime(
            time_mapping['post_date'].astype(str) + ' ' + time_mapping['post_time'].astype(str),
            errors='coerce'
        )

        # 将真实时间匹配回主表 (Left Join)
        df_p = df_p.merge(time_mapping[['post_id', 'real_publish_time']], on='post_id', how='left')

        # 用真实时间覆盖原本的 publish_time (如果某贴没抓到评论，则保留原有的时间兜底)
        if 'publish_time' in df_p.columns:
            df_p['publish_time'] = df_p['real_publish_time'].fillna(df_p['publish_time'])
        else:
            df_p['publish_time'] = df_p['real_publish_time']

        # 删掉临时列，保持表结构干净
        df_p = df_p.drop(columns=['real_publish_time'])

    return df_p, df_c


try:
    df_raw, df_comm = load_all_data()
except Exception as e:
    st.error(f"数据加载失败: {e}")
    st.stop()

# ================= 6. 顶部区 =================
st.title("📊 微博舆情深度分析看板")

top_c1, top_c2, top_c3 = st.columns([2.4, 1.2, 1.2])  # 恢复成三列，去除了发布工具统计

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

    # ---------- 0. 新增：全网声量演化趋势 ----------
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

    st.markdown("---")

    # ---------- 1. 升级：各关键词下辖数据体量与平均互动热度对比 (双轴图) ----------
    st.markdown("### 1. 各关键词下辖数据体量与平均互动热度对比")
    kw_agg = df.groupby("keyword").agg(post_count=("post_id", "count"),
                                       avg_eng=("total_engagement", "mean")).reset_index()
    kw_agg = kw_agg.sort_values("post_count", ascending=False)

    if not kw_agg.empty:
        fig_kw_dual = make_subplots(specs=[[{"secondary_y": True}]])
        # 柱状图：发帖数
        fig_kw_dual.add_trace(
            go.Bar(x=kw_agg["keyword"], y=kw_agg["post_count"], name="发帖总数 (篇)", marker_color="#7DA8D6"),
            secondary_y=False)
        # 折线图：平均热度
        fig_kw_dual.add_trace(
            go.Scatter(x=kw_agg["keyword"], y=kw_agg["avg_eng"], name="平均互动热度", mode="lines+markers",
                       line=dict(color="#DE5358", width=2.5), marker=dict(symbol="square", size=6)), secondary_y=True)

        fig_kw_dual.update_layout(plot_bgcolor="white", paper_bgcolor="white", margin=dict(l=20, r=20, t=20, b=20),
                                  legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
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
                # 🎯 核心联动：先根据选中的关键词过滤出子集，找这个子集的最大热度
                subset_kw = df[df["keyword"] == sel_kw]
                max_eng_kw = int(subset_kw["total_engagement"].max()) if not subset_kw.empty and pd.notna(
                    subset_kw["total_engagement"].max()) else 0
                slider_max = max(1, max_eng_kw) if max_eng_kw == 0 else max_eng_kw  # 防止最大值为0时滑杆报错

                eng_range_kw = st.slider("该词条热度范围", 0, slider_max, (0, slider_max), key="kw_eng_range")

            if st.button("🎲 抽取并锁定关键词样本", key="kw_sample_btn"):
                subset = subset_kw[subset_kw["total_engagement"].between(eng_range_kw[0], eng_range_kw[1])].copy()
                st.session_state.sample_kw = subset.sample(n=min(100, len(subset)),
                                                           random_state=42) if not subset.empty else None

            if st.session_state.sample_kw is not None and not st.session_state.sample_kw.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_kw)} 条数据")
                show_cols = ["post_id", "keyword", "user_name", "post_like_count", "post_comment_count",
                             "post_repost_count", "total_engagement", "content"]
                st.dataframe(
                    st.session_state.sample_kw[[c for c in show_cols if c in st.session_state.sample_kw.columns]],
                    use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 2. 地域分析 (保持不变) ----------
    st.markdown("### 2. 发帖账号地域归属分布（Top 15）")
    if "clean_ip" in df.columns:
        geo_df = df[df["clean_ip"].fillna("") != ""].copy()
        geo_stats = geo_df[geo_df["clean_ip"] != "未知地域"]["clean_ip"].value_counts().head(15).reset_index(
            name="count")
        geo_stats.rename(columns={"index": "province"}, inplace=True)  # 兼容低版本 pandas

        if not geo_stats.empty:
            fig_geo = px.bar(geo_stats.sort_values("count"), x="count",
                             y="clean_ip" if "clean_ip" in geo_stats.columns else "province", orientation="h",
                             text_auto=True, color="count",
                             color_continuous_scale=[[0.0, "#EEF4F7"], [0.5, "#9EB9C2"], [1.0, "#4A6A75"]])
            fig_geo.update_layout(xaxis_title="帖子数量", yaxis_title="地域", plot_bgcolor="white",
                                  paper_bgcolor="white", margin=dict(l=20, r=20, t=10, b=10), coloraxis_showscale=False)
            fig_geo.update_xaxes(showgrid=True, gridcolor="#E5E7EB")
            fig_geo.update_yaxes(showgrid=False)
            st.plotly_chart(fig_geo, use_container_width=True)

    with st.expander("👉 地域样本抽取与锁定", expanded=False):
        geo_options = sorted([x for x in df["clean_ip"].dropna().unique().tolist() if
                              str(x).strip() != "" and x != "未知地域"]) if "clean_ip" in df.columns else []
        if geo_options:
            c1, c2 = st.columns([2, 3])
            with c1:
                sel_geo = st.selectbox("选择地域", options=geo_options, key="geo_select")
            with c2:
                # 🎯 核心联动：计算选中地域的真实最大热度
                subset_geo = df[df["clean_ip"] == sel_geo]
                max_eng_geo = int(subset_geo["total_engagement"].max()) if not subset_geo.empty and pd.notna(
                    subset_geo["total_engagement"].max()) else 0
                slider_max = max(1, max_eng_geo) if max_eng_geo == 0 else max_eng_geo

                eng_range_geo = st.slider("该地域热度范围", 0, slider_max, (0, slider_max), key="geo_eng_range")

            if st.button("🎲 抽取并锁定地域样本", key="geo_sample_btn"):
                subset = subset_geo[subset_geo["total_engagement"].between(eng_range_geo[0], eng_range_geo[1])].copy()
                st.session_state.sample_geo = subset.sample(n=min(100, len(subset)),
                                                            random_state=42) if not subset.empty else None

            if st.session_state.sample_geo is not None and not st.session_state.sample_geo.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_geo)} 条数据")
                st.dataframe(
                    st.session_state.sample_geo[["post_id", "clean_ip", "user_name", "total_engagement", "content"]],
                    use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 3. 发帖用户画像占比与异常流量监测 (已融合新需求) ----------
    st.markdown("### 3. 用户画像分布与异常流量监测")
    if "user_role" in df.columns:
        role_stats = df["user_role"].fillna("未知").value_counts().reset_index(name="count")
        role_color_map = {'普通真实用户': '#5EAC98', '水军/高频通稿号': '#A8C66C', '官方媒体/政务号': '#F2A93B',
                          "未知": "#C7CDD4"}
        role_stats["color_key"] = role_stats["user_role" if "user_role" in role_stats.columns else "index"].apply(
            lambda x: x if x in role_color_map else "未知")

        fig_role = px.pie(role_stats, values="count",
                          names="user_role" if "user_role" in role_stats.columns else "index", hole=0.3,
                          color="color_key", color_discrete_map=role_color_map)
        fig_role.update_traces(textposition="inside", textinfo="percent",
                               marker=dict(line=dict(color="white", width=1.6)), sort=False)

        # 核心修复：极致限制饼图的占位高度，去掉上下边距
        fig_role.update_layout(height=280, margin=dict(t=10, b=10, l=10, r=10), showlegend=True,
                               legend=dict(orientation="v", y=0.5, x=1.05))

        pie_col1, pie_col2 = st.columns([1, 1])
        with pie_col1: st.plotly_chart(fig_role, use_container_width=True)

    # 👇 新增：异常流量与通稿监测中心
    st.markdown("#### 🤖 异常数据")
    tab1, tab2 = st.tabs(["🏆 内容复读机异常数据", "🔥 零评高热度异常数据"])

    with tab1:
        st.caption("💡 诊断逻辑：内容完全相同的帖子，根据被转发的次数倒序排列，提取 Top 50 的疑似水军通稿。")
        # 统计每段内容的重复次数
        content_counts = df["content"].value_counts()
        # 找出重复超过1次的内容
        repeat_contents = content_counts[content_counts > 1].index
        repeat_df = df[df["content"].isin(repeat_contents)].copy()

        if not repeat_df.empty:
            # 记录重复次数并按转发数排序
            repeat_df['全网重复次数'] = repeat_df['content'].map(content_counts)
            repeat_top50 = repeat_df.sort_values(by="post_repost_count", ascending=False).head(50)

            st.dataframe(
                repeat_top50[
                    ["post_id", "user_name", "全网重复次数", "post_repost_count", "total_engagement", "content"]],
                use_container_width=True, height=280
            )
        else:
            st.info("✅ 表现良好，当前数据集中未发现重复发帖异常。")

    with tab2:
        st.caption("💡 诊断逻辑：只有刷赞/转发，但真实评论数为 0 的帖子。利用滑杆调节异常热度下限。")

        # 1. 先圈定出所有 0 评论的帖子
        zero_comment_df = df[df["post_comment_count"] == 0]

        # 2. 动态获取 0 评论帖子中的“真实最高互动量”作为滑杆上限
        if not zero_comment_df.empty and pd.notna(zero_comment_df["total_engagement"].max()):
            max_eng_zero = int(zero_comment_df["total_engagement"].max())
        else:
            max_eng_zero = 0

        # 3. 安全防护：确保 max_value 至少为 10（防止报错），并动态调整默认值
        slider_max_anomaly = max(max_eng_zero, 10)
        default_val = min(50, slider_max_anomaly)  # 默认值50，但绝不能超过上限

        # 增加一个控制滑杆
        anomaly_thresh = st.slider(
            "把【总互动量（点赞+转发）】作为筛选参考 (默认查找 >50 的零评贴)",
            min_value=10, max_value=slider_max_anomaly, value=default_val, step=10, key="anomaly_slider"
        )

        # 应用过滤规则：在 0 评论的底表里，找出热度 >= 滑杆值的帖子
        anomaly_df = zero_comment_df[zero_comment_df["total_engagement"] >= anomaly_thresh]

        if not anomaly_df.empty:
            # 按总热度倒序排列，抽取前 50 条 (不足50条有几条展示几条)
            anomaly_top50 = anomaly_df.sort_values(by="total_engagement", ascending=False).head(50)
            st.warning(f"🚨 在该阈值下抓到了 {len(anomaly_df)} 条异常帖子，下方展示 Top {len(anomaly_top50)}：")
            st.dataframe(
                anomaly_top50[
                    ["post_id", "user_name", "total_engagement", "post_like_count", "post_repost_count", "content"]],
                use_container_width=True, height=280
            )
        else:
            st.success(f"✅ 在互动量 >= {anomaly_thresh} 的条件下，未发现 0 评论的异常高热度帖子。")

    with st.expander("👉 用户画像样本抽取与锁定", expanded=False):
        role_options = sorted(
            [x for x in df["user_role"].dropna().unique().tolist()]) if "user_role" in df.columns else []
        if role_options:
            c1, c2 = st.columns([2, 3])
            with c1:
                sel_role = st.selectbox("选择角色类型", options=role_options, key="role_select")
            with c2:
                # 🎯 核心联动：计算选中角色的真实最大热度
                subset_role = df[df["user_role"] == sel_role]
                max_eng_role = int(subset_role["total_engagement"].max()) if not subset_role.empty and pd.notna(
                    subset_role["total_engagement"].max()) else 0
                slider_max = max(1, max_eng_role) if max_eng_role == 0 else max_eng_role

                eng_range_role = st.slider("该角色热度范围", 0, slider_max, (0, slider_max), key="role_eng_range")

            if st.button("🎲 抽取并锁定角色样本", key="role_sample_btn"):
                subset = subset_role[
                    subset_role["total_engagement"].between(eng_range_role[0], eng_range_role[1])].copy()
                st.session_state.sample_role = subset.sample(n=min(100, len(subset)),
                                                             random_state=42) if not subset.empty else None

            if st.session_state.sample_role is not None and not st.session_state.sample_role.empty:
                st.success(f"已为你锁定符合条件的 {len(st.session_state.sample_role)} 条数据")
                st.dataframe(
                    st.session_state.sample_role[["post_id", "user_role", "user_name", "total_engagement", "content"]],
                    use_container_width=True, height=260)

    st.markdown("---")

    # ---------- 4. 其他特征探索 (多维交叉分析) ----------
    st.markdown("### 4. 其他特征探索 (多维交叉分析)")

    # 🟢 数据预处理：特征工程 (计算帖子长度、精确提取 24 小时和星期)
    if "content_length" not in df.columns:
        df["content_length"] = df["content"].fillna("").astype(str).str.len()

    if "publish_time" in df.columns:
        # 强制转换为 datetime，精确提取小时
        dt_parsed = pd.to_datetime(df["publish_time"], errors="coerce")
        df["hour"] = dt_parsed.dt.hour.fillna(-1).astype(int)

        # 提取星期几，并翻译为中文
        day_map = {"Monday": "周一", "Tuesday": "周二", "Wednesday": "周三",
                   "Thursday": "周四", "Friday": "周五", "Saturday": "周六", "Sunday": "周日"}
        df["day_of_week"] = dt_parsed.dt.day_name().map(day_map)
    else:
        df["hour"] = -1
        df["day_of_week"] = "未知"

    # 检查是否真的有有效的小时数据 (如果全部帖子都在同一时间，或者没有具体时分秒，则给出提示)
    has_real_time = df["hour"].nunique() > 1

    # 统一的用户画像配色 (保持看板风格一致)
    role_color_map = {
        '普通真实用户': '#5EAC98',
        '水军/高频通稿号': '#A8C66C',
        '官方媒体/政务号': '#F2A93B',
        '未知': '#C7CDD4'
    }

    # 🟢 下拉菜单：7大商业洞察场景选择
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
    # ================= 场景 1：时效与流量热力图 =================
    if sel_analysis.startswith("1"):
        st.caption(
            "💡 洞察：横轴为全天 24 小时，纵轴为星期几。颜色越深代表该时段平均互动（赞+评+转）越高。")
        if not has_real_time:
            st.warning("⚠️ 数据源中的 `publish_time` 日期没有精确到小时，无法绘制 24 小时热力图。")
        else:
            # 按小时和星期分组计算平均热度
            heat_df = df[df["hour"] >= 0].groupby(["hour", "day_of_week"])["total_engagement"].mean().reset_index()
            days_order = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

            fig1 = px.density_heatmap(
                heat_df, x="hour", y="day_of_week", z="total_engagement", histfunc="avg",
                category_orders={"day_of_week": days_order},
                color_continuous_scale="Blues", nbinsx=24, title="每周各时段平均互动热度分布"
            )
            fig1.update_layout(
                xaxis=dict(tickmode="array", tickvals=list(range(24)), ticktext=[f"{i}点" for i in range(24)]),
                xaxis_title="发布时间 (0-23点)", yaxis_title="星期", margin=dict(t=40, b=10)
            )
            st.plotly_chart(fig1, use_container_width=True)

            # 👇 新增：传统且绝对稳定的下拉框联动筛选
            st.markdown("#### 🎯 提取特定时段的帖子数据")

            # 使用左右两列并排放置选择器，节省空间
            c1, c2 = st.columns(2)
            with c1:
                sel_day = st.selectbox("📅 选择星期", options=days_order)
            with c2:
                # 传入 0-23 的数字，但展示给用户看的是 "0 点", "1 点"
                sel_hour = st.selectbox("⏰ 选择小时", options=list(range(24)), format_func=lambda x: f"{x} 点")

            # 根据下拉框的值实时过滤原始大表，并按总互动量倒序
            detail_df = df[(df["day_of_week"] == sel_day) & (df["hour"] == sel_hour)].sort_values(
                by="total_engagement", ascending=False)

            if not detail_df.empty:
                st.success(f"✅ 找到了 {len(detail_df)} 条在 **{sel_day} {sel_hour}点** 发布的帖子（按互动热度排序）：")

                # 👇 核心修改：在展示列中加入了 "publish_time" (它包含了完整的日期和时间)
                show_cols = ["post_id", "user_name", "publish_time", "total_engagement", "post_like_count",
                             "post_comment_count", "post_repost_count", "content"]

                # 顺手把 publish_time 转换成更好看的字符串格式，防止 Streamlit 报时区错误
                display_df = detail_df[show_cols].copy()
                display_df["publish_time"] = display_df["publish_time"].dt.strftime('%Y-%m-%d %H:%M:%S')

                st.dataframe(display_df, use_container_width=True, height=280)
            else:
                st.info(f"📭 在 **{sel_day} {sel_hour}点** 暂无帖子记录。")
    # ================= 场景 2：长度与互动散点图 =================
    elif sel_analysis.startswith("2"):
        st.caption("💡 洞察：X 轴为正文长度，Y 轴为互动量（对数轴以拉开差距），颜色区分关键词，气泡大小代表评论量。")
        # 过滤异常超长文，抽样防卡死
        df_len = df[df["content_length"] <= 2000].copy()
        df_sample = df_len.sample(n=min(1500, len(df_len)), random_state=42)

        fig2 = px.scatter(
            df_sample, x="content_length", y="total_engagement", color="keyword",
            size="post_comment_count", hover_data=["user_name", "user_role"],
            opacity=0.7, size_max=40, title="帖子字数长度与互动的黄金区间"
        )
        fig2.update_layout(
            xaxis_title="正文字符长度", yaxis_title="总互动量 (Log对数)",
            yaxis_type="log", margin=dict(t=40, b=10), plot_bgcolor="white"
        )
        fig2.update_xaxes(showgrid=True, gridcolor="#ECECEC")
        fig2.update_yaxes(showgrid=True, gridcolor="#ECECEC")
        st.plotly_chart(fig2, use_container_width=True)

    # ================= 场景 3：发帖工具鄙视链 =================
    elif sel_analysis.startswith("3"):
        st.caption("💡 洞察：展示发帖量 Top 15 的发布工具中，真实用户与水军/通稿号的比例构成差异。")
        if "post_tool" in df.columns:
            top_tools = df["post_tool"].value_counts().nlargest(15).index
            tool_df = df[df["post_tool"].isin(top_tools)]

            fig3 = px.histogram(
                tool_df, y="post_tool", color="user_role", barmode="stack", orientation="h",
                color_discrete_map=role_color_map, title="发帖工具分布与账号真伪鉴定"
            )
            fig3.update_layout(
                xaxis_title="发帖频次", yaxis_title="发布工具",
                yaxis={'categoryorder': 'total ascending'}, margin=dict(t=40, b=10), plot_bgcolor="white"
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.error("❌ 数据中缺少 post_tool 字段。")

        # ================= 场景 4：深夜幽灵雷达图 =================
    elif sel_analysis.startswith("4"):
        st.caption(
            "💡 洞察：通过极坐标雷达图查看各角色在 24 小时的活跃圈。")
        if not has_real_time:
            st.warning("⚠️ 缺乏精确到小时的时间数据，无法绘制时间雷达图。")
        else:
            # 1. 聚合数据
            radar_df = df[df["hour"] >= 0].groupby(["hour", "user_role"]).size().reset_index(name="count")

            # 2. 核心修复：把数字强转为字符串类别，防止 Plotly 把 0-23 当成“角度”
            radar_df["hour_str"] = radar_df["hour"].astype(str) + "点"
            hours_order = [f"{i}点" for i in range(24)]

            fig4 = px.line_polar(
                radar_df, r="count", theta="hour_str", color="user_role", line_close=True,
                color_discrete_map=role_color_map, title="全天发帖活跃度雷达图"
            )

            # 3. 颜值大升级：增加半透明填充，让雷达图真正有“面积感”
            fig4.update_traces(fill='toself', opacity=0.6)

            # 4. 调整表盘直觉：0点在正上方，顺时针排布
            fig4.update_layout(
                polar=dict(
                    radialaxis=dict(visible=True, showticklabels=False, showline=False),  # 保留同心圆网格线，去掉生硬的数字
                    angularaxis=dict(
                        categoryorder="array",
                        categoryarray=hours_order,
                        direction="clockwise",  # 顺时针排布
                        rotation=90  # 让 0 点从正上方开始
                    )
                ),
                margin=dict(t=60, b=40, l=40, r=40)
            )
            st.plotly_chart(fig4, use_container_width=True)

        # ================= 场景 5：地域话题偏好 (升级为：100% 结构堆叠图) =================
    elif sel_analysis.startswith("5"):
        st.caption(
            "💡 洞察：对比各省网民的【话题偏好结构】。每个省份内部的柱状图代表该省网民在不同话题上的占比，柱子越长代表该话题在该省网民中越受关注。")
        if "clean_ip" in df.columns:
            # 过滤并取 Top 15 活跃省份
            top_provs = df[df["clean_ip"] != "未知地域"]["clean_ip"].value_counts().nlargest(15).index
            df_prov = df[df["clean_ip"].isin(top_provs)]

            # 计算每个省份内部的关键词占比
            prov_kw_stats = df_prov.groupby(["clean_ip", "keyword"]).size().reset_index(name="count")
            prov_total = prov_kw_stats.groupby("clean_ip")["count"].sum().reset_index(name="total")
            prov_kw_stats = prov_kw_stats.merge(prov_total, on="clean_ip")
            prov_kw_stats["percentage"] = prov_kw_stats["count"] / prov_kw_stats["total"] * 100

            fig5 = px.bar(
                prov_kw_stats, x="percentage", y="clean_ip", color="keyword",
                orientation="h", title="各省份地域话题偏好结构 (100% 堆叠比)",
                labels={"percentage": "话题占比 (%)", "clean_ip": "地域"},
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig5.update_layout(
                barmode="stack", margin=dict(t=40, l=10, r=10, b=10),
                plot_bgcolor="white", xaxis=dict(showgrid=True, gridcolor="#F0F0F0")
            )
            st.plotly_chart(fig5, use_container_width=True)

    # ================= 场景 6：省份话痨程度箱线图 =================
    elif sel_analysis.startswith("6"):
        st.caption("💡 洞察：展示发帖最活跃的 Top 15 地域，各个省份网民写帖子的字数长度分布与离群值。")
        if "clean_ip" in df.columns:
            top_ips = df[df["clean_ip"] != "未知地域"]["clean_ip"].value_counts().nlargest(15).index
            box_df = df[df["clean_ip"].isin(top_ips)].copy()
            box_df = box_df[box_df["content_length"] <= 1500]  # 过滤异常极长文防止箱体压扁

            fig6 = px.box(
                box_df, x="clean_ip", y="content_length", color="clean_ip",
                hover_data=["post_comment_count", "user_role"], title="各地域网民“话痨”程度与离群习惯"
            )
            fig6.update_layout(
                xaxis_title="地域 (Top 15)", yaxis_title="发帖正文字数",
                showlegend=False, margin=dict(t=40, b=10), plot_bgcolor="white"
            )
            fig6.update_xaxes(tickangle=-45, showgrid=False)
            fig6.update_yaxes(showgrid=True, gridcolor="#ECECEC")
            st.plotly_chart(fig6, use_container_width=True)

            # ================= 场景 7：互动心理 3D 聚类漏斗 =================
    elif sel_analysis.startswith("7"):
        st.caption("💡 洞察：查看三类互动的分布倾向。")
        # 排除 0 互动数据，随机抽取 1000 条进行 3D 渲染防止浏览器卡顿
        df_3d = df[(df["total_engagement"] > 0) & (df["total_engagement"] < 50000)].copy()
        df_3d = df_3d.sample(n=min(1000, len(df_3d)), random_state=42)

        fig7 = px.scatter_3d(
            df_3d, x="post_like_count", y="post_comment_count", z="post_repost_count",
            color="user_role", size="total_engagement", size_max=40, opacity=0.75,
            hover_data=["keyword", "user_name"], color_discrete_map=role_color_map,
            title="互动行为倾向 3D "
        )
        fig7.update_layout(
            scene=dict(xaxis_title='👍点赞', yaxis_title='💬评论', zaxis_title='🔁转发'),
            margin=dict(l=0, r=0, b=0, t=40)
        )
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
                st.markdown(
                    f"**用户:** `{post_data.get('user_name', '')}` | **地域:** `{post_data.get('clean_ip', '')}` | **工具:** `{post_data.get('post_tool', '')}`")
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
                    cols = [c for c in ["comment_user_name", "comment_content", "comment_like_count"] if
                            c in sub_comm.columns]
                    st.dataframe(sub_comm[cols], use_container_width=True, height=350)
                else:
                    st.info("暂无抓取到该帖对应评论。")
        else:
            st.error("❌ 未找到该 Post ID")
